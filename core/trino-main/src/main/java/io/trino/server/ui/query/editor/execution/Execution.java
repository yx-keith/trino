/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.ui.query.editor.execution;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import io.airlift.units.DataSize;
import io.trino.client.*;
import io.trino.server.BasicQueryStats;
import io.trino.server.ui.query.editor.execution.ExecutionClient.ExecutionFailureException;
import io.trino.server.ui.query.editor.execution.QueryClient.QueryTimeOutException;
import io.trino.server.ui.query.editor.output.builds.FileTooLargeException;
import io.trino.server.ui.query.editor.output.builds.JobOutputBuilder;
import io.trino.server.ui.query.editor.output.builds.OutputBuilderFactory;
import io.trino.server.ui.query.editor.output.persistors.Persistor;
import io.trino.server.ui.query.editor.output.persistors.PersistorFactory;
import io.trino.server.ui.query.editor.protocol.Job;
import io.trino.server.ui.query.editor.protocol.JobSessionContext;
import io.trino.server.ui.query.editor.protocol.JobState;
import io.trino.server.ui.query.editor.protocol.Table;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/8 16:57
 */
public class Execution
        implements Callable<Job>
{
    static final Splitter QUERY_SPLITTER = Splitter.on(";").omitEmptyStrings().trimResults();
    private final Job job;
    private final QueryRunner queryRunner;
    private final QueryInfoClient queryInfoClient;
    private final QueryExecutionAuthorizer authorizer;
    private final Duration timeout;
    private final OutputBuilderFactory outputBuilderFactory;
    private final PersistorFactory persistorFactory;

    private final RateLimiter updateLimiter = RateLimiter.create(2.0);
    private final int maxRowsPreviewOutput = 1_000;
    private boolean isCancelled;

    public Execution(Job job, QueryRunner queryRunner, QueryInfoClient queryInfoClient,
                     QueryExecutionAuthorizer authorizer, Duration timeout, OutputBuilderFactory outputBuilderFactory,
                     PersistorFactory persistorFactory)
    {
        this.job = job;
        this.queryRunner = queryRunner;
        this.queryInfoClient = queryInfoClient;
        this.authorizer = authorizer;
        this.timeout = timeout;
        this.outputBuilderFactory = outputBuilderFactory;
        this.persistorFactory = persistorFactory;
    }

    public void cancel()
    {
        isCancelled = true;
    }

    @Override
    public Job call() throws Exception
    {
        return doExecute();
    }

    private Job doExecute()
            throws ExecutionFailureException
    {
        final String userQuery = QUERY_SPLITTER.splitToList(getJob().getQuery()).get(0);
        final JobOutputBuilder outputBuilder;
        job.setQueryStats(createNoOpQueryStats());

        try {
            outputBuilder = outputBuilderFactory.forJob(job);
        }
        catch (IOException e) {
            throw new ExecutionFailureException(job, "Could not create output builder for job", e);
        }
        catch (InvalidQueryException e) {
            throw new ExecutionFailureException(job, e.getMessage(), e);
        }
        final Persistor persistor = persistorFactory.getPersistor(job, job.getOutput());
        final String query = job.getOutput().processQuery(userQuery);
        final Set<Table> tables = new HashSet<>();

        if (!authorizer.isAuthorizedRead(tables)) {
            job.setQueryStats(createNoOpQueryStats());
            throw new ExecutionFailureException(job, "Cannot access tables", null);
        }

        JobSessionContext jobSessionContext = JobSessionContext.buildFromClient(queryRunner.getSession());
        job.setSessionContext(jobSessionContext);

        QueryClient queryClient = new QueryClient(queryRunner, timeout, query);
        try {
            queryClient.executeWith(client -> {
                if (client == null) {
                    return null;
                }

                QueryStatusInfo statusInfo = client.currentStatusInfo();
                QueryData data = client.currentData();
                List<Column> resultColumns = null;
                JobState jobState = null;
                QueryError queryError = null;
                BasicQueryStats queryStats = null;

                if (isCancelled) {
                    throw new ExecutionFailureException(job, "Query was cancelled", null);
                }

                if (statusInfo.getError() != null) {
                    queryError = statusInfo.getError();
                    jobState = JobState.FAILED;
                }

                if ((statusInfo.getInfoUri() != null) && (jobState != JobState.FAILED)) {
                    //获取query在服务端执行的状态及信息,UIBasicQueryInfo接口可扩展,参考io.trino.execution.QueryInfo
                    UIBasicQueryInfo queryInfo = queryInfoClient.getQueryInfo(statusInfo.getInfoUri(), statusInfo.getId());
                    if (queryInfo != null) {
                        queryStats = queryInfo.getBasicQueryStats();
                    }
                }

                if (statusInfo.getInfoUri() != null && job.getInfoUri() == null) {
                    URI infoUri = statusInfo.getInfoUri();
                    String path = infoUri.getPath();
                    path = path.substring(path.indexOf("query.html"));
                    infoUri = URI.create(path + "?" + infoUri.getQuery());
                    job.setInfoUri(infoUri);
                }

                if (statusInfo.getStats() != null) {
                    jobState = JobState.fromStatementState(statusInfo.getStats().getState());
                }

                try {
                    if (statusInfo.getColumns() != null) {
                        resultColumns = statusInfo.getColumns();
                        outputBuilder.addColumns(resultColumns);
                    }

                    if (data.getData() != null) {
                        List<List<Object>> resultsData = ImmutableList.copyOf(data.getData());
                        for (List<Object> row : resultsData) {
                            outputBuilder.addRow(row);
                        }
                    }
                } catch (FileTooLargeException e) {
                    throw new ExecutionFailureException(job, "Output file exceeded maximum configured filesize", e);
                }
                updateJobInfo(tables, resultColumns, queryStats, jobState, queryError);
                return null;
            });
        } catch (QueryTimeOutException e) {
            throw new ExecutionFailureException(job,
                    format("Query exceeded maximum execution time of %s minutes", Duration.millis(e.getElapsedMs()).getStandardMinutes()),
                    e);
        }

        QueryStatusInfo finalResults = queryClient.finalResults();
        if (finalResults != null && finalResults.getInfoUri() != null) {
            UIBasicQueryInfo queryInfo = queryInfoClient.getQueryInfo(finalResults.getInfoUri(), finalResults.getId());

            if (queryInfo != null) {
                updateJobInfo(
                        null,
                        null,
                        queryInfo.getBasicQueryStats(),
                        JobState.fromStatementState(finalResults.getStats().getState()),
                        finalResults.getError());
            }
        }

        if (job.getState() != JobState.FAILED) {
            URI location = persistor.persist(outputBuilder, job);
            if (location != null) {
                job.getOutput().setLocation(location);
            }
        } else {
            throw new ExecutionFailureException(job, null, null);
        }
        return getJob();
    }

    public Job getJob()
    {
        return job;
    }
    protected void updateJobInfo(
            Set<Table> usedTables,
            List<Column> columns,
            BasicQueryStats queryStats,
            JobState state,
            QueryError error)
    {
        if ((usedTables != null) && (usedTables.size() > 0)) {
            job.getTablesUsed().addAll(usedTables);
        }

        if ((columns != null) && (columns.size() > 0)) {
            job.setColumns(columns);
        }

        if (queryStats != null) {
            job.setQueryStats(queryStats);
        }

        if ((state != null) && (job.getState() != JobState.FINISHED) && (job.getState() != JobState.FAILED)) {
            job.setState(state);
        }

        if (error != null) {
            FailureInfo failureInfo = new FailureInfo(
                    error.getFailureInfo().getType(),
                    error.getFailureInfo().getMessage(),
                    null,
                    Collections.<FailureInfo>emptyList(),
                    Collections.<String>emptyList(),
                    error.getFailureInfo().getErrorLocation());

            QueryError queryError = new QueryError(
                    error.getMessage(),
                    error.getSqlState(),
                    error.getErrorCode(),
                    error.getErrorName(),
                    error.getErrorType(),
                    error.getErrorLocation(),
                    failureInfo);

            job.setError(queryError);
        }
    }

    public static BasicQueryStats createNoOpQueryStats()
    {
        DateTime now = DateTime.now();
        io.airlift.units.Duration zeroDuration = new io.airlift.units.Duration(0, TimeUnit.SECONDS);

        return new BasicQueryStats(
                now,
                now,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                0,
                0,
                0,
                0,
                0,
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                0,
                0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                zeroDuration,
                zeroDuration,
                zeroDuration,
                zeroDuration,
                false,
                ImmutableSet.of(),
                OptionalDouble.empty());
    }
}
