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
package io.trino.server.ui.query.editor.protocol;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;
import io.trino.client.Column;
import io.trino.client.QueryError;
import io.trino.server.BasicQueryStats;
import io.trino.server.ui.query.editor.output.PersistentJobOutput;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.*;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/8 15:42
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Job
{
    @JsonProperty
    private final String user;

    @JsonProperty
    private final String query;

    @JsonProperty
    private final UUID uuid;

    @JsonProperty
    private final PersistentJobOutput output;

    @JsonProperty
    private JobQueryStats queryStats;

    @JsonProperty
    private URI infoUri;

    @JsonProperty
    private JobState state;

    //    @JsonProperty
    private List<Column> columns;

    //    @JsonProperty
    private Set<Table> tablesUsed;

    @JsonProperty
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private DateTime queryStarted;

    @JsonProperty
    private QueryError error;

    @JsonProperty
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private DateTime queryFinished;

    @JsonProperty
    private JobSessionContext sessionContext;

    @JsonCreator
    public Job(@JsonProperty("user") final String user,
               @JsonProperty("query") final String query,
               @JsonProperty("uuid") final UUID uuid,
               @JsonProperty("output") final PersistentJobOutput output,
               @JsonProperty("queryStats") final JobQueryStats queryStats,
               @JsonProperty("state") final JobState state,
               @JsonProperty("columns") final List<Column> columns,
               @JsonProperty("tablesUsed") final Set<Table> tablesUsed,
               @JsonProperty("queryStarted") final DateTime queryStarted,
               @JsonProperty("error") final QueryError error,
               @JsonProperty("infoUri") final URI infoUri,
               @JsonProperty("queryFinished") final DateTime queryFinished,
               @JsonProperty("sessionContext") final JobSessionContext sessionContext)
    {
        this.user = user;
        this.query = query;
        this.uuid = uuid;
        this.output = output;
        this.queryStats = queryStats;
        this.state = state;
        this.columns = columns;
        this.tablesUsed = tablesUsed;
        this.queryStarted = queryStarted;
        this.error = error;
        this.infoUri = infoUri;
        this.queryFinished = queryFinished;
    }

    public Job(final String user,
               final String query,
               final UUID uuid,
               final PersistentJobOutput output,
               final JobQueryStats stats,
               final JobState state,
               final List<Column> columns,
               final QueryError error,
               final DateTime queryFinished,
               final JobSessionContext sessionContext)
    {
        this(user,
                query,
                uuid,
                output,
                stats,
                state,
                columns,
                Sets.<Table>newConcurrentHashSet(),
                new DateTime(),
                error,
                null,
                queryFinished,
                sessionContext);
    }

    @JsonIgnore
    public DateTime getQueryFinishedDateTime()
    {
        return queryFinished;
    }

    @JsonIgnore
    public DateTime getQueryStartedDateTime()
    {
        return queryStarted;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public UUID getUuid()
    {
        return uuid;
    }

    @JsonProperty
    public PersistentJobOutput getOutput()
    {
        return output;
    }

    public void setQueryStats(BasicQueryStats queryStats)
    {
        this.queryStats = new JobQueryStats(queryStats.getCreateTime(),
                queryStats.getElapsedTime(),
                queryStats.getProgressPercentage());
    }

    @JsonProperty
    public JobQueryStats getQueryStats()
    {
        return queryStats;
    }

    public void setState(JobState state)
    {
        this.state = state;
    }

    @JsonProperty
    public JobState getState()
    {
        return state;
    }

    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }

    //    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    //    @JsonProperty
    public Set<Table> getTablesUsed()
    {
        return tablesUsed;
    }

    @JsonProperty
    public DateTime getQueryStarted()
    {
        return queryStarted;
    }

    @JsonProperty
    public QueryError getError()
    {
        return error;
    }

    public void setError(QueryError queryError)
    {
        error = queryError;
    }

    public void setQueryFinished(DateTime queryFinished)
    {
        this.queryFinished = queryFinished;
    }

    public void setInfoUri(URI infoUri)
    {
        this.infoUri = infoUri;
    }

    @JsonProperty
    public URI getInfoUri()
    {
        return infoUri;
    }

    @JsonProperty
    public DateTime getQueryFinished()
    {
        return queryFinished;
    }

    @JsonProperty
    public JobSessionContext getSessionContext()
    {
        return sessionContext;
    }

    public void setSessionContext(JobSessionContext sessionContext)
    {
        this.sessionContext = sessionContext;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Job job = (Job) o;
        return uuid.equals(job.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
    }

    /**
     * Minimal query stats as per usage.
     */
    public static class JobQueryStats
    {
        private final DateTime createTime;
        private final Duration elapsedTime;
        private final OptionalDouble progresssPercentage;

        @JsonCreator
        public JobQueryStats(
                @JsonProperty("createTime") DateTime createTime,
                @JsonProperty("elapsedTime") Duration elapsedTime,
                @JsonProperty("progressPercentage") OptionalDouble progresssPercentage)
        {
            this.createTime = createTime;
            this.elapsedTime = elapsedTime;
            this.progresssPercentage = progresssPercentage;
        }

        @JsonProperty
        public DateTime getCreateTime()
        {
            return createTime;
        }

        @JsonProperty
        public Duration getElapsedTime()
        {
            return elapsedTime;
        }

        @JsonProperty
        public OptionalDouble getProgresssPercentage()
        {
            return progresssPercentage;
        }
    }
}
