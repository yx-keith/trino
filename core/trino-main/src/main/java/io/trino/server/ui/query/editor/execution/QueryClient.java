package io.trino.server.ui.query.editor.execution;

import com.google.common.base.Stopwatch;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/8 15:18
 */
public class QueryClient
{
    private final QueryRunner queryRunner;
    private final Duration timeout;
    private final String query;
    private final AtomicReference<QueryStatusInfo> finalResults = new AtomicReference<>();

    public QueryClient(QueryRunner queryRunner, String query)
    {
        this(queryRunner, Duration.ofSeconds(60 * 30), query);
    }

    public QueryClient(QueryRunner queryRunner, org.joda.time.Duration timeout, String query)
    {
        this(queryRunner, Duration.ofMillis(timeout.getMillis()), query);
    }

    public QueryClient(QueryRunner queryRunner, Duration timeout, String query)
    {
        this.queryRunner = queryRunner;
        this.timeout = timeout;
        this.query = query;
    }

    public <T> T executeWith(Function<StatementClient, T> function)
            throws QueryTimeOutException
    {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        T t = null;

        try (StatementClient client = queryRunner.startInternalQuery(query)) {
            while (client.isRunning() && !Thread.currentThread().isInterrupted()) {
                if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > timeout.toMillis()) {
                    throw new QueryTimeOutException(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                }

                t = function.apply(client);
                client.advance();
            }

            finalResults.set(client.finalStatusInfo());
        }
        catch (RuntimeException | QueryTimeOutException e) {
            stopwatch.stop();
            throw e;
        }

        return t;
    }

    public QueryStatusInfo finalResults()
    {
        return finalResults.get();
    }

    public static class QueryTimeOutException
            extends Throwable
    {
        private final long elapsedMs;

        public QueryTimeOutException(long elapsedMs)
        {
            this.elapsedMs = elapsedMs;
        }

        public long getElapsedMs()
        {
            return elapsedMs;
        }
    }}
