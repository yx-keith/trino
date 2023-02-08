package io.trino.server.ui.query.editor;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/1/9 9:24
 */
public class QueryEditorConfig
{
    private Duration executionTimeout = new Duration(15, MINUTES);

    public Duration getExecutionTimeout()
    {
        return executionTimeout;
    }

    @Config("trino.query-ui.execution-timeout")
    public void setExecutionTimeout(Duration executionTimeout)
    {
        this.executionTimeout = executionTimeout;
    }
}
