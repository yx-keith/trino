package io.trino.server.ui.query.editor.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/1/9 19:04
 */
public class ExecutionRequest
{
    @JsonProperty
    private final String query;

    @JsonProperty
    private final JobSessionContext sessionContext;

    @JsonProperty
    private final String defaultConnector;

    @JsonProperty
    private final String defaultSchema;

    @JsonCreator
    public ExecutionRequest(
            @JsonProperty("query") final String query,
            @JsonProperty("defaultConnector") final String defaultConnector,
            @JsonProperty("defaultSchema") final String defaultSchema,
            @JsonProperty("sessionContext") final JobSessionContext sessionContext)
    {
        this.query = query;
        this.sessionContext = sessionContext;
        this.defaultConnector = (sessionContext == null || sessionContext.getCatalog() == null) ?
                (defaultConnector == null ? "hive" : defaultConnector) : sessionContext.getCatalog();
        this.defaultSchema = (sessionContext == null || sessionContext.getSchema() == null) ?
                (defaultSchema == null ? "default" : defaultSchema) : sessionContext.getSchema();
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public String getDefaultConnector()
    {
        return defaultConnector;
    }

    @JsonProperty
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @JsonProperty
    public JobSessionContext getSessionContext()
    {
        return sessionContext;
    }
}
