package io.trino.server.ui.query.editor.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.execution.Input;
import io.trino.execution.QueryStats;

import java.util.Set;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/10 14:06
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UIBasicQueryInfo
{
    private final QueryStats queryStats;
    private final Set<Input> inputs;

    @JsonCreator
    public UIBasicQueryInfo(
            @JsonProperty("queryStats") QueryStats queryStats,
            @JsonProperty("inputs") Set<Input> inputs)
    {
        this.queryStats = queryStats;
        this.inputs = inputs;
    }

    @JsonProperty
    public QueryStats getQueryStats()
    {
        return queryStats;
    }

    @JsonProperty
    public Set<Input> getInputs()
    {
        return inputs;
    }
}
