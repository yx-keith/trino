package io.trino.server.ui.query.editor.resoures;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/3/1 10:30
 */
public class PreviewResponse
{
    private final List<String> columns;
    private final List<List<String>> data;
    private final Integer total;
    private final String fileName;

    @JsonCreator
    public PreviewResponse(@JsonProperty("columns") List<String> columns,
                           @JsonProperty("data") List<List<String>> data,
                           @JsonProperty("total") Integer total,
                           @JsonProperty("fileName") String fileName)
    {
        this.columns = columns;
        this.data = data;
        this.total = total;
        this.fileName = fileName;
    }

    @JsonProperty
    public List<String> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<List<String>> getData()
    {
        return data;
    }

    @JsonProperty
    public int getTotal()
    {
        return total;
    }

    @JsonProperty
    public String getFileName()
    {
        return fileName;
    }
}
