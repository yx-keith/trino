package io.trino.server.ui.query.editor.output;

import java.net.URI;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/8 15:45
 */
public interface PersistentJobOutput
{
    String getType();

    String getDescription();

    URI getLocation();

    void setLocation(URI location);

    String processQuery(String query);
}
