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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/1/9 19:04
 */
public class ExecutionSimpleRequest
{
    @JsonProperty
    private final String query;

    @JsonProperty
    private final String user;

    @JsonProperty
    private final String defaultCatalog;

    @JsonProperty
    private final String defaultSchema;

    @JsonCreator
    public ExecutionSimpleRequest(
            @JsonProperty("query") final String query,
            @JsonProperty("defaultCatalog") final String defaultCatalog,
            @JsonProperty("defaultSchema") final String defaultSchema,
            @JsonProperty("user") final String user)
    {
        this.query = query;
        this.user = user == null ? "trino" : user;
        this.defaultCatalog = defaultCatalog == null ? "hive" : defaultCatalog;
        this.defaultSchema = defaultSchema == null ? "default" : defaultSchema;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public String getDefaultCatalog()
    {
        return defaultCatalog;
    }

    @JsonProperty
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }
}
