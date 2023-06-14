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
package io.trino.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class QueryHiveTableName
{
    private final String databaseName;
    private final String tableName;
    private final String queryId;

    @JsonCreator
    public QueryHiveTableName(@JsonProperty("queryId") String queryId, @JsonProperty("databaseName") String databaseName, @JsonProperty("tableName") String tableName)
    {
        this.queryId = queryId;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static QueryHiveTableName queryHiveTableName(String queryId, String databaseName, String tableName)
    {
        return new QueryHiveTableName(queryId, databaseName, tableName);
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("databaseName", databaseName)
                .add("tableName", tableName)
                .toString();
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

        QueryHiveTableName other = (QueryHiveTableName) o;
        return  Objects.equals(queryId, other.queryId) &&
                Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queryId, databaseName, tableName);
    }
}
