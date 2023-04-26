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
package io.trino.server.ui.query.editor.metadata;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.client.QueryData;
import io.trino.server.ui.query.editor.QueryEditorModule;
import io.trino.server.ui.query.editor.execution.QueryClient;
import io.trino.server.ui.query.editor.execution.QueryRunner;
import io.trino.server.ui.query.editor.protocol.CatalogSchema;
import io.trino.server.ui.query.editor.protocol.Table;
import org.joda.time.Duration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/4/12 10:01
 */
public class SchemaCache
{
    private final Logger log = Logger.get(SchemaCache.class);
    private final QueryRunner.QueryRunnerFactory queryRunnerFactory;

    @Inject
    public SchemaCache(QueryRunner.QueryRunnerFactory queryRunnerFactory)
    {
        this.queryRunnerFactory = requireNonNull(queryRunnerFactory, "queryRunnerFactory is null!");
    }

    public ImmutableList<Table> queryTables(String catalogName, String schemaName, String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorModule.UI_QUERY_SOURCE, user);
        String statement = format("SHOW TABLES FROM %s.%s", catalogName, schemaName);

        Set<String> tablesResult = queryStatement(queryRunner, statement);

        final ImmutableList.Builder<Table> builder = ImmutableList.builder();
        for (String tableName : tablesResult) {
            builder.add(new Table(catalogName, schemaName, tableName));
        }
        return builder.build();
    }

    public ImmutableList<CatalogSchema> querySchemas(String user)
    {
        Set<String> catalogs = queryCatalogs(user);

        final ImmutableList.Builder<CatalogSchema> builder = ImmutableList.builder();
        for (String catalogName : catalogs) {
            builder.add(querySchemas(catalogName, user));
        }
        return builder.build();
    }

    public CatalogSchema querySchemas(String catalogName, String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorModule.UI_QUERY_SOURCE, user);
        String statement = format("SHOW SCHEMAS FROM %s", catalogName);

        Set<String> schemasResult = queryStatement(queryRunner, statement);
        return new CatalogSchema(catalogName, ImmutableList.copyOf(schemasResult));
    }

    public Set<String> queryCatalogs(String user)
    {
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorModule.UI_QUERY_SOURCE, user);
        String statment = format("show catalogs");
        Set<String> catalogsResult = queryStatement(queryRunner,statment);
        return catalogsResult;
    }

    private Set<String> queryStatement(QueryRunner queryRunner, String statement)
    {
        QueryClient queryClient = new QueryClient(queryRunner, Duration.standardSeconds(120), statement);

        final Set<String> resultSet = new HashSet();
        try {
            queryClient.executeWith(client -> {
                QueryData results = client.currentData();
                if (results.getData() != null) {
                    for (List<Object> row : results.getData()) {
                        resultSet.add((String) row.get(0));
                    }
                }
                return null;
            });
        }
        catch (QueryClient.QueryTimeOutException e) {
            log.error("Caught timeout loading data", e);
        }
        return resultSet;
    }
}
