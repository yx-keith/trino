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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.server.protocol.ProtocolUtil;
import io.trino.server.ui.query.editor.QueryEditorModule;
import io.trino.server.ui.query.editor.execution.QueryClient;
import io.trino.server.ui.query.editor.execution.QueryRunner;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.analyzer.TypeSignatureTranslator;
import org.joda.time.Duration;

import java.util.HashSet;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/4/12 10:01
 */
public class ColumnCache
{
    private static final Logger log = Logger.get(ColumnCache.class);
    private static final Joiner FQN_JOINER = Joiner.on('.').skipNulls();
    private final QueryRunner.QueryRunnerFactory queryRunnerFactory;

    @Inject
    public ColumnCache(QueryRunner.QueryRunnerFactory queryRunnerFactory)
    {
        this.queryRunnerFactory = requireNonNull(queryRunnerFactory, "queryRunnerFactory is null!");
    }

    public ImmutableList<Column> getColumns(String catalogName, String schemaName, String tableName, String user)
    {
        return queryColumns(FQN_JOINER.join(catalogName, schemaName, tableName), user);
    }

    private ImmutableList<Column> queryColumns(String fqnTableName, String user)
    {
        String statement = format("SHOW COLUMNS FROM %s", fqnTableName);
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorModule.UI_QUERY_SOURCE, user);
        QueryClient queryClient = new QueryClient(queryRunner, Duration.standardSeconds(60), statement);

        final ImmutableList.Builder<Column> cache = ImmutableList.builder();
        try {
            queryClient.executeWith(client -> {
                QueryData results = client.currentData();
                if (results.getData() != null) {
                    for (List<Object> row : results.getData()) {
                        TypeSignature typeSignature = TypeSignatureTranslator.parseTypeSignature((String) row.get(1), new HashSet<>());
                        Column column = new Column((String) row.get(0), (String) row.get(1), ProtocolUtil.toClientTypeSignature(typeSignature));
                        cache.add(column);
                    }
                }

                return null;
            });
        }
        catch (QueryClient.QueryTimeOutException e) {
            log.error("Caught timeout loading columns", e);
        }

        return cache.build();
    }


}
