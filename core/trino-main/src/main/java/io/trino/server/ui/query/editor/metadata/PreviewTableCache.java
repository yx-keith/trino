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
import io.trino.client.QueryData;
import io.trino.server.ui.query.editor.QueryEditorModule;
import io.trino.server.ui.query.editor.execution.QueryClient;
import io.trino.server.ui.query.editor.execution.QueryRunner;
import org.joda.time.Duration;

import java.util.List;

import static java.lang.String.format;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/4/12 10:01
 */
public class PreviewTableCache
{
    private final QueryRunner.QueryRunnerFactory queryRunnerFactory;
    private static final Logger log = Logger.get(PreviewTableCache.class);
    private static final Joiner FQN_JOINER = Joiner.on('.').skipNulls();
    private static final int PREVIEW_LIMIT = 100;

    @Inject
    public PreviewTableCache(QueryRunner.QueryRunnerFactory queryRunnerFactory)
    {
        this.queryRunnerFactory = queryRunnerFactory;
    }

    public List<List<Object>> getPreviewLimit(String catalogName, String schemaName,
                                              String tableNaem, String user)
    {
        String fqnTableName = FQN_JOINER.join(catalogName, schemaName, tableNaem);
        return queryRows(fqnTableName, user);
    }

    private List<List<Object>> queryRows(String fqnTableName, String user) {
        String query = format("SELECT * FROM %s LIMIT %s", fqnTableName, PREVIEW_LIMIT);
        QueryRunner queryRunner = queryRunnerFactory.create(QueryEditorModule.UI_QUERY_SOURCE, user);
        QueryClient queryClient = new QueryClient(queryRunner, Duration.standardSeconds(60), query);

        final ImmutableList.Builder<List<Object>> cache = ImmutableList.builder();
        try {
            queryClient.executeWith(
                    client -> {
                        QueryData results = client.currentData();
                        if (results.getData() != null) {
                            cache.addAll(results.getData());
                        }
                        return null;
                    });

        } catch (QueryClient.QueryTimeOutException e) {
            log.error("Caught timeout loading columns", e);
        }
        return cache.build();
    }
}
