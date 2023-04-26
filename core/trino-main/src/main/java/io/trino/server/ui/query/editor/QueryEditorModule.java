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
package io.trino.server.ui.query.editor;

import com.google.inject.*;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigDefaults;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.units.Duration;
import io.trino.server.ui.query.editor.execution.ClientSessionFactory;
import io.trino.server.ui.query.editor.execution.ExecutionClient;
import io.trino.server.ui.query.editor.execution.QueryInfoClient;
import io.trino.server.ui.query.editor.execution.QueryRunner.QueryRunnerFactory;
import io.trino.server.ui.query.editor.metadata.ColumnCache;
import io.trino.server.ui.query.editor.metadata.PreviewTableCache;
import io.trino.server.ui.query.editor.metadata.SchemaCache;
import io.trino.server.ui.query.editor.output.PersistentJobOutputFactory;
import io.trino.server.ui.query.editor.output.builds.OutputBuilderFactory;
import io.trino.server.ui.query.editor.output.persistors.CSVPersistorFactory;
import io.trino.server.ui.query.editor.output.persistors.PersistorFactory;
import io.trino.server.ui.query.editor.resoures.ExecuteResource;
import io.trino.server.ui.query.editor.resoures.FilesResource;
import io.trino.server.ui.query.editor.resoures.MetadataResource;
import io.trino.server.ui.query.editor.resoures.ResultsPreviewResource;
import io.trino.server.ui.query.editor.store.files.ExpiringFileStore;
import okhttp3.OkHttpClient;

import javax.inject.Named;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.trino.client.OkHttpUtil.setupCookieJar;
import static io.trino.client.OkHttpUtil.setupTimeouts;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/1/9 15:19
 */
public class QueryEditorModule
        extends AbstractConfigurationAwareModule
{
    public static final String UI_QUERY_SOURCE = "ui-server";
    private static final ConfigDefaults<HttpClientConfig> HTTP_CLIENT_CONFIG_DEFAULTS = d -> new HttpClientConfig()
            .setConnectTimeout(new Duration(10, TimeUnit.SECONDS));

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TelePalConfig.class);
        jaxrsBinder(binder).bind(ExecuteResource.class);
        jaxrsBinder(binder).bind(FilesResource.class);
        jaxrsBinder(binder).bind(ResultsPreviewResource.class);
        jaxrsBinder(binder).bind(MetadataResource.class);

        binder.bind(ColumnCache.class).in(Scopes.SINGLETON);
        binder.bind(SchemaCache.class).in(Scopes.SINGLETON);
        binder.bind(PreviewTableCache.class).in(Scopes.SINGLETON);
        binder.bind(ExecutionClient.class).in(Scopes.SINGLETON);
        binder.bind(PersistentJobOutputFactory.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("query-info-client", ForQueryInfoClient.class)
                .withTracing();
    }

    @Singleton
    @Provides
    public OkHttpClient provideOkHttpClient()
    {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        setupTimeouts(builder, 30, SECONDS);
        setupCookieJar(builder);
        return builder.build();
    }

    @Named("coordinator-uri")
    @Provides
    public URI provideCoordinatorURI(HttpServerConfig httpConfig, TelePalConfig queryEditorConfig)
    {
        if (queryEditorConfig.isRunningEmbeded()) {
            return URI.create("http://localhost:" + httpConfig.getHttpPort());
        } else {
            return URI.create(queryEditorConfig.getCoordinatorUri());
        }
    }

    @Singleton
    @Named("default-catalog")
    @Provides
    public String provideDefaultCatalog()
    {
        return "hive";
    }

    @Provides
    @Singleton
    public ClientSessionFactory provideClientSessionFactory(@Named("coordinator-uri") Provider<URI> uriProvider)
    {
        return new ClientSessionFactory(uriProvider,
                "trino",
                "web-ui",
                "system",
                "information_schema",
                Duration.succinctDuration(15, TimeUnit.MINUTES));
    }

    @Provides
    public QueryInfoClient provideQueryInfoClient(OkHttpClient okHttpClient)
    {
        return new QueryInfoClient(okHttpClient);
    }

    @Provides
    public QueryRunnerFactory provideQueryRunner(ClientSessionFactory sessionFactory, OkHttpClient httpClient)
    {
        return new QueryRunnerFactory(sessionFactory, httpClient);
    }

    @Provides
    public OutputBuilderFactory provideOutputBuilder(TelePalConfig config)
    {
        return new OutputBuilderFactory(config.getResultPath(), config.getMaxFileSize().toBytes(), false);
    }

    @Provides
    @Singleton
    public ExpiringFileStore provideExpiringFileStore(TelePalConfig config)
    {
        return new ExpiringFileStore(config.getMaxResultCount());
    }

    @Provides
    @Singleton
    public CSVPersistorFactory provideCSVPersistorFactory(ExpiringFileStore fileStore)
    {
        return new CSVPersistorFactory(fileStore);
    }

    @Provides
    @Singleton
    public PersistorFactory providePersistorFactory(CSVPersistorFactory csvPersistorFactory)
    {
        return new PersistorFactory(csvPersistorFactory);
    }
}
