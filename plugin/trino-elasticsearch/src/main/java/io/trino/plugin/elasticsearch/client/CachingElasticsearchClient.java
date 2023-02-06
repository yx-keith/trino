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
package io.trino.plugin.elasticsearch.client;


import com.google.common.base.Splitter;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.airlift.stats.TimeStat;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.plugin.elasticsearch.ElasticsearchConfig;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.StrictMath.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingElasticsearchClient
{
    private static final Splitter SPLITTER = Splitter.on(":").limit(2);
    private final LoadingCache<String, BackpressureRestHighLevelClient> clientCache;
    private final ElasticsearchConfig config;
    private final TimeStat backpressureStats = new TimeStat(MILLISECONDS);

    @Inject
    public CachingElasticsearchClient(ElasticsearchConfig config)
    {
        CacheLoader<String, BackpressureRestHighLevelClient> clientCacheLoader = new CacheLoader<>() {
            @Override
            public BackpressureRestHighLevelClient load(String key) {
                return loadClient(key);
            }
        };

        clientCache = newCacheBuilder(OptionalLong.of(config.getEsClientCacheExpireSeconds()),
                OptionalLong.empty(), 1000).build(CacheLoader.asyncReloading(clientCacheLoader, newDirectExecutorService()));
        this.config = config;
    }

    private static EvictableCacheBuilder<Object, Object> newCacheBuilder(OptionalLong expiresAfterWriteMillis,
                                                                         OptionalLong refreshMillis, long maximumSize)
    {
        EvictableCacheBuilder<Object, Object> cacheBuilder = EvictableCacheBuilder.newBuilder();
        if (expiresAfterWriteMillis.isPresent()) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expiresAfterWriteMillis.getAsLong(), SECONDS);
        }
        if (refreshMillis.isPresent() && (!expiresAfterWriteMillis.isPresent() || expiresAfterWriteMillis.getAsLong() > refreshMillis.getAsLong())) {
            cacheBuilder = cacheBuilder.refreshAfterWrite(refreshMillis.getAsLong(), SECONDS);
        }
        cacheBuilder = cacheBuilder
                .maximumSize(maximumSize)
                .recordStats();
        return cacheBuilder;
    }

    public BackpressureRestHighLevelClient getInstance(String key)
    {
        return get(clientCache,key);
    }

    public List<BackpressureRestHighLevelClient> getAllInstances()
    {
        return new ArrayList<>(clientCache.asMap().values());
    }

    private static <K,V> V get(LoadingCache<K,V> cache, K key)
    {
        try {
            return cache.get(key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private BackpressureRestHighLevelClient loadClient(String key)
    {
        List<String> keyList = SPLITTER.splitToList(key);
        String user = keyList.get(0);
        String password = keyList.get(1);
        return creatClient(config, user, password, backpressureStats);
    }

    private BackpressureRestHighLevelClient creatClient(ElasticsearchConfig config, String user, String password, TimeStat backpressureStats)
    {
        RestClientBuilder builder = RestClient.builder(
                config.getHosts().stream()
                        .map(httpHost -> new HttpHost(httpHost, config.getPort(), config.isTlsEnabled() ? "https" : "http"))
                        .toArray(HttpHost[]::new))
                .setMaxRetryTimeoutMillis(toIntExact(config.getMaxRetryTime().toMillis()));

        builder.setHttpClientConfigCallback(ignored -> {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(toIntExact(config.getConnectTimeout().toMillis()))
                    .setSocketTimeout(toIntExact(config.getRequestTimeout().toMillis()))
                    .build();

            IOReactorConfig reactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(config.getHttpThreadCount())
                    .build();

            // the client builder passed to the call-back is configured to use system properties, which makes it
            // impossible to configure concurrency settings, so we need to build a new one from scratch
            HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create()
                    .setDefaultRequestConfig(requestConfig)
                    .setDefaultIOReactorConfig(reactorConfig)
                    .setMaxConnPerRoute(config.getMaxHttpConnections())
                    .setMaxConnTotal(config.getMaxHttpConnections());

            CredentialsProvider credentials = new BasicCredentialsProvider();
            credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
            clientBuilder.setDefaultCredentialsProvider(credentials);

            return clientBuilder;
        });
        return new BackpressureRestHighLevelClient(builder, config, backpressureStats);
    }
}
