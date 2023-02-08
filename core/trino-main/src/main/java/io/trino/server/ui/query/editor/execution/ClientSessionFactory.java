package io.trino.server.ui.query.editor.execution;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;

import javax.inject.Provider;
import java.net.URI;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/8 14:51
 */
public class ClientSessionFactory
{
    private final String defaultSchema;
    private final String catalog;
    private final String source;
    private final String user;
    private final Provider<URI> server;
    private final ZoneId timeZoneId;
    private final Locale locale;
    private final Duration clientSessionTimeout;

    public ClientSessionFactory(Provider<URI> server, String user, String source, String catalog, String defaultSchema, Duration clientSessionTimeout)
    {
        this.server = server;
        this.user = user;
        this.source = source;
        this.catalog = catalog;
        this.defaultSchema = defaultSchema;
        this.timeZoneId = TimeZone.getTimeZone("UTC").toZoneId();
        this.locale = Locale.getDefault();
        this.clientSessionTimeout = firstNonNull(clientSessionTimeout, succinctDuration(1, MINUTES));
    }

    public URI getServer()
    {
        return server.get();
    }

    public ClientSession create(String user, String catalog, String schema, Map<String, String> properties)
    {
        return create(user, catalog, schema, properties, source);
    }

    public ClientSession create(String user, String catalog, String schema, Map<String, String> properties, String source)
    {
        return new ClientSession(server.get(),
                null,
                Optional.of(user),
                source,
                Optional.empty(),
                ImmutableSet.of(),
                null,
                catalog == null ? this.catalog : catalog,
                schema == null ? this.defaultSchema : schema,
                null,
                timeZoneId,
                locale,
                ImmutableMap.<String, String>of(),
                properties,
                ImmutableMap.<String, String>of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                null,
                clientSessionTimeout,
                true);
    }

    public ClientSession create(String source, String user)
    {
        return create(user, this.catalog, defaultSchema, ImmutableMap.<String, String>of(), source);
    }

    public ClientSession create()
    {
        return create(user, catalog, defaultSchema, ImmutableMap.<String, String>of());
    }
}
