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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.connector.CatalogName;
import io.trino.connector.ConnectorManager;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.metadata.*;
import io.trino.metadata.InternalFunctionBundle.InternalFunctionBundleBuilder;
import io.trino.security.AccessControlManager;
import io.trino.security.GroupProviderManager;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.HeaderAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.spi.Plugin;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.exchange.ExchangeManagerFactory;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.trino.spi.security.CertificateAuthenticatorFactory;
import io.trino.spi.security.GroupProviderFactory;
import io.trino.spi.security.HeaderAuthenticatorFactory;
import io.trino.spi.security.PasswordAuthenticatorFactory;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.session.SessionPropertyConfigurationManagerFactory;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.Type;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PluginManager
{
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("io.trino.spi.")
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("org.openjdk.jol.")
            .add("io.trino.metadata.")
            .add("io.trino.operator.scalar.")
            .build();

    private static final Logger log = Logger.get(PluginManager.class);
    private static final String HIVE_UDF_PLUGIN = "io.trino.plugin.hive.HiveUdfPlugin";
    private static final String HIVE_PLUGIN = "io.trino.plugin.hive.HivePlugin";
    private static final String UDF_PROPS_FILE_PATH = format("etc%sudf.properties", File.separatorChar);
    private final PluginsProvider pluginsProvider;
    private final ConnectorManager connectorManager;
    private final GlobalFunctionCatalog globalFunctionCatalog;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final AccessControlManager accessControlManager;
    private final Optional<PasswordAuthenticatorManager> passwordAuthenticatorManager;
    private final CertificateAuthenticatorManager certificateAuthenticatorManager;
    private final Optional<HeaderAuthenticatorManager> headerAuthenticatorManager;
    private final EventListenerManager eventListenerManager;
    private final GroupProviderManager groupProviderManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final TypeRegistry typeRegistry;
    private final BlockEncodingManager blockEncodingManager;
    private final HandleResolver handleResolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final ServerPluginsProviderConfig config;
    private final StaticCatalogStore staticCatalogStore;
    private String pluginName;
    private final MetadataManager metadataManager;
    @Inject
    public PluginManager(
            PluginsProvider pluginsProvider,
            ConnectorManager connectorManager,
            GlobalFunctionCatalog globalFunctionCatalog,
            ResourceGroupManager<?> resourceGroupManager,
            AccessControlManager accessControlManager,
            Optional<PasswordAuthenticatorManager> passwordAuthenticatorManager,
            CertificateAuthenticatorManager certificateAuthenticatorManager,
            Optional<HeaderAuthenticatorManager> headerAuthenticatorManager,
            EventListenerManager eventListenerManager,
            GroupProviderManager groupProviderManager,
            SessionPropertyDefaults sessionPropertyDefaults,
            TypeRegistry typeRegistry,
            BlockEncodingManager blockEncodingManager,
            HandleResolver handleResolver,
            ExchangeManagerRegistry exchangeManagerRegistry,
            ServerPluginsProviderConfig serverPluginsProviderConfig,
            StaticCatalogStore staticCatalogStore,
            MetadataManager metadataManager)
    {
        this.pluginsProvider = requireNonNull(pluginsProvider, "pluginsProvider is null");
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.globalFunctionCatalog = requireNonNull(globalFunctionCatalog, "globalFunctionCatalog is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.accessControlManager = requireNonNull(accessControlManager, "accessControlManager is null");
        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
        this.certificateAuthenticatorManager = requireNonNull(certificateAuthenticatorManager, "certificateAuthenticatorManager is null");
        this.headerAuthenticatorManager = requireNonNull(headerAuthenticatorManager, "headerAuthenticatorManager is null");
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.groupProviderManager = requireNonNull(groupProviderManager, "groupProviderManager is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");
        this.typeRegistry = requireNonNull(typeRegistry, "typeRegistry is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.config = requireNonNull(serverPluginsProviderConfig, "serverPluginsProviderConfig is null");
        this.staticCatalogStore = requireNonNull(staticCatalogStore, "staticCatalogStore is null");
        this.metadataManager = requireNonNull(metadataManager, "metadataManager is null");
    }

    public void loadPlugins()
    {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        pluginsProvider.loadPlugins(this::loadPlugin, PluginManager::createClassLoader);

        typeRegistry.verifyTypes();
    }

    private void loadPlugin(String plugin, Supplier<PluginClassLoader> createClassLoader)
            throws Exception
    {
        log.info("-- Loading plugin %s --", plugin);

        PluginClassLoader pluginClassLoader = createClassLoader.get();

        log.debug("Classpath for plugin:");
        for (URL url : pluginClassLoader.getURLs()) {
            log.debug("    %s", url.getPath());
        }

        handleResolver.registerClassLoader(pluginClassLoader);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader);
        }

        log.info("-- Finished loading plugin %s --", plugin);

        if (HIVE_PLUGIN.equals(pluginName)) {
            staticCatalogStore.loadCatalogs("hive");
            metadataManager.copyFileToLocal(config.getRemoteHiveUdfPropsPath(), UDF_PROPS_FILE_PATH, "hive");
            metadataManager.copyFileToLocal(config.getRemoteHiveUdfJarsPath(), config.getLocalHiveUdfJarsPath().toString(), "hive");
        }
    }

    private void loadPlugin(PluginClassLoader pluginClassLoader)
            throws Exception
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);
        checkState(!plugins.isEmpty(), "No service providers of type %s in the classpath: %s", Plugin.class.getName(), asList(pluginClassLoader.getURLs()));

        for (Plugin plugin : plugins) {
            pluginName = plugin.getClass().getName();
            log.info("Installing %s", pluginName);

            if (HIVE_UDF_PLUGIN.equals(pluginName)) {
                plugin.initHiveUdf(config.getLocalHiveUdfJarsPath(), UDF_PROPS_FILE_PATH);
                plugin.setMaxFunctionRunningTimeEnable(config.getMaxFunctionRunningTimeEnable());
                plugin.setMaxFunctionRunningTimeInSec(config.getMaxFunctionRunningTimeInSec());
                plugin.setFunctionRunningThreadPoolSize(config.getFunctionRunningThreadPoolSize());
            }
            installPlugin(plugin, pluginClassLoader::duplicate);
        }
    }

    public void installPlugin(Plugin plugin, Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory)
    {
        installPluginInternal(plugin, duplicatePluginClassLoaderFactory);
        typeRegistry.verifyTypes();
    }

    private void installPluginInternal(Plugin plugin, Function<CatalogName, ClassLoader> duplicatePluginClassLoaderFactory)
    {
        for (BlockEncoding blockEncoding : plugin.getBlockEncodings()) {
            log.info("Registering block encoding %s", blockEncoding.getName());
            blockEncodingManager.addBlockEncoding(blockEncoding);
        }

        for (Type type : plugin.getTypes()) {
            log.info("Registering type %s", type.getTypeSignature());
            typeRegistry.addType(type);
        }

        for (ParametricType parametricType : plugin.getParametricTypes()) {
            log.info("Registering parametric type %s", parametricType.getName());
            typeRegistry.addParametricType(parametricType);
        }

        for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
            log.info("Registering connector %s", connectorFactory.getName());
            connectorManager.addConnectorFactory(connectorFactory, duplicatePluginClassLoaderFactory);
        }

        Set<Class<?>> functions = plugin.getFunctions();
        if (!functions.isEmpty()) {
            log.info("Registering functions from %s", plugin.getClass().getSimpleName());
            InternalFunctionBundleBuilder builder = InternalFunctionBundle.builder();
            functions.forEach(builder::functions);
            globalFunctionCatalog.addFunctions(builder.build());
        }

        Set<Object> hiveUdfs = plugin.getHiveUdfFunctions();
        if (!hiveUdfs.isEmpty()) {
            log.info("Registering functions from %s", plugin.getClass().getSimpleName());
            InternalFunctionBundleBuilder builder = InternalFunctionBundle.builder();
            hiveUdfs.forEach(udf -> builder.function((SqlFunction) udf));
            globalFunctionCatalog.addFunctions(builder.build());
        }

        for (SessionPropertyConfigurationManagerFactory sessionConfigFactory : plugin.getSessionPropertyConfigurationManagerFactories()) {
            log.info("Registering session property configuration manager %s", sessionConfigFactory.getName());
            sessionPropertyDefaults.addConfigurationManagerFactory(sessionConfigFactory);
        }

        for (ResourceGroupConfigurationManagerFactory configurationManagerFactory : plugin.getResourceGroupConfigurationManagerFactories()) {
            log.info("Registering resource group configuration manager %s", configurationManagerFactory.getName());
            resourceGroupManager.addConfigurationManagerFactory(configurationManagerFactory);
        }

        for (SystemAccessControlFactory accessControlFactory : plugin.getSystemAccessControlFactories()) {
            log.info("Registering system access control %s", accessControlFactory.getName());
            accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        }

        passwordAuthenticatorManager.ifPresent(authenticationManager -> {
            for (PasswordAuthenticatorFactory authenticatorFactory : plugin.getPasswordAuthenticatorFactories()) {
                log.info("Registering password authenticator %s", authenticatorFactory.getName());
                authenticationManager.addPasswordAuthenticatorFactory(authenticatorFactory);
            }
        });

        for (CertificateAuthenticatorFactory authenticatorFactory : plugin.getCertificateAuthenticatorFactories()) {
            log.info("Registering certificate authenticator %s", authenticatorFactory.getName());
            certificateAuthenticatorManager.addCertificateAuthenticatorFactory(authenticatorFactory);
        }

        headerAuthenticatorManager.ifPresent(authenticationManager -> {
            for (HeaderAuthenticatorFactory authenticatorFactory : plugin.getHeaderAuthenticatorFactories()) {
                log.info("Registering header authenticator %s", authenticatorFactory.getName());
                authenticationManager.addHeaderAuthenticatorFactory(authenticatorFactory);
            }
        });

        for (EventListenerFactory eventListenerFactory : plugin.getEventListenerFactories()) {
            log.info("Registering event listener %s", eventListenerFactory.getName());
            eventListenerManager.addEventListenerFactory(eventListenerFactory);
        }

        for (GroupProviderFactory groupProviderFactory : plugin.getGroupProviderFactories()) {
            log.info("Registering group provider %s", groupProviderFactory.getName());
            groupProviderManager.addGroupProviderFactory(groupProviderFactory);
        }

        for (ExchangeManagerFactory exchangeManagerFactory : plugin.getExchangeManagerFactories()) {
            log.info("Registering exchange manager %s", exchangeManagerFactory.getName());
            exchangeManagerRegistry.addExchangeManagerFactory(exchangeManagerFactory);
        }
    }

    public static PluginClassLoader createClassLoader(String pluginName, List<URL> urls)
    {
        ClassLoader parent = PluginManager.class.getClassLoader();
        return new PluginClassLoader(pluginName, urls, parent, SPI_PACKAGES);
    }

    public interface PluginsProvider
    {
        void loadPlugins(Loader loader, ClassLoaderFactory createClassLoader);

        interface Loader
        {
            void load(String description, Supplier<PluginClassLoader> getClassLoader)
                    throws Exception;
        }

        interface ClassLoaderFactory
        {
            PluginClassLoader create(String pluginName, List<URL> urls);
        }
    }
}
