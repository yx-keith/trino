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
package io.trino.plugin.hive.functions;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.function.FunctionNamespaceManager;
import io.trino.spi.function.FunctionNamespaceManagerFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/5 16:28
 */
public class HiveFunctionNamespaceManagerFactory
        implements FunctionNamespaceManagerFactory
{
    private final ClassLoader classLoader;
    private static final String NAME = "hive-functions";
    public HiveFunctionNamespaceManagerFactory(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public FunctionNamespaceManager create(String catalogName, Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new HiveFunctionModule(catalogName, classLoader));

            Injector injector = app
                    .nonStrictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();
            return injector.getInstance(FunctionNamespaceManager.class);
        }
    }
}
