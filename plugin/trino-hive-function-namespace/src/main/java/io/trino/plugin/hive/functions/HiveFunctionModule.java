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

import com.google.inject.*;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.ForHiveFunction;
import io.trino.spi.function.FunctionNamespaceManager;
import io.trino.spi.function.HiveFunctionParser;

import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/5 14:51
 */
public class HiveFunctionModule
    extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final ClassLoader classLoader;

    public HiveFunctionModule(String catalogName, ClassLoader classLoader)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(FunctionNamespaceManager.class).to(HiveFunctionNamespaceManager.class).in(Scopes.SINGLETON);
        binder.bind(HiveFunctionParser.class).to(DynamicHiveFunctionParser.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForHiveFunction
    public ClassLoader getClassLoader()
    {
        return classLoader;
    }
}
