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

import com.google.inject.Inject;
import io.trino.plugin.hive.ForHiveFunction;
import io.trino.spi.function.HiveFunctionParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import static io.trino.plugin.hive.functions.FunctionRegistry.getFunctionInfo;
import static io.trino.plugin.hive.functions.FunctionRegistry.registry;
import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/7 9:05
 */
public class DynamicHiveFunctionParser
        implements HiveFunctionParser
{
    private final ClassLoader classLoader;

    @Inject
    public DynamicHiveFunctionParser(@ForHiveFunction ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public void registryFunction(String functionName)
    {
        registry(functionName);
    }

    @Override
    public Class<?> getClass(String functionName)
            throws ClassNotFoundException
    {
        try {
            return getFunctionInfo(functionName).getFunctionClass();
        } catch (SemanticException | NullPointerException e) {
            throw new ClassNotFoundException("Class of function " + functionName + " not found", e);
        }
    }
}
