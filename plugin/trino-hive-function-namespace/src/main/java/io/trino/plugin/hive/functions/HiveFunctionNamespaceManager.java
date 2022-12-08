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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.hive.ForHiveFunction;
import io.trino.plugin.hive.HiveFunctionClassLoader;
import io.trino.plugin.hive.dynamicfunctions.StaticHiveFunctionInfo;
import io.trino.plugin.hive.dynamicfunctions.DynamicHiveScalarFunction;
import io.trino.plugin.hive.dynamicfunctions.FunctionUtil;
import io.trino.plugin.hive.dynamicfunctions.RecognizedFunctions;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.function.FunctionNamespaceManager;
import io.trino.spi.function.DynamicHiveFunctionInfo;
import io.trino.spi.function.HiveFunctionParser;
import io.trino.spi.function.SqlFunction;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static io.trino.plugin.hive.dynamicfunctions.DynamicHiveScalarFunction.EVALUATE_METHOD_NAME;
import static io.trino.plugin.hive.functions.HiveFunctionErrorCode.functionNotFund;
import static io.trino.plugin.hive.functions.HiveFunctionErrorCode.unsupportedFunctionType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/5 22:13
 */
public class HiveFunctionNamespaceManager
    implements FunctionNamespaceManager
{
    private final ClassLoader classLoader;
    private final Map<URL, HiveFunctionClassLoader> classLoaderCache = new ConcurrentHashMap<>();
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("io.airlift.units.")
            .add("io.trino.hive.")
            .add("org.apache.hadoop.hive.")
            .build();

    private boolean maxFunctionRunningTimeEnable;
    private long maxFuncRunningTimeInSec;
    private int functionRunningThreadPoolSize;
    private final HiveFunctionParser hiveFunctionParser;

    @Inject
    public HiveFunctionNamespaceManager(
            @ForHiveFunction ClassLoader classLoader,
            HiveFunctionParser hiveFunctionParser)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.hiveFunctionParser = requireNonNull(hiveFunctionParser, "hiveFunctionParser is null");
    }

    @Override
    public Optional<SqlFunction> initDynamicHiveFunction(DynamicHiveFunctionInfo dynamicHiveFunctionInfo, String localDir)
    {
        try(ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            String fileName = new File(dynamicHiveFunctionInfo.getResourceUri().trim()).getName();
            URL localUrl;
            try {
                localUrl = new File(format(localDir + "%s" + fileName, File.separatorChar)).toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }

            if (!classLoaderCache.containsKey(localUrl)) {
                classLoaderCache.put(localUrl, new HiveFunctionClassLoader(localUrl, classLoader, SPI_PACKAGES));
            }
            HiveFunctionClassLoader hiveFunctionClassLoader = classLoaderCache.get(localUrl);
            Class<?> functionClass;
            String functionName = dynamicHiveFunctionInfo.getFunctionName();
            try {
                hiveFunctionParser.registryFunction(functionName);
                functionClass = hiveFunctionParser.getClass(functionName);
            } catch (ClassNotFoundException e) {
                throw functionNotFund(functionName);
            }

            if(isAssignableFrom(functionClass, UDF.class)) {
                RecognizedFunctions.addRecognizedFunction(dynamicHiveFunctionInfo.getClassName());
                FunctionUtil funcUtil = new FunctionUtil(dynamicHiveFunctionInfo, hiveFunctionClassLoader);
                Method[] methods = funcUtil.getClazz().getMethods();
                for (Method method : methods) {
                    if (method.getName().equals(EVALUATE_METHOD_NAME)) {
                        return Optional.of(createDynamicHiveScalarFunction(funcUtil, method, hiveFunctionClassLoader));
                    }
                }
            }else {
                throw unsupportedFunctionType(functionClass);
            }
        }
        return Optional.empty();
    }

    private DynamicHiveScalarFunction createDynamicHiveScalarFunction(FunctionUtil funcUtil, Method method, ClassLoader classLoader) {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return new DynamicHiveScalarFunction(funcUtil, method.getGenericParameterTypes(), method.getGenericReturnType(),
                    classLoader, this.maxFunctionRunningTimeEnable, this.maxFuncRunningTimeInSec, this.functionRunningThreadPoolSize);
        }
    }

    private static boolean isAssignableFrom(Class<?> clazz, Class<?>... supers)
    {
        return Stream.of(supers).anyMatch(s -> s.isAssignableFrom(clazz));
    }

    public void setMaxFunctionRunningTimeEnable(boolean maxFunctionRunningTimeEnable)
    {
        this.maxFunctionRunningTimeEnable = maxFunctionRunningTimeEnable;
    }

    public void setMaxFuncRunningTimeInSec(long maxFuncRunningTimeInSec)
    {
        this.maxFuncRunningTimeInSec = maxFuncRunningTimeInSec;
    }

    public void setFunctionRunningThreadPoolSize(int functionRunningThreadPoolSize)
    {
        this.functionRunningThreadPoolSize = functionRunningThreadPoolSize;
    }
}
