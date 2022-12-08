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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveFunctionClassLoader;
import io.trino.plugin.hive.dynamicfunctions.*;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.function.FunctionNamespaceManagerFactory;
import io.trino.spi.function.DynamicHiveFunctionInfo;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.hive.dynamicfunctions.DynamicHiveScalarFunction.EVALUATE_METHOD_NAME;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveFunctionPlugin
        implements Plugin
{
    private static final Logger log = Logger.get(HiveFunctionPlugin.class);
    private static final Splitter SPLITTER = Splitter.on(',').limit(3).trimResults().omitEmptyStrings();
    private final Map<String, StaticHiveFunctionInfo> functionInfoCache = new ConcurrentHashMap<>();
    private final Map<URL, HiveFunctionClassLoader> classLoaderCache = new ConcurrentHashMap<>();
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("io.airlift.units.")
            .add("io.trino.hive.")
            .add("org.apache.hadoop.hive.")
            .build();

    private String funcPropFilePath;
    private File HIVE_UDF_DIR;
    private boolean maxFunctionRunningTimeEnable;
    private long maxFuncRunningTimeInSec;
    private int functionRunningThreadPoolSize;

    @Override
    public void initHiveFunction(String functionDir, String propFilePath)
            throws Exception
    {
        this.funcPropFilePath = requireNonNull(propFilePath, "propFilePath is null.");
        this.HIVE_UDF_DIR = new File(functionDir);
        initFunctionClassloader();
    }

    private void initFunctionClassloader()
            throws Exception
    {
        initFunctionInfo();
        functionInfoCache.forEach((function, functionInfo) -> {
            URL url = functionInfo.getLocalUrl();
            classLoaderCache.putIfAbsent(url, new HiveFunctionClassLoader(url, this.getClass().getClassLoader(), SPI_PACKAGES));
            HiveFunctionClassLoader classLoader = classLoaderCache.get(url);
            functionInfo.setClassLoader(classLoader);
            log.info("hive function %s's classloader %s initialized successful, url is: %s.", functionInfo.getHiveFunctionInfo().getFunctionName(), classLoader, url.toString());
        });
    }

    public void initFunctionInfo()
            throws Exception
    {
        List<String> lines = loadFunctionProperties();
        for (int linenumber = 1; linenumber <= lines.size(); linenumber++) {
            String line = lines.get(linenumber - 1).trim();
            if(line.isEmpty()) {
                continue;
            }

            List<String> parts = SPLITTER.splitToList(line);
            if (parts.size() != 3) {
                throw invalifFile(linenumber, "Expected three parts for the udf.properties file", null);
            }

            String funcName = parts.get(0);
            String className = parts.get(1);
            String jarName = parts.get(2);
            URL localUrl = new File(this.HIVE_UDF_DIR + File.separator + jarName).toURI().toURL();
            DynamicHiveFunctionInfo dynamicHiveFunctionInfo = new DynamicHiveFunctionInfo(funcName, className, jarName, Integer.MAX_VALUE);
            StaticHiveFunctionInfo functionInfo = new StaticHiveFunctionInfo(dynamicHiveFunctionInfo, localUrl, null);
            functionInfoCache.put(funcName, functionInfo);
        }
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .build();
    }

    @Override
    public Set<Object> getHiveUdfFunctions()
    {
        Set<Object> functions = new HashSet<>();
        if(functionInfoCache.isEmpty()) {
            return functions;
        }

        functionInfoCache.forEach((function, functionInfo) -> {
            ClassLoader functionClassLoader = functionInfo.getClassLoader();
            try {
                RecognizedFunctions.addRecognizedFunction(functionInfo.getHiveFunctionInfo().getClassName());
                FunctionUtil funcUtil = new FunctionUtil(functionInfo, functionClassLoader);
                Method[] methods = funcUtil.getClazz().getMethods();
                for (Method method : methods) {
                    if (method.getName().equals(EVALUATE_METHOD_NAME)) {
                        functions.add(createDynamicHiveScalarFunction(funcUtil, method, functionClassLoader));
                    }
                }
            } catch (TrinoException e) {
                log.error("Cannot load function: %s, with exception %s", functionInfo, e);
            }
        });
        return functions;
    }

    private DynamicHiveScalarFunction createDynamicHiveScalarFunction(FunctionUtil funcUtil, Method method, ClassLoader classLoader)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return new DynamicHiveScalarFunction(funcUtil, method.getGenericParameterTypes(), method.getGenericReturnType(),
                    classLoader, this.maxFunctionRunningTimeEnable, this.maxFuncRunningTimeInSec, this.functionRunningThreadPoolSize);
        }
    }

    private List<String> loadFunctionProperties()
    {
        List<String> lines;
        try {
            lines = Files.readAllLines(new File(this.funcPropFilePath).toPath());
        } catch (IOException e) {
            log.info("Loading function properties file error!");
            throw new RuntimeException(e);
        }
        return lines;
    }

    private static TrinoException invalifFile(int lineNumber, String message, Throwable cause)
    {
        return new TrinoException(CONFIGURATION_INVALID, format("Error in udf.properties file line %s: %s", lineNumber, message), cause);
    }

    @Override
    public Iterable<FunctionNamespaceManagerFactory> getFunctionNamespaceManagerFactories() {
        return ImmutableList.of(new HiveFunctionNamespaceManagerFactory(getClassLoader()));
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if(classLoader == null) {
            classLoader = HiveFunctionPlugin.class.getClassLoader();
        }
        return classLoader;
    }

    @Override
    public void setMaxFunctionRunningTimeInSec(long time)
    {
        this.maxFuncRunningTimeInSec = time;
    }

    @Override
    public void setMaxFunctionRunningTimeEnable(boolean enable)
    {
        this.maxFunctionRunningTimeEnable = enable;
    }

    @Override
    public void setFunctionRunningThreadPoolSize(int size)
    {
        this.functionRunningThreadPoolSize = size;
    }
}
