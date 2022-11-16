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
package io.trino.plugin.hive;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.hive.dynamicfunctions.DynamicHiveScalarFunction;
import io.trino.plugin.hive.dynamicfunctions.FunctionInfo;
import io.trino.plugin.hive.dynamicfunctions.FunctionUtil;
import io.trino.plugin.hive.dynamicfunctions.RecognizedFunctions;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.hive.dynamicfunctions.DynamicHiveScalarFunction.EVALUATE_METHOD_NAME;
import static java.util.Objects.requireNonNull;

public class HiveUdfPlugin
        implements Plugin
{
    private static final Logger log = Logger.get(HiveUdfPlugin.class);
    private static final Splitter SPLITTER = Splitter.on(',').limit(3).trimResults().omitEmptyStrings();
    private final Map<String, FunctionInfo> functionInfoCache = new ConcurrentHashMap<>();
    private final Map<URL, HiveUdfClassLoader> classLoaderCache = new ConcurrentHashMap<>();
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

    private void initUdfClassLoader()
            throws Exception
    {
        initFunctionInfo();
        functionInfoCache.forEach((function, functionInfo) -> {
            URL url = functionInfo.getLocalUrl();
            if (!classLoaderCache.containsKey(url)) {
                HiveUdfClassLoader classLoader = new HiveUdfClassLoader(url, this.getClass().getClassLoader(), SPI_PACKAGES);
                functionInfo.setFunctionClassLoader(classLoader);
                classLoaderCache.put(url, classLoader);
                log.info("Create hive udf classloader %s, with url: %s.", classLoader, url.toString());
            } else {
                HiveUdfClassLoader classLoader = classLoaderCache.get(url);
                functionInfo.setFunctionClassLoader(classLoader);
                log.info("hive udf classloader %s already exists, url is: %s.", classLoader, url.toString());
            }
        });
    }

    @Override
    public void initHiveUdf(File functionDir, String propFilePath)
            throws Exception
    {
        this.funcPropFilePath = requireNonNull(propFilePath, "propFilePath is null.");
        this.HIVE_UDF_DIR = requireNonNull(functionDir, "externalFunctionsDir is null.");
        initUdfClassLoader();
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
            ClassLoader functionClassLoader = functionInfo.getFunctionClassLoader();
            try {
                RecognizedFunctions.addRecognizedFunction(functionInfo.getFunctionClassName());
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

    private List<String> loadFunctionInfoFromPropertiesFile()
    {
        List<String> functionNames = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(this.funcPropFilePath))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                log.info("Loading external function metadata: %s.", line);
                functionNames.add(line);
            }
        }
        catch (IOException e) {
            log.error("Cannot load function metadata from function properties file %s, with IOException: %s",
                    this.funcPropFilePath, e);
        }
        return functionNames;
    }

    public void initFunctionInfo()
            throws Exception
    {
        for(String functionMetaInfo : loadFunctionInfoFromPropertiesFile()) {
            List<String> parts = SPLITTER.splitToList(functionMetaInfo);
            String funcName = parts.get(0);
            String funcClassName = parts.get(1);
            String funcJarName = parts.get(2);
            URL localUrl = new File(this.HIVE_UDF_DIR + File.separator + funcJarName).toURI().toURL();
            FunctionInfo functionInfo = new FunctionInfo(funcName, funcClassName, funcJarName, localUrl);
            functionInfoCache.put(funcName, functionInfo);
        }
    }

    private DynamicHiveScalarFunction createDynamicHiveScalarFunction(FunctionUtil funcUtil, Method method, ClassLoader classLoader)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return new DynamicHiveScalarFunction(funcUtil, method.getGenericParameterTypes(), method.getGenericReturnType(),
                    classLoader, this.maxFunctionRunningTimeEnable, this.maxFuncRunningTimeInSec, this.functionRunningThreadPoolSize);
        }
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
