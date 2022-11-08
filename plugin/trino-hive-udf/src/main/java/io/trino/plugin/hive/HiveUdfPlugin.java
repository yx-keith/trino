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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.hive.dynamicfunctions.DynamicHiveScalarFunction;
import io.trino.plugin.hive.dynamicfunctions.FunctionMetadata;
import io.trino.plugin.hive.dynamicfunctions.RecognizedFunctions;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.trino.plugin.hive.dynamicfunctions.DynamicHiveScalarFunction.EVALUATE_METHOD_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveUdfPlugin
        implements Plugin
{
    private static final Logger log = Logger.get(HiveUdfPlugin.class);
    private static final File HIVE_UDF_DIR = new File("hive-udf/jars");
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("io.airlift.units.")
            .add("io.trino.hive.")
            .add("org.apache.hadoop.hive.")
            .build();

    private String funcPropFilePath;
    private ClassLoader funcClassLoader;
    private boolean maxFunctionRunningTimeEnable;
    private long maxFuncRunningTimeInSec;
    private int functionRunningThreadPoolSize;

    public HiveUdfPlugin()
    {
        setUdfClassLoader(HIVE_UDF_DIR);
    }

    private void setUdfClassLoader(File hiveUdfDir)
    {
        List<URL> urls = getURLs(hiveUdfDir);
        this.funcClassLoader = new HiveUdfClassLoader(urls, this.getClass().getClassLoader(), SPI_PACKAGES);
        log.info("Create hive udf classloader %s, with urls: %s.", funcClassLoader, urls.toString());
    }

    @Override
    public void setHiveUdfLoadPath(File externalFunctionsDir, String propFilePath)
    {
        this.funcPropFilePath = requireNonNull(propFilePath, "propFilePath is null.");
        if (!HIVE_UDF_DIR.equals(externalFunctionsDir)) {
            setUdfClassLoader(externalFunctionsDir);
        }
    }

    private List<URL> getURLs(File dir)
    {
        List<URL> urls = new ArrayList<>();
        String dirName = dir.getName();
        if (!dir.exists() || !dir.isDirectory()) {
            log.debug("%s doesn't exist or is not a directory.", dirName);
            return urls;
        }
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            log.debug("%s is empty.", dirName);
            return urls;
        }
        for (File file : files) {
            try {
                urls.add(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                log.error("Failed to add %s to URLs of HiveFunctionsClassLoader.", file);
            }
        }
        return urls;
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
        if (this.funcClassLoader == null) {
            return functions;
        }

        for (String funcMetadataInfo : loadFunctionMetadataFromPropertiesFile()) {
            try {
                RecognizedFunctions.addRecognizedFunction(FunctionMetadata.parseFunctionClassName(funcMetadataInfo)[1]);

                FunctionMetadata functionMetadata = new FunctionMetadata(funcMetadataInfo, this.funcClassLoader);
                Method[] methods = functionMetadata.getClazz().getMethods();
                for (Method method : methods) {
                    if (method.getName().equals(EVALUATE_METHOD_NAME)) {
                        functions.add(createDynamicHiveScalarFunction(functionMetadata, method));
                    }
                }
            }
            catch (TrinoException e) {
                log.error("Cannot load function: %s, with exception %s", funcMetadataInfo, e);
            }
        }
        return functions;
    }

    private List<String> loadFunctionMetadataFromPropertiesFile()
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

    private DynamicHiveScalarFunction createDynamicHiveScalarFunction(FunctionMetadata funcMetadata, Method method)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(this.funcClassLoader)) {
            return new DynamicHiveScalarFunction(funcMetadata, method.getGenericParameterTypes(), method.getGenericReturnType(),
                    this.funcClassLoader, this.maxFunctionRunningTimeEnable, this.maxFuncRunningTimeInSec, this.functionRunningThreadPoolSize);
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
