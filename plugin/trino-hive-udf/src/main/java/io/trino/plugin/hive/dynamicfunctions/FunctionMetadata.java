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
package io.trino.plugin.hive.dynamicfunctions;

import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.hive.dynamicfunctions.RecognizedFunctions.isFunctionRecognized;
import static io.trino.spi.StandardErrorCode.*;
import static java.lang.String.format;

public class FunctionMetadata
{
    private static final Pattern FUNCTION_METADATA_PATTERN = Pattern.compile("(.*)\\s+(.*)");

    private String funcName;
    private String className;
    private Class<?> clazz;
    private Map<String, Method> methodByName;
    private ClassLoader classLoader;

    public FunctionMetadata(String metadata, ClassLoader classLoader)
    {
        this.classLoader = classLoader;
        this.funcName = parseFunctionClassName(metadata)[0];
        this.className = parseFunctionClassName(metadata)[1];
        this.initClazz();
        this.methodByName = new HashMap<>();
    }

    // Return [funcName, className]
    public static String[] parseFunctionClassName(String metadata)
    {
        Matcher matcher = FUNCTION_METADATA_PATTERN.matcher(metadata);
        if (!matcher.matches()) {
            throw new TrinoException(NOT_SUPPORTED, format("Cannot recognize function metadata %s.", metadata));
        }

        return new String[] {matcher.group(1).trim(), matcher.group(2).trim()};
    }

    private void initClazz()
    {
        if (!isFunctionRecognized(this.className)) {
            throw new TrinoException(FUNCTION_NOT_FOUND, format("Class name not recognized: %s. " +
                    "Class name must be registered in the RecognizedFunctions first to avoid security risks", this.className));
        }

        try {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
                this.clazz = Class.forName(this.className, false, this.classLoader);
            }
        }
        catch (ClassNotFoundException e) {
            throw new TrinoException(FUNCTION_NOT_FOUND, format("Cannot find function class %s", this.className));
        }
        catch (Throwable t) {
            throw new TrinoException(NOT_FOUND,
                    format("Function class %s may have dependency issues or implementation issues," +
                            " with throwable %s.", this.className, t));
        }
    }

    public String getFunctionName()
    {
        return this.funcName;
    }

    public String getClassName()
    {
        return this.className;
    }

    public Class<?> getClazz()
    {
        return this.clazz;
    }

    public Object getInstance()
    {
        try {
            return this.clazz.getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Cannot create new instance for class %s with exception: %s.", this.clazz, e));
        }
    }
}
