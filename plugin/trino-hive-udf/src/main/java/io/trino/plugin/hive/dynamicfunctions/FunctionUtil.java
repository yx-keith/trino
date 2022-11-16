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

import static io.trino.plugin.hive.dynamicfunctions.RecognizedFunctions.isFunctionRecognized;
import static io.trino.spi.StandardErrorCode.*;
import static java.lang.String.format;

public class FunctionUtil
{
    private String funcName;
    private String className;
    private Class<?> clazz;
    private ClassLoader classLoader;

    public FunctionUtil(FunctionInfo functionInfo, ClassLoader classLoader)
    {
        this.classLoader = classLoader;
        this.funcName = functionInfo.getFunctionName();
        this.className = functionInfo.getFunctionClassName();
        this.initClazz();
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
