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

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/7 9:08
 */
public final class FunctionRegistry
{
    private static final Registry registry = new Registry(true);

    public static void registry(String functionName)
    {
        registry.registerUDF(functionName, UDF.class, false);
    }

    public static FunctionInfo getFunctionInfo(String functionName)
            throws SemanticException
    {
        FunctionInfo functionInfo = registry.getFunctionInfo(functionName);
        return functionInfo;
    }
}
