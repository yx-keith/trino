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
package io.trino.spi.function;

import java.util.Optional;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/11/30 10:21
 */
public final class DynamicHiveFunctionInfo
{
    private final String schemaName;
    private final String functionName;
    private final String className;
    private final String resourceUri;
    private final int createTime;
    private SqlFunction dynamicHiveFunction;

    public DynamicHiveFunctionInfo(String schemaName, String functionName, String className, String resourceUri, int createTime)
    {
        this.schemaName = schemaName;
        this.functionName = functionName;
        this.className = className;
        this.resourceUri = resourceUri;
        this.createTime = createTime;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getFunctionName()
    {
        return functionName;
    }

    public String getClassName()
    {
        return className;
    }

    public String getResourceUri()
    {
        return resourceUri;
    }

    public int getCreateTime()
    {
        return createTime;
    }

    public SqlFunction getDynamicHiveFunction()
    {
        return dynamicHiveFunction;
    }

    public void setDynamicHiveFunction(SqlFunction dynamicHiveFunction)
    {
        this.dynamicHiveFunction = dynamicHiveFunction;
    }

    @Override
    public String toString() {
        return "DynamicHiveFunctionInfo{" +
                "schemaName='" + schemaName + '\'' +
                ", functionName='" + functionName + '\'' +
                ", className='" + className + '\'' +
                ", resourceUri='" + resourceUri + '\'' +
                ", createTime=" + createTime +
                '}';
    }
}
