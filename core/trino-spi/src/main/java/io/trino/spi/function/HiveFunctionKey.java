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

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/7 16:22
 */
public final class HiveFunctionKey
{
    private final String catalogName;
    private final String schemaname;
    private final String functionName;

    public HiveFunctionKey(String catalogName, String schemaname, String functionName)
    {
        this.catalogName = catalogName;
        this.schemaname = schemaname;
        this.functionName = functionName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaname()
    {
        return schemaname;
    }

    public String getFunctionName()
    {
        return functionName;
    }
}
