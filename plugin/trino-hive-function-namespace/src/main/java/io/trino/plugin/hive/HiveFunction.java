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

import io.trino.spi.function.FunctionMetadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.function.Signature;
import io.trino.spi.function.SqlFunction;

import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/5 15:34
 */
public abstract class HiveFunction
        implements SqlFunction
{
    private final QualifiedObjectName name;
    private final Signature signature;
    private final boolean hidden;
    private final boolean deterministic;
    private final boolean calledOnNullInput;
    private final String description;

    public HiveFunction(QualifiedObjectName name,
                        Signature signature,
                        boolean hidden,
                        boolean deterministic,
                        boolean calledOnNullInput,
                        String description)
    {
        this.name = requireNonNull(name, "name is null");
        this.signature = requireNonNull(signature, "signature is null");
        this.hidden = hidden;
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
        this.description = requireNonNull(description, "description is null");
    }

    public QualifiedObjectName getName()
    {
        return name;
    }


    public Signature getSignature()
    {
        return signature;
    }


    public boolean isDeterministic()
    {
        return deterministic;
    }


    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }


    public String getDescription()
    {
        return description;
    }

    public abstract FunctionMetadata getFunctionMetadata();
}
