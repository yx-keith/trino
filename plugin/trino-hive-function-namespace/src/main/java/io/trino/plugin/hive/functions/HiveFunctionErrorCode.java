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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static java.lang.String.format;
import static io.trino.spi.ErrorType.EXTERNAL;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/7 9:44
 */
public enum HiveFunctionErrorCode
        implements ErrorCodeSupplier
{
    HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE(0, EXTERNAL),
    ;


    private final ErrorCode errorCode;

    HiveFunctionErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0110_0000, "hiveFunctionError", type);
    }

    public static TrinoException unsupportedFunctionType(Class<?> clazz)
    {
       return new TrinoException(HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE,
               format("Unsupported function type %s / %s", clazz.getName(), clazz.getSuperclass().getName()));
    }

    public static TrinoException functionNotFund(String name)
    {
        return new TrinoException(FUNCTION_NOT_FOUND, format("Function %s not registered", name));
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
