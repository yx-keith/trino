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

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.*;
import io.trino.operator.scalar.ChoicesScalarFunctionImplementation;
import io.trino.operator.scalar.ScalarFunctionImplementation;
import io.trino.plugin.hive.dynamicfunctions.utils.TrinoTypeUtil;

import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;

import io.trino.spi.function.*;
import io.trino.spi.type.Type;
import io.trino.util.Reflection;
import org.apache.commons.lang3.ClassUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.hive.dynamicfunctions.utils.TrinoTypeUtil.getTypeSignature;
import static io.trino.plugin.hive.dynamicfunctions.utils.TrinoTypeUtil.getTypeSignatures;
import static io.trino.plugin.hive.dynamicfunctions.utils.HiveObjectTranslator.translateFromHiveObject;
import static io.trino.plugin.hive.dynamicfunctions.utils.HiveObjectTranslator.serializeObject;
import static io.trino.spi.StandardErrorCode.*;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import static java.lang.String.format;

public class DynamicHiveScalarFunction
        extends DynamicSqlScalarFunction
{
    public static final String EVALUATE_METHOD_NAME = "evaluate";
    private static final int EVALUATE_METHOD_PARAM_LENGTH = 5;
    private ExecutorService executor;
    private FunctionUtil funcUtil;
    private java.lang.reflect.Type[] paramJavaTypes;
    private java.lang.reflect.Type returnJavaTypes;
    private Type[] paramTrinoTypes;
    private Type returnTrinoType;
    private ClassLoader classLoader;
    private boolean maxFuncRunningTimeEnable;
    private long maxFuncRunningTimeInSec;

    public DynamicHiveScalarFunction(FunctionUtil funcUtil, java.lang.reflect.Type[] genericParameterTypes,
                                     java.lang.reflect.Type genericReturnType, ClassLoader classLoader, boolean maxFuncRunningTimeEnable,
                                     long maxFuncRunningTimeInSec, int functionRunningThreadPoolSize)
    {
        this.funcUtil = funcUtil;
        this.paramJavaTypes = genericParameterTypes;
        this.returnJavaTypes = genericReturnType;
        this.classLoader = classLoader;
        this.maxFuncRunningTimeEnable = maxFuncRunningTimeEnable;
        if (maxFuncRunningTimeEnable) {
            this.maxFuncRunningTimeInSec = maxFuncRunningTimeInSec;
            this.executor = Executors.newFixedThreadPool(functionRunningThreadPoolSize);
        }
        setFunctionMetadata(build(funcUtil, genericParameterTypes, genericReturnType));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundSignature boundSignature)
    {
        this.paramTrinoTypes = getTypeSignatures(paramJavaTypes).stream().map(TrinoTypeUtil::getType)
                .toArray(Type[]::new);
        this.returnTrinoType = TrinoTypeUtil.getType(getTypeSignature(returnJavaTypes));
        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                NULLABLE_RETURN,
                getNullableArgumentConventions(),
                getMethodHandle());
    }

    private List<InvocationArgumentConvention> getNullableArgumentConventions()
    {
        List<InvocationArgumentConvention> nullableArgConventions = new ArrayList<>(paramTrinoTypes.length);
        for(Type type : paramTrinoTypes) {
            nullableArgConventions.add(type.getJavaType().isPrimitive() ? BOXED_NULLABLE : NEVER_NULL);
        }
        return nullableArgConventions;
    }

    private MethodHandle getMethodHandle()
    {
        MethodHandle genericMethodHandle = Reflection.methodHandle(DynamicHiveScalarFunction.class,
                "invokeHive", getArgumentTypes(paramTrinoTypes, true)).bindTo(this);
        Class<?> returnType = ClassUtils.primitiveToWrapper(returnTrinoType.getJavaType());
        Class<?>[] paramTypes = getArgumentTypes(paramTrinoTypes, false);
        MethodType specificMethodType = MethodType.methodType(returnType, paramTypes);
        return MethodHandles.explicitCastArguments(genericMethodHandle, specificMethodType);
    }

    private Class<?>[] getArgumentTypes(Type[] argTypes, boolean isGeneric)
    {
        Class<?>[] argumentTypes = new Class<?>[argTypes.length];
        for (int i = 0; i < argTypes.length; i++) {
            argumentTypes[i] = isGeneric ? Object.class : ClassUtils.primitiveToWrapper(argTypes[i].getJavaType());
        }
        return argumentTypes;
    }

    @UsedByGeneratedCode
    public Object invokeHive()
    {
        return evaluate();
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj)
    {
        return evaluate(obj);
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj, Object obj1)
    {
        return evaluate(obj, obj1);
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj, Object obj1, Object obj2)
    {
        return evaluate(obj, obj1, obj2);
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj, Object obj1, Object obj2, Object obj3)
    {
        return evaluate(obj, obj1, obj2, obj3);
    }

    @UsedByGeneratedCode
    public Object invokeHive(Object obj, Object obj1, Object obj2, Object obj3, Object obj4)
    {
        return evaluate(obj, obj1, obj2, obj3, obj4);
    }

    private Object evaluate(Object... objs)
    {
        if (objs.length > EVALUATE_METHOD_PARAM_LENGTH) {
            throw new TrinoException(NOT_SUPPORTED,
                    format("Cannot invoke %s for function %s as %s parameters are not supported.",
                            EVALUATE_METHOD_NAME, funcUtil.getClassName(), objs.length));
        }
        Object[] hiveObjs = new Object[objs.length];
        for (int i = 0; i < objs.length; i++) {
            hiveObjs[i] = serializeObject(this.paramTrinoTypes[i], objs[i], this.paramJavaTypes[i]);
        }
        Method method = getEvaluateMethod(objs.length);
        Object result;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            result = invokeFunction(method, hiveObjs);
        }
        return translateFromHiveObject(this.returnTrinoType, result);
    }

    private Method getEvaluateMethod(int paramNum)
    {
        Class<?>[] paramTypes = new Class[paramNum];
        for (int i = 0; i < paramNum; i++) {
            java.lang.reflect.Type paramType = this.paramJavaTypes[i];
            paramTypes[i] = paramType instanceof ParameterizedType
                    ? (Class<?>) ((ParameterizedType) paramType).getRawType()
                    : (Class<?>) paramType;
        }
        try {
            return this.funcUtil.getClazz().getMethod(EVALUATE_METHOD_NAME, paramTypes);
        }
        catch (NoSuchMethodException e) {
            throw new TrinoException(NOT_FOUND, format("Cannot find %s for function %s with signature: %s.",
                    EVALUATE_METHOD_NAME, funcUtil.getClassName(), this.getFunctionMetadata().getSignature()));
        }
    }

    private Object invokeFunction(Method method, Object[] hiveObjs)
    {
        if (!maxFuncRunningTimeEnable) {
            return getResult(method, hiveObjs);
        }
        else {
            Future<Object> future = executor.submit(() ->
                    getResult(method, hiveObjs));
            try {
                return future.get(this.maxFuncRunningTimeInSec, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to get results for method %s of" +
                        "  function %s, with exception: %s.", EVALUATE_METHOD_NAME, funcUtil.getInstance(), e));
            }
        }
    }

    private Object getResult(Method method, Object[] hiveObjs)
    {
        try {
            return method.invoke(this.funcUtil.getInstance(), hiveObjs);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Cannot invoke %s for function %s," +
                    " with exception: %s.", EVALUATE_METHOD_NAME, funcUtil.getInstance(), e));
        }
    }

    @Override
    public boolean equals(Object that)
    {
        return this.getFunctionMetadata().getSignature().equals(((DynamicHiveScalarFunction) that).getFunctionMetadata().getSignature());
    }

    @Override
    public int hashCode()
    {
        return this.getFunctionMetadata().getSignature().hashCode();
    }

    private FunctionMetadata build(FunctionUtil funcUtil, java.lang.reflect.Type[] genericParameterTypes, java.lang.reflect.Type genericReturnType)
    {
        List<Boolean> argumentNullability = new ArrayList<>();
        for (int i = 0; i < genericParameterTypes.length; i++) {
            argumentNullability.add(false);
        }

        return new FunctionMetadata(
                new Signature(funcUtil.getFunctionName(),
                        TrinoTypeUtil.getTypeSignature(genericReturnType),
                        TrinoTypeUtil.getTypeSignatures(genericParameterTypes)),
                new FunctionNullability(true, argumentNullability),
                false,
                true,
                "hive udf: " + funcUtil.getFunctionName(),
                SCALAR);
    }
}
