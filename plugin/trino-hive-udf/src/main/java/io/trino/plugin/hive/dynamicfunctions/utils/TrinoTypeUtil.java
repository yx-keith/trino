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
package io.trino.plugin.hive.dynamicfunctions.utils;

import io.trino.plugin.hive.dynamicfunctions.type.ArrayParametricType;
import io.trino.plugin.hive.dynamicfunctions.type.MapParametricType;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.isPrimitiveJava;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*;

public class TrinoTypeUtil
{
    private static final Pattern DESCRIBE_TYPE_PATTERN = Pattern.compile(
            "(?<type>[a-zA-Z_]+)(\\((?<paramTypes>[a-zA-Z_,]+)?\\))?(\\((?<size>\\d+)(,(?<digits>\\d+))?\\))?");

    private TrinoTypeUtil()
    {
    }

    /**
     * Return a trino type from a type signature
     *
     * @param signature: a type signature
     * @return a trino type
     */
    public static Type getType(TypeSignature signature)
    {
        return getType(signature.toString());
    }

    /**
     * Return a trino type from a type signature information
     *
     * @param signatureInfo: a type signature information
     * @return a trino type
     */
    public static Type getType(String signatureInfo)
    {
        Matcher matcher = DESCRIBE_TYPE_PATTERN.matcher(signatureInfo);
        if (!matcher.matches()) {
            throw new TrinoException(NOT_SUPPORTED, format("Type signature %s is not supported.", signatureInfo));
        }
        String type = matcher.group("type");
        String matchedParamTypes = matcher.group("paramTypes");
        List<String> paramTypes = matchedParamTypes != null ? Arrays.asList(matchedParamTypes.split(","))
                : new ArrayList<>();
        String matchedSize = matcher.group("size");
        int size = matchedSize != null ? Integer.parseInt(matchedSize) : 0;
        String matchedDigits = matcher.group("digits");
        int digits = matchedDigits != null ? Integer.parseInt(matchedDigits) : 0;
        return getType(type, paramTypes, size, digits);
    }

    private static Type getType(String type, List<String> params, int size, int digits)
    {
        switch (type) {
            case StandardTypes.BIGINT:
                return BIGINT;
            case StandardTypes.INTEGER:
                return INTEGER;
            case StandardTypes.SMALLINT:
                return SMALLINT;
            case StandardTypes.TINYINT:
                return TINYINT;
            case StandardTypes.BOOLEAN:
                return BOOLEAN;
            case StandardTypes.DATE:
                return DATE;
            case StandardTypes.DECIMAL:
                return createDecimalType(size, digits);
            case StandardTypes.REAL:
                return REAL;
            case StandardTypes.DOUBLE:
                return DOUBLE;
            case StandardTypes.HYPER_LOG_LOG:
                return HYPER_LOG_LOG;
            case StandardTypes.P4_HYPER_LOG_LOG:
                return P4_HYPER_LOG_LOG;
            case StandardTypes.TIMESTAMP:
                return TIMESTAMP_MILLIS;
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                return TIMESTAMP_TZ_MILLIS;
            case StandardTypes.TIME:
                return TIME_MILLIS;
            case StandardTypes.TIME_WITH_TIME_ZONE:
                return createTimeWithTimeZoneType(3);
            case StandardTypes.VARBINARY:
                return VARBINARY;
            case StandardTypes.CHAR:
                return createCharType(min(size, CharType.MAX_LENGTH));
            case StandardTypes.VARCHAR:
                return size != 0 ? createVarcharType(size) : VARCHAR;
            case StandardTypes.ARRAY:
                return new ArrayParametricType().createType(params);
            case StandardTypes.MAP:
                return new MapParametricType().createType(params);
            default:
                throw new TrinoException(NOT_SUPPORTED, format("Type %s is not supported", type));
        }
    }

    /**
     * Return a list of trino type signatures from n array of Java types
     *
     * @param types: an array of Java types
     * @return a list of trino type signatures
     */
    public static List<TypeSignature> getTypeSignatures(java.lang.reflect.Type[] types)
    {
        List<TypeSignature> signatures = new ArrayList<>(types.length);
        for (java.lang.reflect.Type type : types) {
            signatures.add(getTypeSignature(type));
        }
        return signatures;
    }

    /**
     * Return a trino type signature from a Java type
     *
     * @param type: a Java type
     * @return a trino type signature
     */
    public static TypeSignature getTypeSignature(java.lang.reflect.Type type)
    {
        if (type instanceof Class && isPrimitiveJava((Class<?>) type)) {
            return getTypeSignatureFromPrimitiveJava(type);
        }
        if (type instanceof ParameterizedType) {
            ParameterizedType pType = (ParameterizedType) type;
            if (List.class == pType.getRawType() || ArrayList.class == pType.getRawType()) {
                return getTypeSignatureFromList(pType);
            }
            if (Map.class == pType.getRawType()) {
                return getTypeSignatureFromMap(pType);
            }
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported java type: %s", type));
    }

    private static TypeSignature getTypeSignatureFromPrimitiveJava(java.lang.reflect.Type type)
    {
        return HiveTypeTranslator.translateFromHiveTypeInfo(getPrimitiveTypeInfoFromJavaPrimitive((Class<?>) type));
    }

    private static TypeSignature getTypeSignatureFromList(ParameterizedType pType)
    {
        Class<?> elementType = (Class<?>) pType.getActualTypeArguments()[0];
        return HiveTypeTranslator.translateFromHiveTypeInfo(getListTypeInfo(
                getPrimitiveTypeInfoFromJavaPrimitive(elementType)));
    }

    private static TypeSignature getTypeSignatureFromMap(ParameterizedType pType)
    {
        Class<?> keyType = (Class<?>) pType.getActualTypeArguments()[0];
        Class<?> valueType = (Class<?>) pType.getActualTypeArguments()[1];
        return HiveTypeTranslator.translateFromHiveTypeInfo(getMapTypeInfo(
                getPrimitiveTypeInfoFromJavaPrimitive(keyType),
                getPrimitiveTypeInfoFromJavaPrimitive(valueType)));
    }
}
