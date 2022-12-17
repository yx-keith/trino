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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.dynamicfunctions.DynamicFunctionsConstants;
import io.trino.spi.TrinoException;
import io.trino.spi.type.*;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.*;

import static io.trino.plugin.hive.dynamicfunctions.utils.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*;

public class HiveTypeTranslator
{
    private HiveTypeTranslator()
    {
    }

    static TypeSignature fromHiveTypeInfo(TypeInfo typeInfo)
    {
        return fromHiveTypeInfoLimitDepth(typeInfo, DynamicFunctionsConstants.MAX_HIVE_TYPE_STRUCT_LEVEL);
    }

    private static TypeSignature fromHiveTypeInfoLimitDepth(TypeInfo typeInfo, int maxDepth)
    {
        int nowDepth = maxDepth - 1;
        if (nowDepth <= 0) {
            throw new TrinoException(NOT_SUPPORTED, "Hive type nested structure exceed the limit");
        }
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                Type primitiveType = fromHivePrimitiveTypeInfo((PrimitiveTypeInfo) typeInfo);
                if (primitiveType == null) {
                    break;
                }
                return primitiveType.getTypeSignature();
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                TypeSignature keyType = fromHiveTypeInfoLimitDepth(mapTypeInfo.getMapKeyTypeInfo(), nowDepth);
                TypeSignature valueType = fromHiveTypeInfoLimitDepth(mapTypeInfo.getMapValueTypeInfo(), nowDepth);
                return new TypeSignature(
                        StandardTypes.MAP,
                        ImmutableList.of(TypeSignatureParameter.typeParameter(keyType), TypeSignatureParameter.typeParameter(valueType)));
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                TypeSignature elementType = fromHiveTypeInfoLimitDepth(listTypeInfo.getListElementTypeInfo(), nowDepth);
                return new TypeSignature(
                        StandardTypes.ARRAY,
                        ImmutableList.of(TypeSignatureParameter.typeParameter(elementType)));
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", typeInfo));
    }

    public static Type fromHivePrimitiveTypeInfo(PrimitiveTypeInfo typeInfo)
    {
        return fromHivePrimitiveTypeInfo(typeInfo, DEFAULT_PRECISION);
    }

    static Type fromHivePrimitiveTypeInfo(PrimitiveTypeInfo typeInfo, HiveTimestampPrecision timestampPrecision)
    {
        switch (typeInfo.getPrimitiveCategory()) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return TINYINT;
            case SHORT:
                return SMALLINT;
            case INT:
                return INTEGER;
            case LONG:
                return BIGINT;
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return createUnboundedVarcharType();
            case VARCHAR:
                return createVarcharType(((VarcharTypeInfo) typeInfo).getLength());
            case CHAR:
                return createCharType(((CharTypeInfo) typeInfo).getLength());
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case BINARY:
                return VARBINARY;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                return createDecimalType(decimalTypeInfo.precision(), decimalTypeInfo.scale());
            default:
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive primitive type: %s", typeInfo));
        }
    }

    static TypeInfo translateToHiveTypeInfo(Type type)
    {
        return limitedDepthTranslateToHiveTypeInfo(type, DynamicFunctionsConstants.MAX_HIVE_TYPE_STRUCT_LEVEL);
    }

    private static TypeInfo limitedDepthTranslateToHiveTypeInfo(Type type, int maxDepth)
    {
        int nowDepth = maxDepth - 1;
        if (nowDepth <= 0) {
            throw new TrinoException(NOT_SUPPORTED, "Hive type nested structure exceed the limit");
        }
        if (BOOLEAN.equals(type)) {
            return booleanTypeInfo;
        }
        if (BIGINT.equals(type)) {
            return longTypeInfo;
        }
        if (INTEGER.equals(type)) {
            return intTypeInfo;
        }
        if (SMALLINT.equals(type)) {
            return shortTypeInfo;
        }
        if (TINYINT.equals(type)) {
            return byteTypeInfo;
        }
        if (REAL.equals(type)) {
            return floatTypeInfo;
        }
        if (DOUBLE.equals(type)) {
            return doubleTypeInfo;
        }
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return stringTypeInfo;
            }
            if (varcharType.getBoundedLength() <= HiveVarchar.MAX_VARCHAR_LENGTH) {
                return getVarcharTypeInfo(varcharType.getBoundedLength());
            }
            throw new TrinoException(NOT_SUPPORTED,
                    format("Unsupported Hive type: %s. Supported VARCHAR types: VARCHAR(<=%d), VARCHAR.",
                            type, HiveVarchar.MAX_VARCHAR_LENGTH));
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            int charLength = charType.getLength();
            if (charLength <= HiveChar.MAX_CHAR_LENGTH) {
                return getCharTypeInfo(charLength);
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type: %s." +
                            " Supported CHAR types: CHAR(<=%d).",
                    type, HiveChar.MAX_CHAR_LENGTH));
        }
        if (VARBINARY.equals(type)) {
            return binaryTypeInfo;
        }
        if (DATE.equals(type)) {
            return dateTypeInfo;
        }
        if (TIMESTAMP.equals(type)) {
            return timestampTypeInfo;
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        }
        if (isArrayType(type)) {
            TypeInfo elementType = limitedDepthTranslateToHiveTypeInfo(type.getTypeParameters().get(0), nowDepth);
            return getListTypeInfo(elementType);
        }
        if (isMapType(type)) {
            TypeInfo keyType = limitedDepthTranslateToHiveTypeInfo(type.getTypeParameters().get(0), nowDepth);
            TypeInfo valueType = limitedDepthTranslateToHiveTypeInfo(type.getTypeParameters().get(1), nowDepth);
            return getMapTypeInfo(keyType, valueType);
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", type));
    }

    private static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    private static boolean isMapType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }
}
