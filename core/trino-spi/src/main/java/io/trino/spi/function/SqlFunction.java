package io.trino.spi.function;

import static io.trino.spi.function.FunctionDependencyDeclaration.NO_DEPENDENCIES;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/6 12:26
 */
public interface SqlFunction
{
    FunctionMetadata getFunctionMetadata();

    default FunctionDependencyDeclaration getFunctionDependencies(BoundSignature boundSignature)
    {
        return getFunctionDependencies();
    }

    default FunctionDependencyDeclaration getFunctionDependencies()
    {
        return NO_DEPENDENCIES;
    }
}
