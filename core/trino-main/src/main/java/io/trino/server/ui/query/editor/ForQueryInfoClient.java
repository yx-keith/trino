package io.trino.server.ui.query.editor;

import javax.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/15 15:48
 */
@Retention(RUNTIME)
@Target({FIELD, PARAMETER, METHOD})
@Qualifier
public @interface ForQueryInfoClient
{
}
