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
package io.trino.spi.collect;

import javax.annotation.CheckForNull;

import static java.lang.String.format;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/6 19:03
 */
public class Preconditions
{
    public static void checkArgument(boolean expression, @CheckForNull Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public static void checkArgument(boolean argument, String format, Object... args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format(format, args));
        }
    }
}
