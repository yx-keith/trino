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
package io.trino.server.ui.query.editor.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/9 16:04
 */
public class ExecutionStatus
{
    public static class ExecutionSuccess
    {
        @JsonProperty
        public final UUID uuid;

        @JsonCreator
        public ExecutionSuccess(@JsonProperty("uuid") UUID uuid)
        {
            this.uuid = uuid;
        }

        @JsonProperty
        public UUID getUuid()
        {
            return uuid;
        }
    }

    public static class ExecutionError
    {
        @JsonProperty
        public final String message;

        @JsonCreator
        public ExecutionError(@JsonProperty("message") String message)
        {
            this.message = message;
        }

        public String getMessage()
        {
            return message;
        }
    }
}
