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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestHiveTableHandle
{
    private final JsonCodec<HiveTableHandle> codec = JsonCodec.jsonCodec(HiveTableHandle.class);

    @Test
    public void testRoundTrip()
    {
        HiveTableHandle expected = new HiveTableHandle("schema", "table", ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());

        String json = codec.toJson(expected);
        HiveTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
    }
}
