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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Objects;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/4/12 15:57
 */
@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogSchema
{
    private final String catalogName;
    private final List<String> schemas;

    @JsonCreator
    public CatalogSchema(@JsonProperty("catalogName") String catalogName,
                         @JsonProperty("schemas") List<String> schemas)
    {
        this.catalogName = catalogName;
        this.schemas = schemas;
    }

    @JsonProperty
    public List<String> getSchemas()
    {
        return schemas;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogSchema other = (CatalogSchema) o;
        return Objects.equals(catalogName, other.catalogName) &&
                Objects.equals(schemas, other.schemas);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemas);
    }
}
