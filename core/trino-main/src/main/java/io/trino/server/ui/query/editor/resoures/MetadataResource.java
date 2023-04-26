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
package io.trino.server.ui.query.editor.resoures;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.client.Column;
import io.trino.server.security.ResourceSecurity;
import io.trino.server.ui.query.editor.metadata.ColumnCache;
import io.trino.server.ui.query.editor.metadata.PreviewTableCache;
import io.trino.server.ui.query.editor.metadata.SchemaCache;
import io.trino.server.ui.query.editor.protocol.CatalogSchema;
import io.trino.server.ui.query.editor.protocol.Table;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/4/12 9:58
 */
@Path("/api/metadata")
public class MetadataResource
{
    private final SchemaCache schemaCache;
    private final PreviewTableCache previewTableCache;
    private final ColumnCache columnCache;
    private final String defaultUser = "trino";

    @Inject
    public MetadataResource(SchemaCache schemaCache, PreviewTableCache previewTableCache, ColumnCache columnCache)
    {
        this.schemaCache = schemaCache;
        this.previewTableCache = previewTableCache;
        this.columnCache = columnCache;
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("catalogs")
    public Response getCatalogs(
            @Context HttpServletRequest servletRequest)
            throws ExecutionException
    {
        Set<String> result = schemaCache.queryCatalogs(defaultUser);
        return Response.ok(result).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("schemas")
    public Response getSchemas(
            @Context HttpServletRequest servletRequest)
            throws ExecutionException
    {
        ImmutableList<CatalogSchema> result = schemaCache.querySchemas(defaultUser);
        return Response.ok(result).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{catalog}/schemas")
    public Response getSchemasFromCatalog(
            @PathParam("catalog") String catalogName,
            @Context HttpServletRequest servletRequest)
            throws ExecutionException
    {
        CatalogSchema catalogSchema = schemaCache.querySchemas(catalogName,defaultUser);
        return Response.ok(catalogSchema).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{catalog}/{schema}/tables")
    public Response getTables(
            @PathParam("catalog") String catalogName,
            @PathParam("schema") String schemaName,
            @Context HttpServletRequest servletRequest)
    {
        ImmutableList<Table> result = schemaCache.queryTables(catalogName, schemaName, defaultUser);
        return Response.ok(result).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{catalog}/{schema}/{table}/columns")
    public Response getTableColumns(
            @PathParam("catalog") String catalogName,
            @PathParam("schema") String schemaName,
            @PathParam("table") String tableName,
            @Context HttpServletRequest servletRequest)
    {
        ImmutableList<Column> result = columnCache.getColumns(catalogName, schemaName, tableName, defaultUser);
        return Response.ok(result).build();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{catalog}/{schema}/{table}/preview")
    public Response getTablePreview(
            @PathParam("catalog") String catalogName,
            @PathParam("schema") String schemaName,
            @PathParam("table") String tableName,
            @Context HttpServletRequest servletRequest)
    {
        List<List<Object>> result = previewTableCache.getPreviewLimit(catalogName, schemaName, tableName, defaultUser);
        return Response.ok(result).build();
    }
}
