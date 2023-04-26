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

import com.google.inject.Inject;
import com.opencsv.CSVReader;
import io.airlift.log.Logger;
import io.trino.server.security.ResourceSecurity;
import io.trino.server.ui.query.editor.store.files.ExpiringFileStore;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/24 17:21
 */
@Path("/api/preview")
public class ResultsPreviewResource
{
    private static final Logger LOG = Logger.get(ResultsPreviewResource.class);
    private final ExpiringFileStore fileStore;

    @Inject
    public ResultsPreviewResource(ExpiringFileStore fileStore)
    {
        this.fileStore = fileStore;
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFile(
            @QueryParam("user") String user,
            @QueryParam("pageNum") Integer pageNum,
            @QueryParam("pageSize") Integer pageSize,
            @QueryParam("fileURI") URI fileURI)
    {
        Optional<String> filterUser = Optional.of(user);
        return getFilePreview(fileURI, pageNum, pageSize, filterUser);
    }

    private Response getFilePreview(URI fileURI, Integer pageNum, Integer pageSize, Optional<String> user)
    {
        String fileName = getFilename(fileURI);
        final File file = fileStore.get(fileName, user);
        try {
            if (file == null) {
                throw new FileNotFoundException(fileName + " could not be found");
            }
            else {
                try (final CSVReader reader = new CSVReader(new FileReader(file))) {
                    return getPreviewFromCSV(reader, pageNum, pageSize, fileName);
                }
                catch (IOException e) {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                }
            }
        }
        catch (FileNotFoundException e) {
            LOG.warn(e.getMessage());
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    private String getFilename(URI fileURI)
    {
        return fileURI.getPath().substring(fileURI.getPath().lastIndexOf('/') + 1);
    }

    private Response getPreviewFromCSV(CSVReader reader, Integer pageNum, Integer pageSize, String fileName)
    {
        List<String> columns = new ArrayList<>();
        List<List<String>> data = new ArrayList<>();
        try {
            for (final String columnName : reader.readNext()) {
                columns.add(columnName);
            }
            for (String[] line : reader) {
                data.add(Arrays.asList(line));
            }
            int totalLineNum = data.toArray().length;

            if (pageNum == null || pageSize == null || totalLineNum == 0) {
                return Response.ok(new PreviewResponse(columns, data, totalLineNum, fileName)).build();
            }
            if (pageNum <= 0 || pageSize <= 0) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            else if (totalLineNum - pageSize * (pageNum - 1) <= 0) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
            int start = (pageNum - 1) * pageSize;
            int end = Math.min(pageNum * pageSize, totalLineNum);
            List<List<String>> subData = data.subList(start, end);
            return Response.ok(new PreviewResponse(columns, subData, totalLineNum, fileName)).build();
        } catch (IOException e) {
            LOG.error(e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }
}
