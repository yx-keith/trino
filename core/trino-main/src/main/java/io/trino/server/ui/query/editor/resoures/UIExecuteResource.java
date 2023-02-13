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
import io.trino.security.AccessControl;
import io.trino.security.AccessControlUtil;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.ProtocolConfig;
import io.trino.server.SessionContext;
import io.trino.server.ui.query.editor.QueryEditorConfig;
import io.trino.server.ui.query.editor.execution.ExecutionClient;
import io.trino.server.ui.query.editor.protocol.ExecutionRequest;
import io.trino.server.ui.query.editor.protocol.ExecutionStatus.ExecutionError;
import io.trino.server.ui.query.editor.protocol.ExecutionStatus.ExecutionSuccess;
import io.trino.spi.security.Identity;
import org.joda.time.Duration;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.trino.server.HttpRequestSessionContextFactory.AUTHENTICATED_IDENTITY;
import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/18 22:22
 */

@Path("/api")
public class UIExecuteResource
{
    private final ExecutionClient client;
    private final QueryEditorConfig config;
    private final AccessControl accessControl;
    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final Optional<String> alternateHeaderName;

    @Inject
    public UIExecuteResource(HttpRequestSessionContextFactory sessionContextFactory,
                             ExecutionClient client,
                             QueryEditorConfig config,
                             AccessControl accessControl,
                             ProtocolConfig protocolConfig)
    {
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.client = client;
        this.config = config;
        this.accessControl = accessControl;
        this.alternateHeaderName = requireNonNull(protocolConfig, "protocolConfig is null").getAlternateHeaderName();
    }

    @PUT
    @Path("execute")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response execute(ExecutionRequest request,
                            @Context HttpServletRequest servletRequest,
                            @Context HttpHeaders httpHeaders,
                            @Context UriInfo uriInfo)
    {
        Optional<String> remoteAddress = Optional.ofNullable(servletRequest.getRemoteAddr());
        Optional<Identity> identity = Optional.ofNullable((Identity) servletRequest.getAttribute(AUTHENTICATED_IDENTITY));
        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();
        SessionContext sessionContext = sessionContextFactory.createSessionContext(headers, alternateHeaderName, remoteAddress, identity);
        String user = AccessControlUtil.getUser(accessControl, sessionContext);
        if (user != null) {
            final List<UUID> uuids = client.runQuery(
                    request,
                    user,
                    Duration.millis(config.getExecutionTimeout().toMillis()),
                    servletRequest);

            List<ExecutionSuccess> successList = uuids.stream().map(ExecutionSuccess::new).collect(Collectors.toList());
            return Response.ok(successList).build();
        }
        return Response.status(Response.Status.NOT_FOUND)
                .entity(new ExecutionError("Currently not able to execute"))
                .build();
    }
}
