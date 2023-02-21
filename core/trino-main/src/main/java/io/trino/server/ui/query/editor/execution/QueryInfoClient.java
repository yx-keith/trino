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
package io.trino.server.ui.query.editor.execution;

import io.airlift.log.Logger;
import io.trino.client.JsonCodec;
import io.trino.client.JsonResponse;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/8 16:59
 */
public class QueryInfoClient
{
    private static final Logger LOG = Logger.get(QueryInfoClient.class);
    private static final String USER_AGENT_VALUE = QueryInfoClient.class.getSimpleName() +
            "/" +
            firstNonNull(QueryInfoClient.class.getPackage().getImplementationVersion(), "unknown");
    private static final JsonCodec<UIBasicQueryInfo> QUERY_INFO_CODEC = JsonCodec.jsonCodec(UIBasicQueryInfo.class);
    private final OkHttpClient okHttpClient;

    public QueryInfoClient(OkHttpClient okHttpClient)
    {
        this.okHttpClient = okHttpClient;
    }

    public UIBasicQueryInfo getQueryInfo(URI serverUri, String queryId)
    {
        Request.Builder request = prepareRequest(serverUri, queryId);
        JsonResponse<UIBasicQueryInfo> response = JsonResponse.execute(QUERY_INFO_CODEC, okHttpClient, request.build(), OptionalLong.empty());
        if ((response.getStatusCode() != HTTP_OK) || !response.hasValue()) {
            if (response.getStatusCode() != Response.Status.GONE.getStatusCode()) {
                LOG.warn("Error while getting query info! {}", response.getValue());
            }
            return null;
        }
        return response.getValue();
    }

    private Request.Builder prepareRequest(URI uri, String queryId)
    {
        uri = requireNonNull(uri, "infoUri is null");
        HttpUrl url = HttpUrl.get(uri);
        url = url.newBuilder().encodedPath("/v1/query/" + queryId).query(null).build();

        Request.Builder request = new Request.Builder()
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .url(url);
        return request;
    }
}
