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
package io.trino.plugin.mysql.security;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import static io.trino.plugin.mysql.MysqlErrorCode.MYSQL_AUTHORIZE_ERROR;


public class AuthorizeManager
{
    private static final Logger log = Logger.get(AuthorizeManager.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static boolean isPriviledged(String user, String sql, String url)
    {
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(url);
        AuthorizeRequestBody requestBody = new AuthorizeRequestBody("", sql, user);
        String json = null;
        try {
            json = objectMapper.writeValueAsString(requestBody);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        assert json != null;
        StringEntity requestEntity = new StringEntity(json,"UTF-8");
        requestEntity.setContentEncoding("UTF-8");
        requestEntity.setContentType("application/json");
        HttpResponse response = null;
        String data = "";
        try {
            post.setEntity(requestEntity);
            response = client.execute(post);
            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity resEntity = response.getEntity();
                data = EntityUtils.toString(resEntity);
                AuthorizeResponseBody customResponse = deserialize(data);
                if(customResponse.getSuccess() == 0) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("mysql conector authorize error..., response data: " + data);
            assert response != null;
            throw new TrinoException(MYSQL_AUTHORIZE_ERROR, "mysql conector authorize error, status code: " + response.getStatusLine().getStatusCode());
        }
        log.error("mysql conector authorize failed..., response data: " + data);
        throw new TrinoException(MYSQL_AUTHORIZE_ERROR, "mysql conector authorize failed!");
    }

    private static AuthorizeResponseBody deserialize(String json)
    {
        AuthorizeResponseBody response;
        try {
            response = objectMapper.readValue(json, AuthorizeResponseBody.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return response;
    }
}
