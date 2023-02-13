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
package io.trino.server.ui.query.editor.output.persistors;

import io.trino.server.ui.query.editor.execution.QueryExecutionAuthorizer;
import io.trino.server.ui.query.editor.output.builds.JobOutputBuilder;
import io.trino.server.ui.query.editor.protocol.Job;

import java.net.URI;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/9 13:45
 */
public interface Persistor
{
    boolean canPersist(QueryExecutionAuthorizer authorizer);

    URI persist(JobOutputBuilder outputBuilder, Job job);
}
