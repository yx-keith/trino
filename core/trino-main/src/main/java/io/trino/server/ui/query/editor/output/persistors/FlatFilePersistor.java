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

import io.airlift.log.Logger;
import io.trino.server.ui.query.editor.execution.QueryExecutionAuthorizer;
import io.trino.server.ui.query.editor.output.builds.JobOutputBuilder;
import io.trino.server.ui.query.editor.protocol.Job;
import io.trino.server.ui.query.editor.store.files.ExpiringFileStore;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import static java.lang.String.format;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/9 13:48
 */
public class FlatFilePersistor
        implements Persistor
{
    private static final Logger LOG = Logger.get(FlatFilePersistor.class);
    private final ExpiringFileStore fileStore;

    public FlatFilePersistor(ExpiringFileStore fileStore)
    {
        this.fileStore = fileStore;
    }

    @Override
    public boolean canPersist(QueryExecutionAuthorizer authorizer)
    {
        return true;
    }

    @Override
    public URI persist(JobOutputBuilder outputBuilder, Job job)
    {
        File file = outputBuilder.build();

        try {
            fileStore.addFile(file.getName(), job.getUser(), file);
        }
        catch (IOException e) {
            LOG.error("Caught error adding file to local store", e);
        }

        return URI.create(format("../api/files/%s", file.getName()));
    }
}
