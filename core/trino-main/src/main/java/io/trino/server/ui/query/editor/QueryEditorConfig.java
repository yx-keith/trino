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
package io.trino.server.ui.query.editor;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/1/9 9:24
 */
public class QueryEditorConfig
{
    private Duration executionTimeout = new Duration(15, MINUTES);
    private boolean isRunningEmbeded = true;
    private String coordinatorUri;
    private DataSize maxFileSize = new DataSize(1, DataSize.Unit.GIGABYTE);
    private int maxResultCount = 1000;

    public Duration getExecutionTimeout()
    {
        return executionTimeout;
    }

    @Config("trino.query-ui.execution-timeout")
    public void setExecutionTimeout(Duration executionTimeout)
    {
        this.executionTimeout = executionTimeout;
    }

    @Config("trino.query-ui.embeded-mode")
    public QueryEditorConfig setRunningEmbeded(boolean runningEmbeded)
    {
        isRunningEmbeded = runningEmbeded;
        return this;
    }

    public boolean isRunningEmbeded()
    {
        return isRunningEmbeded;
    }

    @Config("trino.query-ui.server.uri")
    public QueryEditorConfig setCoordinatorUri(String coordinatorUri)
    {
        this.coordinatorUri = coordinatorUri;
        return this;
    }

    public String getCoordinatorUri()
    {
        return coordinatorUri;
    }

    public DataSize getMaxFileSize()
    {
        return maxFileSize;
    }

    @Config("trino.query-ui.max-result-size-mb")
    public QueryEditorConfig setMaxFileSize(int maxFileSizeMb)
    {
        this.maxFileSize = new DataSize(maxFileSizeMb, DataSize.Unit.MEGABYTE);
        return this;
    }

    public int getMaxResultCount()
    {
        return maxResultCount;
    }

    @Config("hetu.query-ui.max-result-count")
    public void setMaxResultCount(int maxResultCount)
    {
        this.maxResultCount = maxResultCount;
    }
}
