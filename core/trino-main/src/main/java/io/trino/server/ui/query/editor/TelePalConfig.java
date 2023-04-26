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
public class TelePalConfig
{
    private Duration executionTimeout = new Duration(15, MINUTES);
    private boolean isRunningEmbeded = true;
    private String coordinatorUri;
    private DataSize maxFileSize = new DataSize(1, DataSize.Unit.GIGABYTE);
    private int maxResultCount = 1000;
    private String resultPath = "/tmp";

    public Duration getExecutionTimeout()
    {
        return executionTimeout;
    }

    @Config("telepal.query-execution-timeout")
    public void setExecutionTimeout(Duration executionTimeout)
    {
        this.executionTimeout = executionTimeout;
    }

    @Config("telepal.query-embeded-mode")
    public TelePalConfig setRunningEmbeded(boolean runningEmbeded)
    {
        isRunningEmbeded = runningEmbeded;
        return this;
    }

    public boolean isRunningEmbeded()
    {
        return isRunningEmbeded;
    }

    @Config("trino.server.uri")
    public TelePalConfig setCoordinatorUri(String coordinatorUri)
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

    @Config("telepal.query-max-result-size-mb")
    public TelePalConfig setMaxFileSize(int maxFileSizeMb)
    {
        this.maxFileSize = new DataSize(maxFileSizeMb, DataSize.Unit.MEGABYTE);
        return this;
    }

    public int getMaxResultCount()
    {
        return maxResultCount;
    }

    @Config("telepal.query-max-result-count")
    public TelePalConfig setMaxResultCount(int maxResultCount)
    {
        this.maxResultCount = maxResultCount;
        return this;
    }

    public String getResultPath()
    {
        return resultPath;
    }

    @Config("telepal.query-result-path")
    public TelePalConfig setResultPath(String resultPath)
    {
        this.resultPath = resultPath;
        return this;
    }
}
