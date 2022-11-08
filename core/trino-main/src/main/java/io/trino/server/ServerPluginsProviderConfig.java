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
package io.trino.server;

import io.airlift.configuration.Config;

import java.io.File;

public class ServerPluginsProviderConfig
{
    private File installedPluginsDir = new File("plugin");

    public File getInstalledPluginsDir()
    {
        return installedPluginsDir;
    }

    @Config("plugin.dir")
    public ServerPluginsProviderConfig setInstalledPluginsDir(File installedPluginsDir)
    {
        this.installedPluginsDir = installedPluginsDir;
        return this;
    }

    private File hiveUdfDir = new File("hive-udf");
    private boolean maxFunctionRunningTimeEnable;
    private long maxFunctionRunningTimeInSec = 600;
    private int functionRunningThreadPoolSize = 100;

    public File getHiveUdfDir()
    {
        return this.hiveUdfDir;
    }

    @Config("hive-udf.dir")
    public ServerPluginsProviderConfig setHiveUdfDir(File hiveUdfDir)
    {
        this.hiveUdfDir = hiveUdfDir;
        return this;
    }

    public boolean getMaxFunctionRunningTimeEnable()
    {
        return this.maxFunctionRunningTimeEnable;
    }

    @Config("max-function-running-time-enable")
    public ServerPluginsProviderConfig setMaxFunctionRunningTimeEnable(boolean maxFunctionRunningTimeEnable)
    {
        this.maxFunctionRunningTimeEnable = maxFunctionRunningTimeEnable;
        return this;
    }

    public long getMaxFunctionRunningTimeInSec()
    {
        return this.maxFunctionRunningTimeInSec;
    }

    @Config("max-function-running-time-in-second")
    public ServerPluginsProviderConfig setMaxFunctionRunningTimeInSec(long time)
    {
        this.maxFunctionRunningTimeInSec = time;
        return this;
    }

    public int getFunctionRunningThreadPoolSize()
    {
        return this.functionRunningThreadPoolSize;
    }

    @Config("function-running-thread-pool-size")
    public ServerPluginsProviderConfig setFunctionRunningThreadPoolSize(int functionRunningThreadPoolSize)
    {
        this.functionRunningThreadPoolSize = functionRunningThreadPoolSize;
        return this;
    }
}
