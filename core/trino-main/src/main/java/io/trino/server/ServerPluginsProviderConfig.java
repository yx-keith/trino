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

    private File localHiveUdfJarsPath = new File("hive-udf/jars");
    private String remoteHiveUdfPropsPath = "hdfs://localhost:8020/user/hive/udf/props/udf.properties";
    private String remoteHiveUdfJarsPath = "hdfs://localhost:8020/user/hive/udf/jars";
    private boolean maxFunctionRunningTimeEnable;
    private long maxFunctionRunningTimeInSec = 600;
    private int functionRunningThreadPoolSize = 100;

    public File getLocalHiveUdfJarsPath()
    {
        return this.localHiveUdfJarsPath;
    }

    @Config("hive-udf.dir")
    public ServerPluginsProviderConfig setLocalHiveUdfJarsPath(File localHiveUdfJarsPath)
    {
        this.localHiveUdfJarsPath = localHiveUdfJarsPath;
        return this;
    }

    public String getRemoteHiveUdfPropsPath()
    {
        return this.remoteHiveUdfPropsPath;
    }

    @Config("remote-hive-udf-props.path")
    public ServerPluginsProviderConfig setRemoteHiveUdfPropsPath(String remoteHiveUdfPropsPath)
    {
        this.remoteHiveUdfPropsPath = remoteHiveUdfPropsPath;
        return this;
    }

    public String getRemoteHiveUdfJarsPath()
    {
        return this.remoteHiveUdfJarsPath;
    }

    @Config("remote-hive-udf-jars.path")
    public ServerPluginsProviderConfig setRemoteHiveUdfJarsPath(String remoteHiveUdfJarsPath)
    {
        this.remoteHiveUdfJarsPath = remoteHiveUdfJarsPath;
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
