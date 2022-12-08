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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.io.Files.getNameWithoutExtension;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/5 17:04
 */
public class FunctionNamespaceStore
{
    private static final Logger log = Logger.get(FunctionNamespaceStore.class);
    private static final String FUNCTION_NAMESPACE_MANAGER_NAME = "function-namespace-manager.name";
    private final Metadata metadata;
    private final File configDir;
    private final AtomicBoolean functionNamespaceLoading = new AtomicBoolean();

    @Inject
    public FunctionNamespaceStore(Metadata metadata, FunctionNamespaceStoreConfig config)
    {
        this.metadata = metadata;
        this.configDir = config.getFunctionNamespaceConfigurationDir();
    }

    public void loadFunctionNamespaceManagers()
            throws Exception
    {
        if (!functionNamespaceLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(configDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                String catalogName = getNameWithoutExtension(file.getName());
                Map<String, String> properties = new HashMap<>(loadPropertiesFrom(file.getPath()));
                checkState(!isNullOrEmpty(properties.get(FUNCTION_NAMESPACE_MANAGER_NAME)),
                        "Function namespace configuration %s does not contain %s",
                        file.getAbsoluteFile(),
                        FUNCTION_NAMESPACE_MANAGER_NAME);
                loadFunctionNamespaceManager(catalogName, properties);
            }
        }
    }

    private void loadFunctionNamespaceManager(String catalogName, Map<String, String> properties)
    {
        log.info("-- Loading function namespace manager for catalog %s --", catalogName);
        String functionNamespaceManagerName = properties.remove(FUNCTION_NAMESPACE_MANAGER_NAME);
        checkState(!isNullOrEmpty(functionNamespaceManagerName), "%s property must be present", FUNCTION_NAMESPACE_MANAGER_NAME);
        metadata.loadFunctionNamespaceManager(functionNamespaceManagerName, catalogName, properties);
        log.info("-- Added function namespace manager [%s] --", catalogName);
    }

    private static List<File> listFiles(File dir)
    {
        if (dir != null && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
