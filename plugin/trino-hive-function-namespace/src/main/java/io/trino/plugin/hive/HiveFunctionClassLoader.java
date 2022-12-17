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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author yaoxiao
 * @version 1.0
 * @time 2021/8/13 1:02 下午
 */
public class HiveFunctionClassLoader
        extends URLClassLoader

{
    private static final ClassLoader PLATFORM_CLASS_LOADER = getPlatformClassLoader();
    private final ClassLoader spiClassLoader;
    private final List<String> spiPackages;
    private final List<String> spiResources;

    public HiveFunctionClassLoader(
            URL url,
            ClassLoader spiClassLoader,
            Iterable<String> spiPackages)
    {
        this(List.of(url),
                spiClassLoader,
                spiPackages,
                Iterables.transform(spiPackages, HiveFunctionClassLoader::classNameToResource));
    }

    private HiveFunctionClassLoader(
            List<URL> urls,
            ClassLoader spiClassLoader,
            Iterable<String> spiPackages,
            Iterable<String> spiResources)
    {
        super(urls.toArray(new URL[urls.size()]), PLATFORM_CLASS_LOADER);
        this.spiClassLoader = requireNonNull(spiClassLoader, "pluginClassLoader is null");
        this.spiPackages = ImmutableList.copyOf(spiPackages);
        this.spiResources = ImmutableList.copyOf(spiResources);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException
    {
        synchronized (getClassLoadingLock(name)) {
            Class<?> cachedClass = findLoadedClass(name);
            if (cachedClass != null) {
                return resolveClass(cachedClass, resolve);
            }
            if (isPluginClass(name)) {
                return resolveClass(spiClassLoader.loadClass(name), resolve);
            }
            return super.loadClass(name, resolve);
        }
    }

    private Class<?> resolveClass(Class<?> clazz, boolean resolve)
    {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    @Override
    public URL getResource(String name)
    {
        if (isPluginResource(name)) {
            return spiClassLoader.getResource(name);
        }
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name)
            throws IOException
    {
        if (isPluginClass(name)) {
            return spiClassLoader.getResources(name);
        }
        return super.getResources(name);
    }

    private boolean isPluginClass(String name)
    {
        return spiPackages.stream().anyMatch(name::startsWith);
    }

    private boolean isPluginResource(String name)
    {
        return spiResources.stream().anyMatch(name::startsWith);
    }

    private static String classNameToResource(String className)
    {
        return className.replace('.', '/');
    }
}
