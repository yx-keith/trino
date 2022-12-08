package io.trino.plugin.hive.dynamicfunctions;

import io.trino.spi.function.DynamicHiveFunctionInfo;

import java.net.URL;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2022/12/6 8:54
 */
public class StaticHiveFunctionInfo
{
    private DynamicHiveFunctionInfo dynamicHiveFunctionInfo;
    private URL localUrl;
    private ClassLoader classLoader;

    public StaticHiveFunctionInfo(DynamicHiveFunctionInfo dynamicHiveFunctionInfo, URL localUrl, ClassLoader classLoader)
    {
        this.dynamicHiveFunctionInfo = dynamicHiveFunctionInfo;
        this.localUrl = localUrl;
        this.classLoader = classLoader;
    }

    public DynamicHiveFunctionInfo getHiveFunctionInfo()
    {
        return dynamicHiveFunctionInfo;
    }

    public URL getLocalUrl()
    {
        return localUrl;
    }

    public ClassLoader getClassLoader()
    {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    @Override
    public String toString() {
        return "StaticHiveFunctionInfo{" +
                "dynamicHiveFunctionInfo=" + dynamicHiveFunctionInfo +
                ", localUrl=" + localUrl +
                ", classLoader=" + classLoader +
                '}';
    }
}
