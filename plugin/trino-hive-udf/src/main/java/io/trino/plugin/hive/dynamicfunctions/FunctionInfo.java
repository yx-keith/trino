package io.trino.plugin.hive.dynamicfunctions;

import java.net.URL;

public class FunctionInfo
{
    private String functionName;
    private String functionClassName;
    private String functionJarName;
    private URL localUrl;
    private ClassLoader functionClassLoader;

    public FunctionInfo(String functionName, String functionClassName, String functionJarName, URL localUrl)
    {
        this.functionName = functionName;
        this.functionClassName = functionClassName;
        this.localUrl = localUrl;
        this.functionJarName = functionJarName;
    }

    public String getFunctionName()
    {
        return functionName;
    }

    public void setFunctionName(String functionName)
    {
        this.functionName = functionName;
    }

    public String getFunctionClassName()
    {
        return functionClassName;
    }

    public void setFunctionClassName(String functionClassName)
    {
        this.functionClassName = functionClassName;
    }

    public String getFunctionJarName()
    {
        return functionJarName;
    }

    public void setFunctionJarName(String functionJarName)
    {
        this.functionJarName = functionJarName;
    }

    public ClassLoader getFunctionClassLoader()
    {
        return functionClassLoader;
    }

    public void setFunctionClassLoader(ClassLoader functionClassLoader)
    {
        this.functionClassLoader = functionClassLoader;
    }

    public URL getLocalUrl() {

        return localUrl;
    }

    public void setLocalUrl(URL localUrl)
    {
        this.localUrl = localUrl;
    }

    @Override
    public String toString() {
        return "FunctionInfo{" +
                "functionName='" + functionName + '\'' +
                ", functionClassName='" + functionClassName + '\'' +
                ", functionJarName='" + functionJarName + '\'' +
                ", functionJarUrl=" + localUrl +
                ", functionClassLoader=" + functionClassLoader +
                '}';
    }
}
