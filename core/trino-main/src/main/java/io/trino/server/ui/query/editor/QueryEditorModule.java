package io.trino.server.ui.query.editor;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.server.ui.query.editor.resoures.UIExecuteResource;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/1/9 15:19
 */
public class QueryEditorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(QueryEditorConfig.class);

        jaxrsBinder(binder).bind(UIExecuteResource.class);
    }
}
