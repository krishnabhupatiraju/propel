import inspect

from plugout import PluginManager

def get_plugins(base_class,
                path,
                follow_links=False,
                exclude_base_from_result=True):
    if not inspect.isclass(base_class):
        raise Exception('{} not a class'.format(base_class))
    pm = PluginManager(base_class=base_class)
    pm.load_from_filepath(path_reference=path,
                          followlinks=follow_links)
    plugins = []
    if exclude_base_from_result:
        for plugin in pm.plugins:
            if not plugin is base_class:
                plugins.append(plugin)
        return plugins
    else:
        return pm.plugins
