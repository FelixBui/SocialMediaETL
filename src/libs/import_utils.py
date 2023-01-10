import sys
import importlib


def import_module_by_string(module_path):
    importlib.import_module(module_path)


def get_module_by_string(module_path):
    import_module_by_string(module_path)
    mod = sys.modules.get(module_path)
    if mod is None:
        raise ImportError("Can't import '{0}'".format(module_path))
    return mod


def get_object_by_string(module_path, func_name):
    mod = get_module_by_string(module_path)
    func = mod and getattr(mod, func_name, None)
    if func is None:
        raise ImportError("Can't get object '{0}' from module '{1}'".format(func_name, module_path))
    return func
