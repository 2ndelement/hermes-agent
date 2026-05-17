try:
    from .adapter import register
except ImportError:
    import importlib.util
    import sys
    from pathlib import Path

    adapter_path = Path(__file__).with_name("adapter.py")
    spec = importlib.util.spec_from_file_location("plugin_adapter_qqbot_plus_init", adapter_path)
    if spec is None or spec.loader is None:
        raise
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("plugin_adapter_qqbot_plus_init", module)
    spec.loader.exec_module(module)
    register = module.register

__all__ = ["register"]
