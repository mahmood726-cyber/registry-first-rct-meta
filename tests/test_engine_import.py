"""Import-smoke tests.

Guards against the regression where a top-level import of a non-existent module
made engine.py, validation.py, and both CLI entry points fail at import time
(ModuleNotFoundError) while the rest of the suite stayed green.
"""

import importlib


def test_engine_module_imports() -> None:
    module = importlib.import_module("registry_first_ma.engine")
    assert hasattr(module, "RegistryFirstEngine")


def test_validation_module_imports() -> None:
    importlib.import_module("registry_first_ma.validation")


def test_cli_entrypoint_modules_import() -> None:
    importlib.import_module("scripts.run_validation")
    importlib.import_module("scripts.run_topic_engine")
