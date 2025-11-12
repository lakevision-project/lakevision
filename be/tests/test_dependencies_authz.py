import importlib
import sys
import types

import pytest

import app.dependencies as dependencies


@pytest.fixture(autouse=True)
def clear_bundled_authz_module():
    """Ensure the temporary app._default_authz module does not leak between tests."""
    sys.modules.pop("app._default_authz", None)
    yield
    sys.modules.pop("app._default_authz", None)


def test_load_authz_class_falls_back_to_bundled_impl(monkeypatch):
    """Default config should fall back to bundled authz.py when the import fails."""
    monkeypatch.setattr(dependencies.config, "AUTHZ_MODULE", dependencies.DEFAULT_AUTHZ_MODULE)
    monkeypatch.setattr(dependencies.config, "AUTHZ_CLASS", dependencies.DEFAULT_AUTHZ_CLASS)

    def fake_import(_):
        raise ModuleNotFoundError("authz module missing")

    monkeypatch.setattr(dependencies.importlib, "import_module", fake_import)

    cls = dependencies._load_authz_class()
    assert cls.__name__ == "Authz"
    assert cls.__module__ == "app._default_authz"


def test_load_authz_class_uses_bundled_when_default_missing_attr(monkeypatch):
    """If the default module loads but lacks the class, fall back to the bundled file."""
    empty_module = types.ModuleType("app.authz")
    monkeypatch.setattr(dependencies.config, "AUTHZ_MODULE", dependencies.DEFAULT_AUTHZ_MODULE)
    monkeypatch.setattr(dependencies.config, "AUTHZ_CLASS", dependencies.DEFAULT_AUTHZ_CLASS)
    monkeypatch.setattr(dependencies.importlib, "import_module", lambda _: empty_module)

    cls = dependencies._load_authz_class()
    assert cls.__name__ == "Authz"
    assert cls.__module__ == "app._default_authz"


def test_load_authz_class_prefers_custom_plugin(monkeypatch):
    """Custom module/class combinations should be returned when import succeeds."""
    custom_module = types.ModuleType("custom.authz")

    class CustomAuthz:
        pass

    custom_module.CustomAuthz = CustomAuthz
    monkeypatch.setattr(dependencies.config, "AUTHZ_MODULE", "custom.authz")
    monkeypatch.setattr(dependencies.config, "AUTHZ_CLASS", "CustomAuthz")
    original_import = dependencies.importlib.import_module

    def fake_import(name):
        if name == "custom.authz":
            return custom_module
        return original_import(name)

    monkeypatch.setattr(dependencies.importlib, "import_module", fake_import)

    cls = dependencies._load_authz_class()
    assert cls is CustomAuthz


def test_load_authz_class_raises_for_custom_failures(monkeypatch):
    """Non-default modules should still raise errors when they cannot be imported."""
    monkeypatch.setattr(dependencies.config, "AUTHZ_MODULE", "custom.missing")
    monkeypatch.setattr(dependencies.config, "AUTHZ_CLASS", "Missing")

    def fake_import(_):
        raise ModuleNotFoundError("boom")

    monkeypatch.setattr(dependencies.importlib, "import_module", fake_import)

    with pytest.raises(ModuleNotFoundError):
        dependencies._load_authz_class()
