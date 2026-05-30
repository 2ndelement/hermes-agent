from pathlib import Path
import tomllib


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_faster_whisper_is_not_a_base_dependency():
    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    deps = data["project"]["dependencies"]

    assert not any(dep.startswith("faster-whisper") for dep in deps)

    voice_extra = data["project"]["optional-dependencies"]["voice"]
    assert any(dep.startswith("faster-whisper") for dep in voice_extra)


def test_manifest_includes_bundled_skills():
    manifest = (REPO_ROOT / "MANIFEST.in").read_text(encoding="utf-8")

    assert "graft skills" in manifest
    assert "graft optional-skills" in manifest


def test_manifest_includes_bundled_plugins():
    manifest = (REPO_ROOT / "MANIFEST.in").read_text(encoding="utf-8")

    assert "graft plugins" in manifest


def test_pyproject_includes_bundled_plugin_data():
    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    package_data = data["tool"]["setuptools"]["package-data"]

    assert "plugins" in package_data
    assert "platforms/qqbot-plus/plugin.yaml" in package_data["plugins"]
    assert "platforms/qqbot-plus/__init__.py" in package_data["plugins"]
    assert "platforms/qqbot-plus/adapter.py" in package_data["plugins"]
