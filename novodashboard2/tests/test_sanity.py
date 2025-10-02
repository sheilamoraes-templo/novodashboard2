from __future__ import annotations

from configs.settings import get_settings


def test_settings_dirs_created() -> None:
    s = get_settings()
    assert (s.data_dir / "raw").exists()
    assert (s.data_dir / "api_cache").exists()
    assert (s.data_dir / "warehouse").exists()


