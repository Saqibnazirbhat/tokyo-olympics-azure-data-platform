"""Tests for configuration loading, merging, and path building."""

from __future__ import annotations

from tokyo_olympics.config import load_config


def test_dev_uses_local_storage():
    config = load_config(env="dev")
    assert config.env == "dev"
    assert config.storage.use_local is True
    # dev.yaml overrides base.yaml shuffle partitions.
    assert config.spark.shuffle_partitions == 4


def test_prod_builds_abfss_paths():
    config = load_config(env="prod")
    assert config.storage.use_local is False
    bronze = config.layer_path("bronze", "medals")
    assert bronze.startswith("abfss://")
    assert "tokyoolympicdata.dfs.core.windows.net" in bronze
    assert bronze.endswith("/bronze/medals")


def test_local_paths_for_dev():
    config = load_config(env="dev")
    assert config.raw_path("athletes").endswith("raw-data/Athletes.csv")
    assert config.layer_path("gold", "medal_standings").endswith("gold/medal_standings")


def test_env_var_override(monkeypatch):
    monkeypatch.setenv("TOKYO_SPARK__SHUFFLE_PARTITIONS", "99")
    monkeypatch.setenv("TOKYO_STORAGE__USE_LOCAL", "false")
    config = load_config(env="dev")
    assert config.spark.shuffle_partitions == 99  # int-coerced
    assert config.storage.use_local is False  # bool-coerced


def test_raw_encoding_lookup():
    config = load_config(env="dev")
    assert config.raw_encoding("coaches") == "ISO-8859-1"
    assert config.raw_encoding("medals") == "UTF-8"
