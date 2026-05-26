"""Configuration loading and typed access.

Config precedence (lowest to highest):
    conf/base.yaml  <  conf/<env>.yaml  <  TOKYO_* environment variables

This replaces the original notebook's hardcoded paths and inline secrets with a
single, testable, environment-aware source of truth.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_CONF_DIR = Path(__file__).resolve().parents[2] / "conf"
_ENV_PREFIX = "TOKYO_"
_ENV_NESTED_SEP = "__"  # TOKYO_STORAGE__USE_LOCAL -> storage.use_local


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    shuffle_partitions: int


@dataclass(frozen=True)
class StorageConfig:
    account_name: str
    container: str
    use_local: bool
    local_root: str

    def layer_root(self, layer_path: str) -> str:
        """Absolute root of a medallion layer (abfss URI or local path)."""
        if self.use_local:
            return f"{self.local_root.rstrip('/')}/{layer_path}"
        return f"abfss://{self.container}@{self.account_name}.dfs.core.windows.net/" f"{layer_path}"


@dataclass(frozen=True)
class SecretsConfig:
    scope: str
    tenant_id_key: str
    client_id_key: str
    client_secret_key: str


@dataclass(frozen=True)
class DataQualityConfig:
    fail_fast: bool


@dataclass(frozen=True)
class AppConfig:
    env: str
    spark: SparkConfig
    storage: StorageConfig
    secrets: SecretsConfig
    data_quality: DataQualityConfig
    layers: dict[str, str]
    datasets: dict[str, dict[str, str]]

    # -- path helpers -------------------------------------------------------
    def raw_path(self, dataset: str) -> str:
        """Full path to a dataset's raw CSV file."""
        file_name = self.datasets[dataset]["raw_file"]
        return f"{self.storage.layer_root(self.layers['raw'])}/{file_name}"

    def raw_encoding(self, dataset: str) -> str:
        """Character encoding of a dataset's raw CSV (defaults to UTF-8)."""
        return self.datasets[dataset].get("encoding", "UTF-8")

    def layer_path(self, layer: str, dataset: str) -> str:
        """Full path to a dataset's table within a bronze/silver/gold layer."""
        return f"{self.storage.layer_root(self.layers[layer])}/{dataset}"

    @property
    def dataset_names(self) -> list[str]:
        return list(self.datasets.keys())


# ---------------------------------------------------------------------------
# Loading & merging
# ---------------------------------------------------------------------------
def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` into ``base``, returning a new dict."""
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _coerce(raw: str) -> Any:
    """Coerce an env-var string into bool/int where it clearly is one."""
    lowered = raw.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw


def _apply_env_overrides(cfg: dict[str, Any]) -> dict[str, Any]:
    """Overlay TOKYO_*-prefixed env vars onto the merged config dict."""
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(_ENV_PREFIX) or env_key == f"{_ENV_PREFIX}ENV":
            continue
        path = env_key[len(_ENV_PREFIX) :].lower().split(_ENV_NESTED_SEP)
        cursor: dict[str, Any] = cfg
        for part in path[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[path[-1]] = _coerce(env_val)
    return cfg


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_config(env: str | None = None, conf_dir: Path | None = None) -> AppConfig:
    """Build the typed :class:`AppConfig` for ``env``.

    ``env`` defaults to the ``TOKYO_ENV`` environment variable, then ``dev``.
    """
    env = (env or os.getenv(f"{_ENV_PREFIX}ENV") or "dev").lower()
    conf_dir = conf_dir or _CONF_DIR

    merged = _read_yaml(conf_dir / "base.yaml")
    merged = _deep_merge(merged, _read_yaml(conf_dir / f"{env}.yaml"))
    merged = _apply_env_overrides(merged)

    return AppConfig(
        env=env,
        spark=SparkConfig(**merged["spark"]),
        storage=StorageConfig(**merged["storage"]),
        secrets=SecretsConfig(**merged["secrets"]),
        data_quality=DataQualityConfig(**merged["data_quality"]),
        layers=merged["layers"],
        datasets=merged["datasets"],
    )
