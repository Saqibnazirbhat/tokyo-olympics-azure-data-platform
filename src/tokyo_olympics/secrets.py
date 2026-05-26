"""Secret resolution that works both on Databricks and locally.

The original notebook pasted the service-principal client id/secret straight
into source. This module instead resolves them at runtime from:

* **Databricks**: a secret scope backed by Azure Key Vault (``dbutils.secrets``).
* **Local/CI**: environment variables (``ADLS_TENANT_ID`` etc.).

No credential ever lives in the repository.
"""

from __future__ import annotations

import os
from typing import Protocol

from .config import SecretsConfig
from .logging_utils import get_logger

logger = get_logger(__name__)


class SecretProvider(Protocol):
    """Anything that can resolve a named secret to its value."""

    def get(self, key: str) -> str: ...


class EnvSecretProvider:
    """Resolve secrets from environment variables (local / CI use)."""

    def get(self, key: str) -> str:
        value = os.getenv(key)
        if not value:
            raise KeyError(f"Required secret '{key}' is not set in the environment.")
        return value


class DatabricksSecretProvider:
    """Resolve secrets from a Databricks secret scope (Key Vault backed)."""

    def __init__(self, dbutils: object, scope: str) -> None:
        self._dbutils = dbutils
        self._scope = scope

    def get(self, key: str) -> str:
        return self._dbutils.secrets.get(scope=self._scope, key=key)  # type: ignore[attr-defined]


def resolve_secret_provider(
    secrets_cfg: SecretsConfig, dbutils: object | None = None
) -> SecretProvider:
    """Pick the right provider for the current runtime.

    Passing ``dbutils`` (available in Databricks notebooks/jobs) selects the
    Key Vault backed scope; otherwise we fall back to environment variables.
    """
    if dbutils is not None:
        logger.info("Using Databricks secret scope '%s'.", secrets_cfg.scope)
        return DatabricksSecretProvider(dbutils, secrets_cfg.scope)
    logger.info("Using environment-variable secret provider (local/CI).")
    return EnvSecretProvider()
