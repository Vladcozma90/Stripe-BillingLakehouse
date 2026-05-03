from __future__ import annotations

from functools import lru_cache

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


@lru_cache(maxsize=8)
def _get_secret_client(key_vault_url: str) -> SecretClient:
    credential = DefaultAzureCredential()

    return SecretClient(
        vault_url=key_vault_url,
        credential=credential,
    )

def get_kv_secret(secret_name: str, key_vault_url: str) -> str:
    try:

        client = _get_secret_client(key_vault_url)
        secret = client.get_secret(secret_name)

        if not secret.value:
            raise RuntimeError(f"Secret '{secret_name}' exists but has no value.")
        
        return secret.value
    
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve secret '{secret_name}' from Azure Key Vault.") from e
    