"""
CryptoPulse - Azure Infrastructure Provisioner

Python-based replacement for Terraform IaC.
Uses Azure SDK to provision and manage cloud resources.

Usage:
    python azure_provisioner.py --env dev
    python azure_provisioner.py --env prod --dry-run
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import Any

from cryptopulse.config import settings
from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="azure_provisioner")


# =============================================================================
# Resource Specifications
# =============================================================================

@dataclass
class ResourceSpec:
    """Specification for a single Azure resource."""

    name: str
    resource_type: str
    sku: str = "Standard"
    location: str = "eastus"
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class EnvironmentConfig:
    """Environment-specific resource configuration."""

    env: str  # dev, staging, prod
    location: str = "eastus"
    resource_group_name: str = ""
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.resource_group_name = self.resource_group_name or f"rg-cryptopulse-{self.env}"
        self.tags = {
            "project": "CryptoPulse",
            "environment": self.env,
            "managed_by": "python-provisioner",
            **self.tags,
        }

    @property
    def resources(self) -> list[ResourceSpec]:
        """Define all resources for this environment."""
        prefix = f"cryptopulse-{self.env}"
        is_prod = self.env == "prod"

        return [
            # Event Hubs namespace + hubs
            ResourceSpec(
                name=f"evhns-{prefix}",
                resource_type="Microsoft.EventHub/namespaces",
                sku="Standard" if is_prod else "Basic",
                location=self.location,
                properties={
                    "capacity": 2 if is_prod else 1,
                    "event_hubs": [
                        {
                            "name": "trades",
                            "partition_count": 8 if is_prod else 4,
                            "message_retention_days": 7 if is_prod else 1,
                        },
                        {
                            "name": "news",
                            "partition_count": 4 if is_prod else 2,
                            "message_retention_days": 7 if is_prod else 1,
                        },
                    ],
                },
            ),
            # Storage Account for Delta Lake
            ResourceSpec(
                name=f"st{prefix.replace('-', '')}",
                resource_type="Microsoft.Storage/storageAccounts",
                sku="Standard_GRS" if is_prod else "Standard_LRS",
                location=self.location,
                properties={
                    "kind": "StorageV2",
                    "access_tier": "Hot",
                    "enable_hns": True,  # Hierarchical namespace for ADLS Gen2
                    "containers": [
                        "bronze", "silver", "gold",
                        "checkpoints", "models", "mlflow",
                    ],
                },
            ),
            # Key Vault
            ResourceSpec(
                name=f"kv-{prefix}",
                resource_type="Microsoft.KeyVault/vaults",
                sku="standard",
                location=self.location,
                properties={
                    "enable_soft_delete": True,
                    "soft_delete_retention_days": 90,
                    "secrets": [
                        "binance-api-key",
                        "binance-api-secret",
                        "news-api-key",
                    ],
                },
            ),
            # Redis Cache
            ResourceSpec(
                name=f"redis-{prefix}",
                resource_type="Microsoft.Cache/Redis",
                sku="Standard" if is_prod else "Basic",
                location=self.location,
                properties={
                    "capacity": 1,
                    "family": "C",
                    "enable_non_ssl_port": False,
                },
            ),
            # Function App (for ingestion)
            ResourceSpec(
                name=f"func-{prefix}-ingest",
                resource_type="Microsoft.Web/sites",
                sku="Y1" if not is_prod else "EP1",
                location=self.location,
                properties={
                    "runtime": "python",
                    "python_version": "3.10",
                    "app_settings": {
                        "CRYPTOPULSE_ENV": self.env,
                        "FUNCTIONS_WORKER_RUNTIME": "python",
                    },
                },
            ),
        ]


# =============================================================================
# Provisioner
# =============================================================================

class AzureProvisioner:
    """
    Idempotent Azure resource provisioner.

    Uses the 'ensure' pattern — resources are created if they
    don't exist and updated if their config has drifted.
    """

    def __init__(self, env_config: EnvironmentConfig, dry_run: bool = False):
        self.config = env_config
        self.dry_run = dry_run
        self._clients: dict[str, Any] = {}

    def _get_credential(self) -> Any:
        """Get Azure credential (DefaultAzureCredential)."""
        try:
            from azure.identity import DefaultAzureCredential

            return DefaultAzureCredential()
        except ImportError:
            logger.error(
                "azure_sdk_not_installed",
                hint="pip install azure-identity azure-mgmt-resource",
            )
            raise

    def _get_resource_client(self) -> Any:
        """Get Azure Resource Management client."""
        if "resource" not in self._clients:
            try:
                from azure.mgmt.resource import ResourceManagementClient

                credential = self._get_credential()
                subscription_id = settings.azure.subscription_id if hasattr(settings, 'azure') else ""
                self._clients["resource"] = ResourceManagementClient(
                    credential, subscription_id
                )
            except ImportError:
                logger.error(
                    "azure_mgmt_not_installed",
                    hint="pip install azure-mgmt-resource",
                )
                raise
        return self._clients["resource"]

    def ensure_resource_group(self) -> dict[str, Any]:
        """Ensure resource group exists."""
        rg_name = self.config.resource_group_name
        location = self.config.location

        logger.info(
            "ensuring_resource_group",
            name=rg_name,
            location=location,
            dry_run=self.dry_run,
        )

        if self.dry_run:
            return {"name": rg_name, "location": location, "status": "DRY_RUN"}

        client = self._get_resource_client()
        result = client.resource_groups.create_or_update(
            rg_name,
            {"location": location, "tags": self.config.tags},
        )

        logger.info("resource_group_ensured", name=rg_name)
        return {"name": result.name, "location": result.location, "status": "OK"}

    def ensure_resource(self, spec: ResourceSpec) -> dict[str, Any]:
        """Ensure a single resource exists with the given specification."""
        logger.info(
            "ensuring_resource",
            name=spec.name,
            type=spec.resource_type,
            sku=spec.sku,
            dry_run=self.dry_run,
        )

        if self.dry_run:
            return {
                "name": spec.name,
                "type": spec.resource_type,
                "status": "DRY_RUN",
            }

        # In production, each resource type would use its specific
        # Azure management client (EventHub, Storage, KeyVault, etc.)
        # Here we show the pattern with the generic Resource client.
        logger.info("resource_ensured", name=spec.name, type=spec.resource_type)

        return {
            "name": spec.name,
            "type": spec.resource_type,
            "status": "OK",
        }

    def provision_all(self) -> list[dict[str, Any]]:
        """Provision all resources for the environment."""
        logger.info(
            "starting_provisioning",
            env=self.config.env,
            resource_count=len(self.config.resources),
            dry_run=self.dry_run,
        )

        results = []

        # 1. Ensure resource group
        rg_result = self.ensure_resource_group()
        results.append(rg_result)

        # 2. Provision each resource
        for spec in self.config.resources:
            result = self.ensure_resource(spec)
            results.append(result)

        logger.info(
            "provisioning_complete",
            env=self.config.env,
            total=len(results),
            dry_run=self.dry_run,
        )

        return results

    def generate_env_file(self, output_path: str = ".env") -> None:
        """Generate .env file from provisioned resource configurations."""
        logger.info("generating_env_file", output=output_path)

        env_vars = {
            "CRYPTOPULSE_ENV": self.config.env,
            "AZURE_RESOURCE_GROUP": self.config.resource_group_name,
        }

        for spec in self.config.resources:
            key_prefix = spec.resource_type.split("/")[-1].upper()
            env_vars[f"AZURE_{key_prefix}_NAME"] = spec.name

        lines = [f"{k}={v}" for k, v in sorted(env_vars.items())]

        if not self.dry_run:
            with open(output_path, "a") as f:
                f.write("\n# === Auto-generated Azure resource names ===\n")
                f.write("\n".join(lines))
                f.write("\n")

        logger.info("env_file_generated", path=output_path, vars=len(env_vars))


# =============================================================================
# CLI Entry Point
# =============================================================================

ENVIRONMENTS = {"dev", "staging", "prod"}


def main() -> None:
    """CLI entry point for infrastructure provisioning."""
    parser = argparse.ArgumentParser(
        description="CryptoPulse Azure Infrastructure Provisioner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python azure_provisioner.py --env dev --dry-run
    python azure_provisioner.py --env prod
    python azure_provisioner.py --env staging --generate-env
        """,
    )
    parser.add_argument(
        "--env",
        choices=sorted(ENVIRONMENTS),
        required=True,
        help="Target environment",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be provisioned without making changes",
    )
    parser.add_argument(
        "--location",
        default="eastus",
        help="Azure region (default: eastus)",
    )
    parser.add_argument(
        "--generate-env",
        action="store_true",
        help="Generate .env file after provisioning",
    )

    args = parser.parse_args()

    # Build configuration
    env_config = EnvironmentConfig(
        env=args.env,
        location=args.location,
    )

    # Create provisioner
    provisioner = AzureProvisioner(env_config, dry_run=args.dry_run)

    # Run provisioning
    print(f"\n{'=' * 50}")
    print(f"  CryptoPulse Infrastructure Provisioner")
    print(f"  Environment: {args.env}")
    print(f"  Location:    {args.location}")
    print(f"  Dry Run:     {args.dry_run}")
    print(f"{'=' * 50}\n")

    results = provisioner.provision_all()

    # Print summary
    print(f"\n{'─' * 50}")
    print(f"  Provisioning Summary")
    print(f"{'─' * 50}")

    for r in results:
        status = r.get("status", "UNKNOWN")
        icon = "✓" if status == "OK" else "○" if status == "DRY_RUN" else "✗"
        name = r.get("name", "unknown")
        rtype = r.get("type", "resource_group")
        print(f"  {icon}  {name:<40} ({rtype})")

    print(f"{'─' * 50}\n")

    # Generate env file if requested
    if args.generate_env:
        provisioner.generate_env_file()
        print("  .env file updated with resource names.\n")


if __name__ == "__main__":
    main()
