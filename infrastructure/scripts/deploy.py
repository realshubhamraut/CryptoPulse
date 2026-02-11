#!/usr/bin/env python3
"""
CryptoPulse - Python Deployment Script

Replaces the Bash deploy.sh with a Python-native deployment workflow.
Validates prerequisites, provisions infrastructure, and deploys services.

Usage:
    python deploy.py --env dev
    python deploy.py --env prod --skip-provision
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from cryptopulse.logging import get_logger

logger = get_logger(__name__, component="deploy")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

ENVIRONMENTS = {"dev", "staging", "prod"}


# =============================================================================
# Utility Helpers
# =============================================================================

def _run(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a shell command with logging."""
    logger.info("running_command", cmd=" ".join(cmd))
    return subprocess.run(cmd, check=check, capture_output=capture, text=True)


def _check_command(name: str) -> bool:
    """Check if a CLI tool is available."""
    result = subprocess.run(
        ["which", name], capture_output=True, text=True
    )
    return result.returncode == 0


# =============================================================================
# Prerequisite Checks
# =============================================================================

def check_prerequisites() -> list[str]:
    """Validate all required tools are installed. Returns list of errors."""
    errors = []

    required_tools = {
        "python3": "Python 3.10+",
        "az": "Azure CLI (brew install azure-cli)",
        "docker": "Docker (https://docker.com)",
    }

    for tool, description in required_tools.items():
        if not _check_command(tool):
            errors.append(f"Missing: {description}")

    # Check Python version
    if sys.version_info < (3, 10):
        errors.append(f"Python 3.10+ required (found {sys.version})")

    return errors


def check_azure_login() -> bool:
    """Verify Azure CLI is logged in."""
    result = _run(["az", "account", "show"], check=False, capture=True)

    if result.returncode != 0:
        logger.warning("azure_not_logged_in")
        print("\n⚠  Not logged in to Azure. Running 'az login'...")
        _run(["az", "login"])

    # Get subscription info
    result = _run(
        ["az", "account", "show", "--query", "name", "-o", "tsv"],
        capture=True,
    )
    print(f"✓  Azure subscription: {result.stdout.strip()}")
    return True


# =============================================================================
# Deployment Steps
# =============================================================================

def provision_infrastructure(env: str, dry_run: bool = False) -> None:
    """Run the Python infrastructure provisioner."""
    provisioner_path = SCRIPT_DIR.parent / "azure_provisioner.py"

    cmd = [
        sys.executable,
        str(provisioner_path),
        "--env", env,
    ]

    if dry_run:
        cmd.append("--dry-run")

    cmd.append("--generate-env")

    _run(cmd)


def deploy_functions(env: str) -> None:
    """Deploy Azure Functions."""
    logger.info("deploying_functions", env=env)

    func_app_name = f"func-cryptopulse-{env}-ingest"

    if _check_command("func"):
        _run([
            "func", "azure", "functionapp", "publish",
            func_app_name,
            "--python",
        ])
    else:
        logger.warning(
            "func_cli_not_installed",
            hint="Install Azure Functions Core Tools: npm install -g azure-functions-core-tools@4",
        )
        print("⚠  Azure Functions CLI not found. Skipping function deployment.")


def deploy_api(env: str) -> None:
    """Deploy the FastAPI application."""
    logger.info("deploying_api", env=env)

    # For production, this would push a Docker image to ACR
    # and update the App Service / Container App
    if env == "dev":
        print("ℹ  For local development, run: uvicorn api.main:app --reload")
    else:
        print(f"ℹ  Deploy API to Azure App Service for '{env}' environment.")


# =============================================================================
# CLI Entry Point
# =============================================================================

def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="CryptoPulse Deployment Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
        help="Preview deployment without making changes",
    )
    parser.add_argument(
        "--skip-provision",
        action="store_true",
        help="Skip infrastructure provisioning",
    )
    parser.add_argument(
        "--skip-functions",
        action="store_true",
        help="Skip Azure Functions deployment",
    )

    args = parser.parse_args()

    # Banner
    print()
    print("╔════════════════════════════════════════════╗")
    print("║     CryptoPulse Deployment                ║")
    print(f"║ Environment: {args.env:<30}║")
    print(f"║ Dry Run:     {str(args.dry_run):<30}║")
    print("╚════════════════════════════════════════════╝")
    print()

    # 1. Prerequisites
    print("─── Checking prerequisites ───")
    errors = check_prerequisites()
    if errors:
        for e in errors:
            print(f"  ✗  {e}")
        sys.exit(1)
    print("  ✓  All prerequisites met\n")

    # 2. Azure login
    print("─── Azure authentication ───")
    check_azure_login()
    print()

    # 3. Provision infrastructure
    if not args.skip_provision:
        print("─── Provisioning infrastructure ───")
        provision_infrastructure(args.env, dry_run=args.dry_run)
        print()

    # 4. Deploy Functions
    if not args.skip_functions:
        print("─── Deploying Azure Functions ───")
        deploy_functions(args.env)
        print()

    # 5. Deploy API
    print("─── Deploying API ───")
    deploy_api(args.env)
    print()

    # Done
    print("╔════════════════════════════════════════════╗")
    print("║         Deployment Complete!               ║")
    print("╚════════════════════════════════════════════╝")
    print()
    print("Next steps:")
    print("  1. Set secrets in Key Vault:")
    print(f"     az keyvault secret set --vault-name kv-cryptopulse-{args.env} --name binance-api-key --value YOUR_KEY")
    print()
    print("  2. Verify API health:")
    print("     curl https://api-cryptopulse-{env}.azurewebsites.net/health")
    print()


if __name__ == "__main__":
    main()
