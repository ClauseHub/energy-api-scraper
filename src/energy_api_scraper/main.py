"""Scrape supplier TIL data and submit via the ClauseHub Energy API client.

Uses the energy-api-client-py library for API communication and
authentication. Scraped data flows through the same API endpoints
that any external consumer would use.

Requires:
    CLAUSEHUB_API_URL  — API base URL (default: https://clausehub-energy-api.fly.dev)
    CLAUSEHUB_JWT      — Admin JWT token for authentication

Usage:
    uv run scrape
    uv run scrape --dry-run
"""

import argparse
import os
import sys

import structlog
from energy_api_client import AuthenticatedClient
from energy_api_client.api.suppliers import list_suppliers_suppliers_get

from energy_api_scraper.scrapers.base import BaseScraper, TariffRow
from energy_api_scraper.scrapers.kraken import (
    EdfScraper,
    EonNextScraper,
    OctopusScraper,
)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()

SCRAPERS: list[BaseScraper] = [
    OctopusScraper,
    EdfScraper,
    EonNextScraper,
]

JSONAPI_CT = "application/vnd.api+json"


def _get_or_create_supplier(
    client: AuthenticatedClient,
    name: str,
    cache: dict[str, str],
) -> str:
    """Get existing supplier resource_id or create via API."""
    if name in cache:
        return cache[name]

    response = list_suppliers_suppliers_get.sync_detailed(client=client)
    if response.status_code.value == 200 and response.parsed:
        data = response.parsed.additional_properties.get("data", [])
        for s in data:
            attrs = s.get("attributes", {})
            s_name = attrs.get("name", "")
            s_id = s.get("id", "")
            cache[s_name] = s_id
            if s_name == name:
                return s_id

    httpx_client = client.get_httpx_client()
    resp = httpx_client.post(
        "/suppliers",
        json={
            "data": {
                "type": "suppliers",
                "attributes": {"name": name},
            }
        },
        headers={"Content-Type": JSONAPI_CT},
    )
    if resp.status_code == 201:
        resource_id = resp.json()["data"][0]["id"]
        cache[name] = resource_id
        logger.info("supplier_created", name=name, id=resource_id)
        return resource_id

    logger.error(
        "supplier_create_failed",
        name=name,
        status=resp.status_code,
        body=resp.text[:200],
    )
    raise RuntimeError(f"Failed to create supplier {name}")


def _create_tariff(
    client: AuthenticatedClient,
    supplier_id: str,
    row: TariffRow,
) -> bool:
    """Create a tariff via the API. Returns True on success."""
    consumables = []
    if row.elec_rate:
        consumables.append(
            {
                "type": "Electricity",
                "value": float(row.elec_rate),
                "priceUnit": "pence",
                "consumptionUnit": "kWh",
                "standingChargeValue": float(row.elec_standing)
                if row.elec_standing
                else 0,
                "standingChargePriceUnit": "pence",
                "standingChargePeriod": "per day",
            }
        )
    if row.gas_rate:
        consumables.append(
            {
                "type": "Gas",
                "value": float(row.gas_rate),
                "priceUnit": "pence",
                "consumptionUnit": "kWh",
                "standingChargeValue": float(row.gas_standing)
                if row.gas_standing
                else 0,
                "standingChargePriceUnit": "pence",
                "standingChargePeriod": "per day",
            }
        )

    regions = [{"pesCode": code, "areaName": name} for code, name in row.regions]

    contract: dict[str, object] = {
        "type": row.contract_type,
        "paymentMethod": row.payment_method,
    }
    if row.exit_fee_value:
        contract["exitFees"] = [{"value": float(row.exit_fee_value), "unit": "GBP"}]

    body = {
        "data": {
            "type": "tariffs",
            "attributes": {
                "name": row.tariff_name,
                "clientTariffId": row.client_tariff_id,
                "consumableRange": row.consumable_range,
                "consumables": consumables,
                "regions": regions,
                "contract": [contract],
            },
        }
    }

    httpx_client = client.get_httpx_client()
    resp = httpx_client.post(
        f"/suppliers/{supplier_id}/tariffs",
        json=body,
        headers={"Content-Type": JSONAPI_CT},
    )
    if resp.status_code == 201:
        return True

    logger.warning(
        "tariff_create_failed",
        client_id=row.client_tariff_id,
        status=resp.status_code,
        body=resp.text[:200],
    )
    return False


def _sign_in(api_url: str, email: str, password: str) -> str:
    """Sign in via the ClauseHub API login endpoint."""
    import httpx

    resp = httpx.post(
        f"{api_url}/auth/login",
        json={"email": email, "password": password},
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Login failed: {resp.status_code} {resp.text[:200]}")
    token = resp.json().get("access_token", "")
    if not token:
        raise RuntimeError("Login succeeded but no access_token")
    logger.info("authenticated", email=email)
    return token


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape TIL data and submit via ClauseHub API"
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get(
            "CLAUSEHUB_API_URL", "https://clausehub-energy-api.fly.dev"
        ),
        help="ClauseHub API base URL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape only, don't submit to API",
    )
    args = parser.parse_args()

    all_rows: list[TariffRow] = []
    for scraper in SCRAPERS:
        rows = scraper.run()
        all_rows.extend(rows)

    logger.info(
        "scrape_complete",
        total_tariffs=len(all_rows),
        suppliers=len(SCRAPERS),
    )

    if args.dry_run:
        logger.info("dry_run", message="Skipping API submission")
        return

    # Authenticate via the ClauseHub API login endpoint
    admin_email = os.environ.get("ADMIN_EMAIL", "")
    admin_password = os.environ.get("ADMIN_PASSWORD", "")

    if not all([admin_email, admin_password]):
        logger.error(
            "missing_credentials",
            message="Set ADMIN_EMAIL and ADMIN_PASSWORD",
        )
        sys.exit(1)

    token = _sign_in(args.api_url, admin_email, admin_password)

    client = AuthenticatedClient(
        base_url=args.api_url,
        token=token,
    )
    supplier_cache: dict[str, str] = {}
    stats = {"created": 0, "failed": 0}

    with client:
        for row in all_rows:
            try:
                supplier_id = _get_or_create_supplier(
                    client, row.supplier_name, supplier_cache
                )
                if _create_tariff(client, supplier_id, row):
                    stats["created"] += 1
                else:
                    stats["failed"] += 1
            except Exception:
                stats["failed"] += 1
                logger.exception(
                    "submission_error",
                    client_id=row.client_tariff_id,
                )

    logger.info("submission_complete", **stats)


if __name__ == "__main__":
    main()
