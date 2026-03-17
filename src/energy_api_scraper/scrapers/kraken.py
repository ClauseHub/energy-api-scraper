"""Generic Kraken Technology platform scraper.

Octopus Energy, EDF Energy, and E.ON Next all run on the Kraken platform
and expose the same public REST API at /v1/products/.

This module provides a generic scraper that works for any Kraken-based
supplier, configured only by the API base URL and supplier name.

Each product returns TIL-standard data: tariff name, unit rates, standing
charges, exit fees, and discounts — broken down by PES region and payment
method.
"""

import httpx
import structlog

from energy_api_scraper.scrapers.base import OCTOPUS_REGION_MAP, BaseScraper, TariffRow

logger = structlog.get_logger()

# Products to skip (export tariffs, outgoing, flux export, etc.)
SKIP_KEYWORDS = ("EXPORT", "OUTGOING", "FLUX-EXPORT")

# Kraken payment type keys → our payment method names
PAYMENT_TYPE_MAP: dict[str, str] = {
    "direct_debit_monthly": "Monthly Direct Debit",
    "direct_debit_quarterly": "Quarterly Direct Debit",
    "prepayment": "Monthly Cash or Cheque",
    "varying": "Monthly Direct Debit",  # EDF uses "varying" for DD
}

# Preferred payment type to extract (in priority order)
PREFERRED_PAYMENT_TYPES = [
    "direct_debit_monthly",
    "varying",
    "prepayment",
]


class KrakenScraper(BaseScraper):
    """Generic scraper for any Kraken Technology platform supplier."""

    def __init__(
        self,
        supplier_name: str,
        api_base: str,
        client_id_prefix: str,
    ) -> None:
        self.supplier_name = supplier_name
        self.api_base = api_base.rstrip("/")
        self.client_id_prefix = client_id_prefix

    def fetch_tariffs(self) -> list[TariffRow]:
        rows: list[TariffRow] = []
        products = self._fetch_products()

        for product in products:
            code = product["code"]
            name = product["display_name"]
            is_variable = product.get("is_variable", False)

            # Skip export/outgoing tariffs
            if any(kw in code.upper() for kw in SKIP_KEYWORDS):
                continue

            logger.info(
                "fetching_product",
                supplier=self.supplier_name,
                code=code,
                name=name,
            )

            try:
                product_rows = self._fetch_product(code, name, is_variable)
                rows.extend(product_rows)
            except Exception:
                logger.exception(
                    "product_fetch_failed",
                    supplier=self.supplier_name,
                    code=code,
                )

        return rows

    def _fetch_products(self) -> list[dict]:
        """Fetch all products from the Kraken API."""
        all_products: list[dict] = []
        url: str | None = f"{self.api_base}/v1/products/"

        while url:
            resp = httpx.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            all_products.extend(data.get("results", []))
            url = data.get("next")

        return all_products

    def _fetch_product(
        self, code: str, name: str, is_variable: bool
    ) -> list[TariffRow]:
        resp = httpx.get(f"{self.api_base}/v1/products/{code}/", timeout=30)
        resp.raise_for_status()
        data = resp.json()

        rows: list[TariffRow] = []

        elec_tariffs = data.get("single_register_electricity_tariffs", {})
        gas_tariffs = data.get("single_register_gas_tariffs", {})

        for region_key, (pes_code, area_name) in OCTOPUS_REGION_MAP.items():
            elec_region = elec_tariffs.get(region_key, {})
            gas_region = gas_tariffs.get(region_key, {})

            # Find the best available payment type
            elec_data = self._pick_payment_type(elec_region)
            gas_data = self._pick_payment_type(gas_region)

            if not elec_data and not gas_data:
                continue

            has_elec = bool(elec_data)
            has_gas = bool(gas_data)
            if has_elec and has_gas:
                consumable_range = "Dual"
            elif has_elec:
                consumable_range = "Electricity"
            else:
                consumable_range = "Gas"

            # Extract rates (inc VAT)
            elec_rate = ""
            elec_standing = ""
            exit_fee = ""
            if has_elec:
                sr = elec_data.get("standard_unit_rate_inc_vat")
                sc = elec_data.get("standing_charge_inc_vat")
                ef = elec_data.get("exit_fees_inc_vat")
                if sr is not None:
                    elec_rate = f"{sr:.2f}"
                if sc is not None:
                    elec_standing = f"{sc:.2f}"
                if ef and ef > 0:
                    exit_fee = f"{ef:.2f}"

            gas_rate = ""
            gas_standing = ""
            if has_gas:
                sr = gas_data.get("standard_unit_rate_inc_vat")
                sc = gas_data.get("standing_charge_inc_vat")
                if sr is not None:
                    gas_rate = f"{sr:.2f}"
                if sc is not None:
                    gas_standing = f"{sc:.2f}"

            client_id = f"{self.client_id_prefix}-{code}-{pes_code}"

            row = TariffRow(
                supplier_name=self.supplier_name,
                tariff_name=name,
                client_tariff_id=client_id,
                consumable_range=consumable_range,
                elec_rate=elec_rate,
                elec_standing=elec_standing,
                gas_rate=gas_rate,
                gas_standing=gas_standing,
                contract_type="Variable" if is_variable else "Fixed",
                payment_method="Monthly Direct Debit",
                exit_fee_value=exit_fee,
                regions=[(pes_code, area_name)],
            )
            rows.append(row)

        return rows

    def _pick_payment_type(self, region_data: dict) -> dict | None:
        """Pick the best available payment type from region data."""
        for pt in PREFERRED_PAYMENT_TYPES:
            if pt in region_data:
                return region_data[pt]
        # Fall back to first available
        if region_data:
            return next(iter(region_data.values()))
        return None


# ── Pre-configured supplier instances ────────────────────────────────────────

OctopusScraper = KrakenScraper(
    supplier_name="Octopus Energy",
    api_base="https://api.octopus.energy",
    client_id_prefix="OCT",
)

EdfScraper = KrakenScraper(
    supplier_name="EDF Energy",
    api_base="https://api.edfgb-kraken.energy",
    client_id_prefix="EDF",
)

EonNextScraper = KrakenScraper(
    supplier_name="E.ON Next",
    api_base="https://api.eonnext-kraken.energy",
    client_id_prefix="EON",
)
