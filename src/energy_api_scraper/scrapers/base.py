"""Base class for supplier scrapers.

Each supplier scraper inherits from this and implements fetch_tariffs().
The base class handles CSV output in the format expected by ingest_tariffs.py.
"""

import csv
from dataclasses import dataclass, field
from pathlib import Path

import structlog

logger = structlog.get_logger()

# Octopus API region letter → PES code + area name mapping
OCTOPUS_REGION_MAP: dict[str, tuple[str, str]] = {
    "_A": ("10", "Eastern"),
    "_B": ("11", "East Midlands"),
    "_C": ("12", "London"),
    "_D": ("13", "Merseyside and North Wales"),
    "_E": ("14", "Midlands"),
    "_F": ("15", "Northern"),
    "_G": ("16", "North Western"),
    "_H": ("17", "Southern"),
    "_J": ("18", "South Eastern"),
    "_K": ("19", "South Wales"),
    "_L": ("20", "South Western"),
    "_M": ("21", "Yorkshire"),
    "_N": ("22", "South Scotland"),
    "_P": ("23", "North Scotland"),
}

# EDF region name → PES code mapping
EDF_REGION_MAP: dict[str, str] = {
    "East Midlands": "11",
    "Eastern": "10",
    "London": "12",
    "Midlands": "14",
    "North East": "15",
    "North Scotland": "23",
    "North Wales and Merseyside": "13",
    "North Wales\nand Merseyside": "13",
    "North West": "16",
    "South East": "18",
    "South Scotland": "22",
    "South Wales": "19",
    "South West": "20",
    "Southern": "17",
    "Yorkshire": "21",
}

CSV_HEADER = [
    "supplier_name",
    "tariff_name",
    "client_tariff_id",
    "consumable_range",
    "elec_rate",
    "elec_standing",
    "gas_rate",
    "gas_standing",
    "contract_type",
    "payment_method",
    "exit_fee_value",
    "regions",
]


@dataclass
class TariffRow:
    """A single tariff row matching the ingestion CSV schema."""

    supplier_name: str
    tariff_name: str
    client_tariff_id: str
    consumable_range: str  # Dual, Electricity, Gas
    elec_rate: str = ""  # pence/kWh
    elec_standing: str = ""  # pence/day
    gas_rate: str = ""  # pence/kWh
    gas_standing: str = ""  # pence/day
    contract_type: str = "Variable"  # Variable, Fixed
    payment_method: str = "Monthly Direct Debit"
    exit_fee_value: str = ""
    regions: list[tuple[str, str]] = field(default_factory=list)

    def to_csv_dict(self) -> dict[str, str]:
        regions_str = ";".join(f"{code}|{name}" for code, name in self.regions)
        return {
            "supplier_name": self.supplier_name,
            "tariff_name": self.tariff_name,
            "client_tariff_id": self.client_tariff_id,
            "consumable_range": self.consumable_range,
            "elec_rate": self.elec_rate,
            "elec_standing": self.elec_standing,
            "gas_rate": self.gas_rate,
            "gas_standing": self.gas_standing,
            "contract_type": self.contract_type,
            "payment_method": self.payment_method,
            "exit_fee_value": self.exit_fee_value,
            "regions": regions_str,
        }


class BaseScraper:
    """Base class for supplier scrapers."""

    supplier_name: str = "Unknown"

    def fetch_tariffs(self) -> list[TariffRow]:
        raise NotImplementedError

    def run(self) -> list[TariffRow]:
        logger.info("scraper_started", supplier=self.supplier_name)
        try:
            rows = self.fetch_tariffs()
            logger.info(
                "scraper_complete",
                supplier=self.supplier_name,
                tariff_count=len(rows),
            )
            return rows
        except Exception:
            logger.exception("scraper_failed", supplier=self.supplier_name)
            return []


def write_csv(rows: list[TariffRow], output_path: str) -> None:
    """Write tariff rows to CSV in the ingestion format."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_dict())

    logger.info("csv_written", path=str(path), row_count=len(rows))
