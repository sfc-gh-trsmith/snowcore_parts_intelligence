#!/usr/bin/env python3
"""
Generate small CSV datasets (<=10K rows) for the UPIP demo.
Large datasets are generated in Snowflake via stored procedures.

Includes: supplier_master, engineering_docs, purchase_orders,
          supplier_risk_scores, consolidation_scenarios
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

RANDOM_SEED = 42

# Extended supplier data with new columns for multi-persona support
SUPPLIERS = [
    # (ID, Name, Region, Rating, LeadTime, Preferred, Spend, Tier, ContractEnd, QualityCert)
    ("SUP001", "Arctic Components", "NA", 4.6, 12, True, 1850000.50, "Preferred", "2027-06-30", "ISO 9001"),
    ("SUP002", "BioFlux Precision", "EU", 4.2, 18, False, 980000.00, "Approved", "2026-12-31", "ISO 13485"),
    ("SUP003", "Helios Fasteners", "APAC", 4.1, 22, False, 410000.75, "Conditional", "2026-03-31", "ISO 9001"),
    ("SUP004", "Northwind Metals", "NA", 4.8, 10, True, 2250000.10, "Preferred", "2028-01-15", "ISO 9001"),
    ("SUP005", "Orchid Motion", "EU", 3.9, 28, False, 620000.90, "Conditional", "2026-06-30", None),
    ("SUP006", "Quanta Actuators", "NA", 4.3, 16, True, 1340000.00, "Preferred", "2027-09-30", "ISO 13485"),
    ("SUP007", "Regulus BioFab", "EU", 4.0, 20, False, 540000.40, "Approved", "2026-11-30", "ISO 13485"),
    ("SUP008", "Sierra Microdrive", "APAC", 4.5, 14, True, 1120000.30, "Preferred", "2027-04-15", "ISO 9001"),
    ("SUP009", "Titan Forge", "NA", 4.7, 11, True, 2055000.20, "Preferred", "2028-03-31", "ISO 9001"),
    ("SUP010", "Umberline Tech", "EU", 3.8, 30, False, 380000.00, "Conditional", "2026-02-28", None),
    ("SUP011", "Vector Valveworks", "NA", 4.4, 15, True, 960000.00, "Approved", "2027-07-31", "ISO 13485"),
    ("SUP012", "Willowridge Polymers", "APAC", 4.1, 19, False, 470000.55, "Approved", "2026-08-31", "ISO 9001"),
]

DOCS = [
    (
        "DOC001",
        "Valve",
        "ISO 13485",
        "Valve assemblies must meet ISO 13485 traceability and include lot-level "
        "material certification and sterilization verification.",
        "docs/iso_13485_valves.md",
    ),
    (
        "DOC002",
        "Actuator",
        "21 CFR Part 11",
        "Actuator compliance requires audit trails for firmware updates, electronic "
        "signatures, and verification of access controls for calibration routines.",
        "docs/21cfr_part11_actuators.md",
    ),
    (
        "DOC003",
        "Motor",
        "ISO 9001",
        "Motor assemblies must document torque testing procedures and retain "
        "calibration records for five years in accordance with ISO 9001.",
        "docs/iso_9001_motors.md",
    ),
    (
        "DOC004",
        "Valve",
        "ISO 14971",
        "Risk management files shall document valve failure modes, hazard analysis, "
        "and mitigation steps per ISO 14971.",
        "docs/iso_14971_valves.md",
    ),
]

# Supplier risk scores - deterministic based on supplier characteristics
SUPPLIER_RISK_SCORES = [
    # (SupplierID, FinancialRisk, DeliveryRisk, QualityRisk, CompositeRisk, SupplyContinuity)
    ("SUP001", 0.15, 0.12, 0.10, 0.12, 0.92),  # Arctic - Preferred, high performer
    ("SUP002", 0.25, 0.22, 0.18, 0.22, 0.85),  # BioFlux - Approved, solid
    ("SUP003", 0.45, 0.38, 0.35, 0.39, 0.68),  # Helios - Conditional, elevated risk
    ("SUP004", 0.10, 0.08, 0.12, 0.10, 0.95),  # Northwind - Top performer
    ("SUP005", 0.55, 0.48, 0.42, 0.48, 0.58),  # Orchid - High risk, no cert
    ("SUP006", 0.18, 0.15, 0.14, 0.16, 0.90),  # Quanta - Preferred, reliable
    ("SUP007", 0.30, 0.28, 0.22, 0.27, 0.80),  # Regulus - Approved, moderate
    ("SUP008", 0.12, 0.14, 0.10, 0.12, 0.93),  # Sierra - Preferred, excellent
    ("SUP009", 0.08, 0.10, 0.08, 0.09, 0.96),  # Titan - Best performer
    ("SUP010", 0.62, 0.55, 0.50, 0.56, 0.52),  # Umberline - Highest risk
    ("SUP011", 0.20, 0.18, 0.16, 0.18, 0.88),  # Vector - Approved, good
    ("SUP012", 0.32, 0.30, 0.28, 0.30, 0.78),  # Willowridge - Approved, moderate
]

# Consolidation scenarios for VP dashboard
CONSOLIDATION_SCENARIOS = [
    # (ID, Name, SourceSuppliers, TargetSupplier, PartsAffected, ProjectedSavings, ImplCost, ROI%, Status)
    (
        "CONS001",
        "NA Fastener Consolidation",
        '["SUP003", "SUP010"]',
        "SUP001",
        145,
        285000.00,
        45000.00,
        533.33,
        "proposed",
    ),
    (
        "CONS002",
        "EU BioTech Supplier Merge",
        '["SUP005", "SUP007"]',
        "SUP002",
        89,
        178000.00,
        32000.00,
        456.25,
        "approved",
    ),
    (
        "CONS003",
        "APAC Motor Standardization",
        '["SUP003", "SUP012"]',
        "SUP008",
        112,
        156000.00,
        28000.00,
        457.14,
        "in_progress",
    ),
    (
        "CONS004",
        "Premium Valve Consolidation",
        '["SUP005", "SUP010"]',
        "SUP011",
        67,
        198000.00,
        55000.00,
        260.00,
        "proposed",
    ),
    (
        "CONS005",
        "Industrial Metals Optimization",
        '["SUP003"]',
        "SUP004",
        234,
        312000.00,
        62000.00,
        403.23,
        "completed",
    ),
    (
        "CONS006",
        "Cross-BU Actuator Alliance",
        '["SUP005", "SUP007", "SUP010"]',
        "SUP006",
        156,
        425000.00,
        85000.00,
        400.00,
        "proposed",
    ),
]

# Part categories for generating purchase orders
PART_CATEGORIES = ["Valve", "Motor", "Fastener", "Actuator", "Sensor", "Pump"]


def write_csv(path: Path, headers: list[str], rows: list[tuple]) -> None:
    """Write rows to CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def generate_purchase_orders(count: int = 500) -> list[tuple]:
    """Generate realistic purchase order data with maverick flags and cycle times."""
    random.seed(RANDOM_SEED)
    base_date = datetime(2025, 1, 1)
    orders = []

    supplier_ids = [s[0] for s in SUPPLIERS]
    preferred_suppliers = {s[0] for s in SUPPLIERS if s[5]}  # PREFERRED_FLAG is index 5

    for i in range(count):
        po_id = f"PO{i+1:06d}"
        part_id = f"G{(i % 1000):09d}"  # Match PART_MASTER GLOBAL_ID pattern
        supplier_id = random.choice(supplier_ids)

        quantity = random.randint(10, 500)
        unit_price = round(random.uniform(15.0, 450.0), 2)
        total_amount = round(quantity * unit_price, 2)

        # Status distribution: 60% received, 25% approved, 15% draft
        status_roll = random.random()
        if status_roll < 0.60:
            status = "received"
        elif status_roll < 0.85:
            status = "approved"
        else:
            status = "draft"

        # Generate realistic dates
        created_offset = random.randint(0, 350)
        created_at = base_date + timedelta(days=created_offset)

        if status in ("approved", "received"):
            # Approval takes 1-5 days
            approval_days = random.randint(1, 5)
            approved_at = created_at + timedelta(days=approval_days)
        else:
            approved_at = None

        if status == "received":
            # Receiving takes 7-45 days after approval
            receiving_days = random.randint(7, 45)
            received_at = approved_at + timedelta(days=receiving_days)
        else:
            received_at = None

        # Maverick flag: 15% of orders are off-contract (not from preferred suppliers)
        # Higher probability if using non-preferred supplier
        is_maverick = (
            supplier_id not in preferred_suppliers and random.random() < 0.35
        ) or (supplier_id in preferred_suppliers and random.random() < 0.05)

        orders.append((
            po_id,
            part_id,
            supplier_id,
            quantity,
            unit_price,
            total_amount,
            status,
            created_at.strftime("%Y-%m-%d %H:%M:%S"),
            approved_at.strftime("%Y-%m-%d %H:%M:%S") if approved_at else None,
            received_at.strftime("%Y-%m-%d %H:%M:%S") if received_at else None,
            is_maverick,
        ))

    return orders


def main(output_dir: str = "data/synthetic") -> None:
    """Generate all synthetic CSV datasets."""
    random.seed(RANDOM_SEED)
    output_path = Path(output_dir)

    # Supplier master with extended columns
    write_csv(
        output_path / "supplier_master.csv",
        [
            "SUPPLIER_ID",
            "SUPPLIER_NAME",
            "SUPPLIER_REGION",
            "RATING",
            "AVG_LEAD_TIME_DAYS",
            "PREFERRED_FLAG",
            "TOTAL_SPEND",
            "SUPPLIER_TIER",
            "CONTRACT_END_DATE",
            "QUALITY_CERTIFICATION",
        ],
        SUPPLIERS,
    )
    print("  - supplier_master.csv (12 rows)")

    # Engineering docs
    write_csv(
        output_path / "engineering_docs.csv",
        ["DOC_ID", "PART_FAMILY", "REGULATORY_STANDARD", "DOC_TEXT", "SOURCE_URI"],
        DOCS,
    )
    print("  - engineering_docs.csv (4 rows)")

    # Purchase orders
    purchase_orders = generate_purchase_orders(500)
    write_csv(
        output_path / "purchase_orders.csv",
        [
            "PO_ID",
            "PART_GLOBAL_ID",
            "SUPPLIER_ID",
            "QUANTITY",
            "UNIT_PRICE",
            "TOTAL_AMOUNT",
            "PO_STATUS",
            "CREATED_AT",
            "APPROVED_AT",
            "RECEIVED_AT",
            "IS_MAVERICK",
        ],
        purchase_orders,
    )
    maverick_count = sum(1 for po in purchase_orders if po[10])
    print(f"  - purchase_orders.csv (500 rows, {maverick_count} maverick)")

    # Supplier risk scores
    write_csv(
        output_path / "supplier_risk_scores.csv",
        [
            "SUPPLIER_ID",
            "FINANCIAL_RISK",
            "DELIVERY_RISK",
            "QUALITY_RISK",
            "COMPOSITE_RISK",
            "SUPPLY_CONTINUITY",
        ],
        SUPPLIER_RISK_SCORES,
    )
    print("  - supplier_risk_scores.csv (12 rows)")

    # Consolidation scenarios
    write_csv(
        output_path / "consolidation_scenarios.csv",
        [
            "SCENARIO_ID",
            "SCENARIO_NAME",
            "SOURCE_SUPPLIERS",
            "TARGET_SUPPLIER_ID",
            "PARTS_AFFECTED",
            "PROJECTED_SAVINGS",
            "IMPLEMENTATION_COST",
            "ROI_PCT",
            "STATUS",
        ],
        CONSOLIDATION_SCENARIOS,
    )
    print("  - consolidation_scenarios.csv (6 rows)")

    print(f"\nGenerated all CSVs in {output_path}")


if __name__ == "__main__":
    main()
