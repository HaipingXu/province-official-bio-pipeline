"""
Phase 5: Excel Export

Exports all rows to a single Excel file: {city}_officials.xlsx
"""

import argparse
import json
from pathlib import Path

import pandas as pd

from config import COLUMNS, OUTPUT_DIR, LOGS_DIR
from postprocess import run_postprocess

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def rows_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Convert flat row dicts to DataFrame with correct column order."""
    df = pd.DataFrame(rows)

    # Ensure all columns exist
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # Handle empty column W (index 22, key "")
    if "" in df.columns:
        df.rename(columns={"": "（空）"}, inplace=True)
        final_columns = [c if c != "" else "（空）" for c in COLUMNS]
    else:
        final_columns = COLUMNS.copy()
        # Find W column and ensure it exists
        if "（空）" not in df.columns:
            df["（空）"] = ""
        final_columns = [c if c != "" else "（空）" for c in COLUMNS]

    # Select only defined columns in order
    available = [c for c in final_columns if c in df.columns]
    df = df[available]

    return df


def save_excel_with_style(df: pd.DataFrame, path: Path, sheet_name: str = "履历数据"):
    """Save DataFrame to Excel with basic formatting."""
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

        # Auto-adjust column widths
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = 0
            col_letter = col_cells[0].column_letter
            for cell in col_cells:
                try:
                    cell_len = len(str(cell.value)) if cell.value else 0
                    if cell_len > max_len:
                        max_len = cell_len
                except Exception:
                    pass
            # Cap at 40 chars width, minimum 8
            ws.column_dimensions[col_letter].width = min(max(max_len + 2, 8), 40)

    print(f"  ✓ Saved {len(df)} rows to {path.name}")


def export(
    rows: list[dict],
    city: str,
    province: str,
) -> dict:
    """Export all rows to a single Excel file."""
    print(f"\n=== Phase 5: Excel Export ===")

    df = rows_to_dataframe(rows)
    all_path = OUTPUT_DIR / f"{city}_officials.xlsx"
    save_excel_with_style(df, all_path, sheet_name="全部履历")

    print(f"\n✓ Export complete: {all_path.name} ({len(df)} rows)")
    return {
        "total_rows": len(df),
        "paths": {"officials": str(all_path)},
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export results to Excel")
    parser.add_argument("--input", default="logs/deepseek_results.json")
    parser.add_argument("--verif", default="logs/verification_report.json")
    parser.add_argument("--city", default="深圳")
    parser.add_argument("--province", default="广东")
    args = parser.parse_args()

    rows = run_postprocess(
        deepseek_results_path=Path(args.input),
        verification_report_path=Path(args.verif),
        city=args.city,
        province=args.province,
    )
    export(rows, args.city, args.province)
