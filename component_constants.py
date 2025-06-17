import polars as pl
from datetime import datetime

bp_df = pl.concat(
    [
        pl.DataFrame(
            [
                {"month_date": "2025-04-01", "bp_sale": 1427906, "bp_repair_cost": 1354432},
                {"month_date": "2025-05-01", "bp_sale": 1135285, "bp_repair_cost": 1076833},
                {"month_date": "2025-06-01", "bp_sale": 1215772, "bp_repair_cost": 1193359},
                {"month_date": "2025-07-01", "bp_sale": 1176895, "bp_repair_cost": 1158096},
                {"month_date": "2025-08-01", "bp_sale": 998006, "bp_repair_cost": 1107236},
                {"month_date": "2025-09-01", "bp_sale": 998347, "bp_repair_cost": 1085013},
                {"month_date": "2025-10-01", "bp_sale": 643246, "bp_repair_cost": 634031},
                {"month_date": "2025-11-01", "bp_sale": 491517, "bp_repair_cost": 459870},
                {"month_date": "2025-12-01", "bp_sale": 179980, "bp_repair_cost": 176861},
                {"month_date": "2026-01-01", "bp_sale": 468983, "bp_repair_cost": 399166},
                {"month_date": "2026-02-01", "bp_sale": 692134, "bp_repair_cost": 563387},
                {"month_date": "2026-03-01", "bp_sale": 513376, "bp_repair_cost": 585005},
            ]
        ).with_columns(site_name=pl.lit("MEL")),
        pl.DataFrame(
            [
                {"month_date": "2025-04-01", "bp_sale": 273238, "bp_repair_cost": 539929},
                {"month_date": "2025-05-01", "bp_sale": 218614, "bp_repair_cost": 653760},
                {"month_date": "2025-06-01", "bp_sale": 351812, "bp_repair_cost": 650925},
                {"month_date": "2025-07-01", "bp_sale": 712487, "bp_repair_cost": 963389},
                {"month_date": "2025-08-01", "bp_sale": 777726, "bp_repair_cost": 796701},
                {"month_date": "2025-09-01", "bp_sale": 598596, "bp_repair_cost": 903969},
                {"month_date": "2025-10-01", "bp_sale": 82429, "bp_repair_cost": 607540},
                {"month_date": "2025-11-01", "bp_sale": 1100315, "bp_repair_cost": 1060073},
                {"month_date": "2025-12-01", "bp_sale": 793449, "bp_repair_cost": 860422},
                {"month_date": "2026-01-01", "bp_sale": 618807, "bp_repair_cost": 764631},
                {"month_date": "2026-02-01", "bp_sale": 1252529, "bp_repair_cost": 1150039},
                {"month_date": "2026-03-01", "bp_sale": 1214221, "bp_repair_cost": 1179123},
            ]
        ).with_columns(site_name=pl.lit("SPENCE")),
    ]
).with_columns(month_date=pl.col("month_date").str.to_date("%Y-%m-%d"))


# map months to the corresponding cc_amount.
component_data_raw_960E = [
    {"component_name": "blower_parrilla", "month": "2025-04-01", "cc_amount": 2},
    {"component_name": "blower_parrilla", "month": "2025-05-01", "cc_amount": 1},
    {"component_name": "blower_parrilla", "month": "2025-07-01", "cc_amount": 4},
    {"component_name": "blower_parrilla", "month": "2025-09-01", "cc_amount": 1},
    {"component_name": "blower_parrilla", "month": "2025-10-01", "cc_amount": 2},
    {"component_name": "blower_parrilla", "month": "2025-11-01", "cc_amount": 1},
    {"component_name": "blower_parrilla", "month": "2026-01-01", "cc_amount": 2},
    {"component_name": "blower_parrilla", "month": "2026-03-01", "cc_amount": 1},
    {"component_name": "cilindro_direccion", "month": "2025-04-01", "cc_amount": 6},
    {"component_name": "cilindro_direccion", "month": "2025-05-01", "cc_amount": 4},
    {"component_name": "cilindro_direccion", "month": "2025-06-01", "cc_amount": 2},
    {"component_name": "cilindro_direccion", "month": "2025-07-01", "cc_amount": 7},
    {"component_name": "cilindro_direccion", "month": "2025-08-01", "cc_amount": 3},
    {"component_name": "cilindro_direccion", "month": "2025-09-01", "cc_amount": 2},
    {"component_name": "cilindro_direccion", "month": "2025-11-01", "cc_amount": 1},
    {"component_name": "cilindro_direccion", "month": "2025-12-01", "cc_amount": 2},
    {"component_name": "cilindro_direccion", "month": "2026-01-01", "cc_amount": 2},
    {"component_name": "cilindro_direccion", "month": "2026-02-01", "cc_amount": 1},
    {"component_name": "cilindro_direccion", "month": "2026-03-01", "cc_amount": 3},
    {"component_name": "cilindro_levante", "month": "2025-06-01", "cc_amount": 2},
    {"component_name": "motor_traccion", "month": "2025-04-01", "cc_amount": 2},
    {"component_name": "motor_traccion", "month": "2025-06-01", "cc_amount": 3},
    {"component_name": "motor_traccion", "month": "2025-07-01", "cc_amount": 1},
    {"component_name": "motor_traccion", "month": "2025-08-01", "cc_amount": 1},
    {"component_name": "conjunto_maza_suspension", "month": "2025-04-01", "cc_amount": 4},
    {"component_name": "conjunto_maza_suspension", "month": "2025-05-01", "cc_amount": 4},
    {"component_name": "conjunto_maza_suspension", "month": "2025-06-01", "cc_amount": 4},
    {"component_name": "conjunto_maza_suspension", "month": "2025-07-01", "cc_amount": 5},
    {"component_name": "conjunto_maza_suspension", "month": "2025-08-01", "cc_amount": 3},
    {"component_name": "conjunto_maza_suspension", "month": "2025-09-01", "cc_amount": 5},
    {"component_name": "conjunto_maza_suspension", "month": "2025-10-01", "cc_amount": 4},
    {"component_name": "conjunto_maza_suspension", "month": "2025-11-01", "cc_amount": 4},
    {"component_name": "conjunto_maza_suspension", "month": "2025-12-01", "cc_amount": 3},
    {"component_name": "conjunto_maza_suspension", "month": "2026-01-01", "cc_amount": 1},
    {"component_name": "conjunto_maza_suspension", "month": "2026-02-01", "cc_amount": 3},
    {"component_name": "conjunto_maza_suspension", "month": "2026-03-01", "cc_amount": 2},
    {"component_name": "suspension_trasera", "month": "2025-06-01", "cc_amount": 2},
    {"component_name": "suspension_trasera", "month": "2025-07-01", "cc_amount": 3},
    {"component_name": "suspension_trasera", "month": "2025-08-01", "cc_amount": 4},
    {"component_name": "suspension_trasera", "month": "2025-09-01", "cc_amount": 3},
    {"component_name": "suspension_trasera", "month": "2025-10-01", "cc_amount": 3},
    {"component_name": "suspension_trasera", "month": "2025-11-01", "cc_amount": 3},
    {"component_name": "suspension_trasera", "month": "2025-12-01", "cc_amount": 2},
    {"component_name": "suspension_trasera", "month": "2026-01-01", "cc_amount": 4},
    {"component_name": "suspension_trasera", "month": "2026-02-01", "cc_amount": 3},
    {"component_name": "suspension_trasera", "month": "2026-03-01", "cc_amount": 1},
    {"component_name": "modulo_potencia", "month": "2025-04-01", "cc_amount": 3},
    {"component_name": "modulo_potencia", "month": "2025-05-01", "cc_amount": 1},
    {"component_name": "modulo_potencia", "month": "2025-06-01", "cc_amount": 2},
    {"component_name": "modulo_potencia", "month": "2025-07-01", "cc_amount": 2},
    {"component_name": "modulo_potencia", "month": "2025-08-01", "cc_amount": 2},
    {"component_name": "modulo_potencia", "month": "2025-09-01", "cc_amount": 1},
    {"component_name": "modulo_potencia", "month": "2025-10-01", "cc_amount": 1},
    {"component_name": "modulo_potencia", "month": "2025-11-01", "cc_amount": 1},
    {"component_name": "modulo_potencia", "month": "2025-12-01", "cc_amount": 1},
    {"component_name": "modulo_potencia", "month": "2026-01-01", "cc_amount": 2},
    {"component_name": "modulo_potencia", "month": "2026-02-01", "cc_amount": 1},
]

component_data_raw_930E = [
    {"component_name": "blower_parrilla", "month": "2025-10-01", "cc_amount": 1},
    {"component_name": "cilindro_direccion", "month": "2025-05-01", "cc_amount": 1},
    {"component_name": "cilindro_direccion", "month": "2026-03-01", "cc_amount": 2},
    {"component_name": "cilindro_levante", "month": "2025-05-01", "cc_amount": 2},
    {"component_name": "cilindro_levante", "month": "2025-06-01", "cc_amount": 1},
    {"component_name": "motor_traccion", "month": "2025-07-01", "cc_amount": 1},
    {"component_name": "motor_traccion", "month": "2026-03-01", "cc_amount": 1},
    {"component_name": "conjunto_maza_suspension", "month": "2025-08-01", "cc_amount": 1},
    {"component_name": "conjunto_maza_suspension", "month": "2025-09-01", "cc_amount": 2},
    {"component_name": "conjunto_maza_suspension", "month": "2026-03-01", "cc_amount": 1},
    {"component_name": "suspension_trasera", "month": "2025-08-01", "cc_amount": 1},
    {"component_name": "suspension_trasera", "month": "2025-09-01", "cc_amount": 2},
]

component_data_raw_980E = [
    {"component_name": "cilindro_direccion", "changeout_week": "W23-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W24-2025"},
    {"component_name": "modulo_potencia", "changeout_week": "W25-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W25-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W25-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W25-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W26-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W26-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W26-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W27-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W27-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W27-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W27-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W28-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W28-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W28-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W28-2025"},
    {"component_name": "modulo_potencia", "changeout_week": "W29-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W29-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W29-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W29-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W30-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W30-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W30-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W30-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W30-2025"},
    {"component_name": "modulo_potencia", "changeout_week": "W31-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W31-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W31-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W31-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W31-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W31-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W32-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W32-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W32-2025"},
    {"component_name": "modulo_potencia", "changeout_week": "W33-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W33-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W33-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W33-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W34-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W34-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W34-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W34-2025"},
    # Data from third image (W35-W36)
    {"component_name": "motor_traccion", "changeout_week": "W35-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W35-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W35-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W35-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W35-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W36-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W36-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W36-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W36-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W36-2025"},
    # Data from fourth image (W37-W38)
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W37-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W37-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W37-2025"},
    {"component_name": "modulo_potencia", "changeout_week": "W38-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W38-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W38-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W38-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W38-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W38-2025"},
    # Data from fifth image (W39-W40)
    {"component_name": "cilindro_direccion", "changeout_week": "W39-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W39-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W39-2025"},
    {"component_name": "modulo_potencia", "changeout_week": "W40-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W40-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W40-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W40-2025"},
    # Data from sixth image (W41-W42)
    {"component_name": "cilindro_levante", "changeout_week": "W41-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W41-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W42-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W42-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W42-2025"},
    # Data from seventh image (W43-W44)
    {"component_name": "motor_traccion", "changeout_week": "W43-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W43-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W43-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W43-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W43-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W44-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W44-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W44-2025"},
    # Data from eighth image (W45-W46)
    {"component_name": "motor_traccion", "changeout_week": "W45-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W45-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W46-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W46-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W46-2025"},
    # Data from ninth image (W47-W48)
    {"component_name": "modulo_potencia", "changeout_week": "W47-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W47-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W47-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W48-2025"},
    {"component_name": "blower_parrilla", "changeout_week": "W48-2025"},
    # Data from tenth image (W49-W50)
    {"component_name": "motor_traccion", "changeout_week": "W49-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W49-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W49-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W50-2025"},
    {"component_name": "cilindro_direccion", "changeout_week": "W50-2025"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W50-2025"},
    # Data from eleventh image (W52 & W01)
    {"component_name": "modulo_potencia", "changeout_week": "W52-2025"},
    {"component_name": "motor_traccion", "changeout_week": "W52-2025"},
    {"component_name": "suspension_trasera", "changeout_week": "W52-2025"},
    {"component_name": "cilindro_levante", "changeout_week": "W01-2026"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W01-2026"},
    {"component_name": "suspension_trasera", "changeout_week": "W01-2026"},
    {"component_name": "blower_parrilla", "changeout_week": "W01-2026"},
    # Data from twelfth image (W02-W03)
    {"component_name": "motor_traccion", "changeout_week": "W02-2026"},
    {"component_name": "cilindro_levante", "changeout_week": "W02-2026"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W02-2026"},
    {"component_name": "suspension_trasera", "changeout_week": "W02-2026"},
    {"component_name": "blower_parrilla", "changeout_week": "W02-2026"},
    {"component_name": "motor_traccion", "changeout_week": "W03-2026"},
    {"component_name": "cilindro_direccion", "changeout_week": "W03-2026"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W03-2026"},
    # Data from thirteenth image (W04-W05)
    {"component_name": "motor_traccion", "changeout_week": "W04-2026"},
    {"component_name": "cilindro_direccion", "changeout_week": "W04-2026"},
    {"component_name": "suspension_trasera", "changeout_week": "W04-2026"},
    {"component_name": "blower_parrilla", "changeout_week": "W04-2026"},
    {"component_name": "motor_traccion", "changeout_week": "W05-2026"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W05-2026"},
    {"component_name": "suspension_trasera", "changeout_week": "W05-2026"},
    # Data from fourteenth image (W06-W07)
    {"component_name": "cilindro_direccion", "changeout_week": "W06-2026"},
    {"component_name": "suspension_trasera", "changeout_week": "W06-2026"},
    {"component_name": "blower_parrilla", "changeout_week": "W06-2026"},
    {"component_name": "motor_traccion", "changeout_week": "W07-2026"},
    {"component_name": "cilindro_direccion", "changeout_week": "W07-2026"},
    {"component_name": "cilindro_direccion", "changeout_week": "W07-2026"},  # Second entry in the same cell
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W07-2026"},
    {"component_name": "blower_parrilla", "changeout_week": "W07-2026"},
    # Data from fifteenth image (W08-W09)
    {"component_name": "motor_traccion", "changeout_week": "W08-2026"},
    {"component_name": "cilindro_direccion", "changeout_week": "W08-2026"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W08-2026"},
    {"component_name": "suspension_trasera", "changeout_week": "W08-2026"},
    {"component_name": "motor_traccion", "changeout_week": "W09-2026"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W09-2026"},
    {"component_name": "blower_parrilla", "changeout_week": "W09-2026"},
    # Data from sixteenth image (W10-W11)
    {"component_name": "motor_traccion", "changeout_week": "W10-2026"},
    {"component_name": "cilindro_direccion", "changeout_week": "W10-2026"},
    {"component_name": "suspension_trasera", "changeout_week": "W10-2026"},
    {"component_name": "blower_parrilla", "changeout_week": "W10-2026"},
    {"component_name": "motor_traccion", "changeout_week": "W11-2026"},
    {"component_name": "cilindro_direccion", "changeout_week": "W11-2026"},
    {"component_name": "suspension_trasera", "changeout_week": "W11-2026"},
    {"component_name": "blower_parrilla", "changeout_week": "W11-2026"},
    # Data from seventeenth image (W12-W13)
    {"component_name": "motor_traccion", "changeout_week": "W12-2026"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W12-2026"},
    {"component_name": "motor_traccion", "changeout_week": "W13-2026"},
    {"component_name": "conjunto_maza_suspension", "changeout_week": "W13-2026"},
    {"component_name": "suspension_trasera", "changeout_week": "W13-2026"},
    {"component_name": "blower_parrilla", "changeout_week": "W13-2026"},
]
component_data_raw_spence = pl.DataFrame(component_data_raw_980E)

component_data_raw_spence = (
    component_data_raw_spence.with_columns(
        [
            pl.col("changeout_week")
            .str.extract(r"W(\d+)-(\d+)", 1)
            .cast(pl.Int32)
            .alias("week_num"),  # Extract week number
            pl.col("changeout_week").str.extract(r"W(\d+)-(\d+)", 2).cast(pl.Int32).alias("year_num"),  # Extract year
        ]
    )
    .with_columns(
        [
            # Create the date: January 1st + (week_number - 1) weeks
            (pl.date(pl.col("year_num"), 1, 1) + pl.duration(weeks=pl.col("week_num") - 1)).alias("changeout_date")
        ]
    )
    .drop(["week_num", "year_num"])  # Clean up temporary columns
    .with_columns(
        [
            # Create a 'month' column by converting the date and truncating it to the month
            pl.col("changeout_date")
            .dt.truncate("1mo")
            .alias("month")
        ]
    )
    .group_by("component_name", "month")  # Group by component and the new month column.
    .agg(
        # Count the number of entries (rows) in each group.
        pl.len().alias("cc_amount")
    )
    .sort("component_name", "month")  # Sort the final result for readability.
)


def calculate_prorrata_sale(df):
    """
    Calculate prorrata_sale (Costo Prorrata) using vectorized Polars operations

    Formula:
    - If component_hours in range [mtbo, mtbo*1.05]: gfa_overhaul_rate * (1 + ((effective_hours - mtbo) / mtbo))
    - Otherwise: gfa_overhaul_rate * (effective_hours / mtbo)

    Args:
        df: DataFrame with required columns from schema

    Returns:
        DataFrame with updated 'prorrata_sale' column
    """

    result_df = df.with_columns(
        [
            # Calculate effective hours: M = MIN(component_hours, mtbo_105)
            pl.min_horizontal([pl.col("proj_component_hours"), pl.col("mtbo") * 1.05]).alias("effective_hours"),
            # Apply prorrata formula
            pl.when(
                # Condition: effective_hours >= mtbo AND effective_hours <= mtbo*1.05
                (pl.min_horizontal([pl.col("proj_component_hours"), pl.col("mtbo") * 1.05]) >= pl.col("mtbo"))
                & (pl.min_horizontal([pl.col("proj_component_hours"), pl.col("mtbo") * 1.05]) <= pl.col("mtbo") * 1.05)
            )
            .then(
                # Case 1: Hours in normal range (100% to 105%)
                pl.col("gfa_overhaul_rate")
                * (
                    1
                    + (
                        (pl.min_horizontal([pl.col("proj_component_hours"), pl.col("mtbo") * 1.05]) - pl.col("mtbo"))
                        / pl.col("mtbo")
                    )
                )
            )
            .otherwise(
                # Case 2: Hours outside normal range
                pl.col("gfa_overhaul_rate")
                * (pl.min_horizontal([pl.col("proj_component_hours"), pl.col("mtbo") * 1.05]) / pl.col("mtbo"))
            )
            .fill_null(0.0)
            .round(0)
            .alias("prorrata_sale"),  # Final column name
        ]
    ).drop(
        ["effective_hours"]
    )  # Clean up intermediate column

    return result_df
