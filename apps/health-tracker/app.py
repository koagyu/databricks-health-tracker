import os
import io
import time
import datetime
import base64
import csv
import urllib.request
import shutil
import secrets

import dash
from dash import dcc, html, Input, Output, State, callback, no_update, ALL, ctx
from fpdf import FPDF
import plotly.graph_objects as go
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CATALOG = os.environ.get("DATABRICKS_CATALOG", "dbws_weu_dev")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "health_tracker")

TABLE_HEALTH = f"`{CATALOG}`.`{SCHEMA}`.health_metrics"
TABLE_NUTRITION = f"`{CATALOG}`.`{SCHEMA}`.nutrition_reference"
TABLE_CONSUMPTION = f"`{CATALOG}`.`{SCHEMA}`.consumption_log"

VIEW_HEALTH_REPORT = f"`{CATALOG}`.`{SCHEMA}`.health_metrics_report"
VIEW_CONSUMPTION_REPORT = f"`{CATALOG}`.`{SCHEMA}`.consumption_report"
VIEW_CONSUMPTION_AVG = f"`{CATALOG}`.`{SCHEMA}`.consumption_avg"
VIEW_CONSUMPTION_MAX = f"`{CATALOG}`.`{SCHEMA}`.consumption_max"
VIEW_CONSUMPTION_MIN = f"`{CATALOG}`.`{SCHEMA}`.consumption_min"

# ---------------------------------------------------------------------------
# Unicode font setup (Hungarian characters require a TTF font)
# ---------------------------------------------------------------------------
_FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts")
_FONT_URLS = {
    "DejaVuSans.ttf": "https://cdn.jsdelivr.net/fontsource/fonts/dejavu-sans@latest/latin-400-normal.ttf",
    "DejaVuSans-Bold.ttf": "https://cdn.jsdelivr.net/fontsource/fonts/dejavu-sans@latest/latin-700-normal.ttf",
    "DejaVuSans-Oblique.ttf": "https://cdn.jsdelivr.net/fontsource/fonts/dejavu-sans@latest/latin-400-italic.ttf",
}
_SYSTEM_FONT_DIRS = [
    "/usr/share/fonts/truetype/dejavu",
    "/usr/share/fonts/dejavu-sans-fonts",
    "/usr/share/fonts/dejavu",
    "/usr/share/fonts/TTF",
]


def _find_system_font(name):
    for d in _SYSTEM_FONT_DIRS:
        path = os.path.join(d, name)
        if os.path.exists(path):
            return path
    return None


def _ensure_fonts():
    os.makedirs(_FONT_DIR, exist_ok=True)
    for name, url in _FONT_URLS.items():
        dest = os.path.join(_FONT_DIR, name)
        if os.path.exists(dest):
            continue
        # Try system fonts first
        sys_path = _find_system_font(name)
        if sys_path:
            shutil.copy2(sys_path, dest)
            continue
        # Download from fontsource CDN (verified to support Hungarian chars)
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0")
        with urllib.request.urlopen(req, timeout=30) as resp:
            with open(dest, "wb") as f:
                f.write(resp.read())


_ensure_fonts()

def _register_fonts(pdf):
    """Register DejaVu Sans (Unicode) font family on a FPDF instance."""
    pdf.add_font("DejaVu", "", os.path.join(_FONT_DIR, "DejaVuSans.ttf"))
    pdf.add_font("DejaVu", "B", os.path.join(_FONT_DIR, "DejaVuSans-Bold.ttf"))
    pdf.add_font("DejaVu", "I", os.path.join(_FONT_DIR, "DejaVuSans-Oblique.ttf"))

# Databricks SDK auto-authenticates in the Apps environment
w = WorkspaceClient()

# Use the dedicated health-tracker warehouse (configured via env var)
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")
if not WAREHOUSE_ID:
    raise RuntimeError("DATABRICKS_WAREHOUSE_ID environment variable not set. "
                       "Please configure it in app.yaml.")


def execute(query, fetch=False):
    """Execute SQL via Databricks SDK Statement Execution API."""
    resp = w.statement_execution.execute_statement(
        statement=query,
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG,
        schema=SCHEMA,
    )
    # Wait for completion
    while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(0.5)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if resp.status and resp.status.state == StatementState.FAILED:
        raise RuntimeError(f"SQL error: {resp.status.error}")

    if fetch and resp.result and resp.result.data_array:
        cols = [c.name for c in resp.manifest.schema.columns]
        rows = resp.result.data_array
        return pd.DataFrame(rows, columns=cols)
    elif fetch:
        return pd.DataFrame()
    return None


# ---------------------------------------------------------------------------
# Cache for nutrition items (avoids DB query on every keystroke)
# ---------------------------------------------------------------------------
_nutrition_cache = {"items": [], "ts": 0}
CACHE_TTL = 30  # seconds


def get_nutrition_items():
    """Return cached list of nutrition item names, refresh every CACHE_TTL seconds."""
    now = time.time()
    if now - _nutrition_cache["ts"] > CACHE_TTL:
        df = execute(f"SELECT DISTINCT name FROM {TABLE_NUTRITION} WHERE name NOT LIKE 'adhoc-%' ORDER BY name", fetch=True)
        _nutrition_cache["items"] = df["name"].tolist() if not df.empty else []
        _nutrition_cache["ts"] = now
    return _nutrition_cache["items"]


def invalidate_cache():
    """Force cache refresh on next call."""
    _nutrition_cache["ts"] = 0


# ---------------------------------------------------------------------------
# Initialize tables
# ---------------------------------------------------------------------------
def init_tables():
    execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_HEALTH} (
            date DATE,
            weight_kg DOUBLE,
            bpm_morning INT,
            bpm_evening INT
        ) USING DELTA
    """)
    execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NUTRITION} (
            name STRING,
            portion_type STRING COMMENT 'piece, 100g, or 100ml',
            sugar_g DOUBLE,
            fiber_g DOUBLE,
            protein_g DOUBLE,
            fat_g DOUBLE
        ) USING DELTA
    """)
    execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_CONSUMPTION} (
            date DATE,
            time TIMESTAMP,
            name STRING,
            pieces DOUBLE,
            grams DOUBLE,
            ml DOUBLE
        ) USING DELTA
    """)


# Tables are pre-created in dbws_weu_dev.health_tracker
# init_tables()

# ---------------------------------------------------------------------------
# Translations
# ---------------------------------------------------------------------------
T = {
    "en": {
        "app_title": "Health Tracker",
        "tab_health": "Health Metrics",
        "tab_nutrition": "Nutrition Reference",
        "tab_consumption": "Consumption",
        "log_metrics": "Log Today's Metrics",
        "date": "Date",
        "weight_kg": "Weight (kg)",
        "morning": "Morning",
        "evening": "Evening",
        "save": "Save",
        "saved": "Saved!",
        "select_date": "Please select a date.",
        "last_30": "Last 30 Days",
        "nutr_table": "Nutrition Reference Table",
        "add_new": "Add New Item",
        "name": "Name",
        "measurement": "Measurement",
        "portion": "Portion",
        "portion_size": "Portion size (g/ml)",
        "nutr_per_100": "Nutritional facts per 100g/ml:",
        "energy": "Energy (kcal)",
        "carbs": "Carbs",
        "sugar": "Sugar",
        "fat": "Fat",
        "sat_fat": "Saturated fat",
        "unsat_fat": "Unsaturated fat",
        "protein": "Protein",
        "fiber": "Fiber",
        "salt": "Salt",
        "added": "Added",
        "name_required": "Name is required.",
        "portion_size_required": "Portion size is required when portion is checked.",
        "log_consumption": "Log Consumption",
        "food_drink": "Food/Drink name",
        "portions": "Portions",
        "grams": "Grams",
        "ml": "ml",
        "consumption_log": "Consumption Log",
        "no_entries": "No entries for this date.",
        "no_entries_yet": "No entries yet.",
        "total": "TOTAL",
        "no_match": "No matching items found.",
        "not_found": "Item not found in nutrition reference. Please register it first.",
        "qty_required": "Enter a quantity (portions, grams, or ml).",
        "logged": "Logged",
        "tab_reports": "Reports",
        "tab_entry": "Data Entry",
        "health_summary": "Health Metrics Summary",
        "show_table": "Show table",
        "hide_table": "Hide table",
        "nutr_count": "{n} items registered",
        "min": "MIN",
        "max": "MAX",
        "avg": "AVG",
        "import_csv": "Import CSV",
        "import_drag": "Drag & drop or click to upload CSV",
        "import_success": "{n} rows imported successfully.",
        "import_skipped": "({s} date-only rows skipped)",
        "import_error": "Import error: {e}",
        "import_no_data": "No valid data rows found in CSV.",
        "adhoc_entry": "Ad-hoc Entry"
    },
    "hu": {
        "app_title": "Eg\u00e9szs\u00e9g K\u00f6vet\u0151",
        "tab_health": "Eg\u00e9szs\u00e9g",
        "tab_nutrition": "T\u00e1p\u00e9rt\u00e9k Referencia",
        "tab_consumption": "Fogyaszt\u00e1s",
        "log_metrics": "Napi m\u00e9r\u00e9sek",
        "date": "D\u00e1tum",
        "weight_kg": "T\u00f6meg (kg)",
        "morning": "Reggel",
        "evening": "Este",
        "save": "Ment\u00e9s",
        "saved": "Mentve!",
        "select_date": "V\u00e1lassz d\u00e1tumot.",
        "last_30": "Utols\u00f3 30 nap",
        "nutr_table": "T\u00e1p\u00e9rt\u00e9k Referencia T\u00e1bla",
        "add_new": "\u00daj elem hozz\u00e1ad\u00e1sa",
        "name": "N\u00e9v",
        "measurement": "M\u00e9rt\u00e9kegys\u00e9g",
        "portion": "Adag",
        "portion_size": "Adagm\u00e9ret (g/ml)",
        "nutr_per_100": "T\u00e1p\u00e9rt\u00e9k 100g/ml-re:",
        "energy": "Energia (kcal)",
        "carbs": "Sz\u00e9nhidr\u00e1t",
        "sugar": "Cukor",
        "fat": "Zs\u00edr",
        "sat_fat": "Tel\u00edtett zs\u00edrsavak",
        "unsat_fat": "Tel\u00edtetlen zs\u00edrsavak",
        "protein": "Feh\u00e9rje",
        "fiber": "Rost",
        "salt": "S\u00f3",
        "added": "Hozz\u00e1adva",
        "name_required": "A n\u00e9v megad\u00e1sa k\u00f6telez\u0151.",
        "portion_size_required": "Adagm\u00e9ret megad\u00e1sa k\u00f6telez\u0151.",
        "log_consumption": "Fogyaszt\u00e1s r\u00f6gz\u00edt\u00e9se",
        "food_drink": "\u00c9tel/Ital n\u00e9v",
        "portions": "Adag",
        "grams": "Gramm",
        "ml": "ml",
        "consumption_log": "Fogyaszt\u00e1si napl\u00f3",
        "no_entries": "Nincs bejegyz\u00e9s erre a napra.",
        "no_entries_yet": "M\u00e9g nincs bejegyz\u00e9s.",
        "total": "\u00d6SSZESEN",
        "no_match": "Nincs tal\u00e1lat.",
        "not_found": "Nem tal\u00e1lhat\u00f3 a t\u00e1p\u00e9rt\u00e9k referenci\u00e1ban. Regisztr\u00e1ld el\u0151sz\u00f6r!",
        "qty_required": "Adj meg mennyis\u00e9get (adag, gramm, vagy ml).",
        "logged": "R\u00f6gz\u00edtve",
        "tab_reports": "Eredm\u00e9nyek",
        "health_summary": "Eg\u00e9szs\u00e9g\u00fcgyi \u00f6sszefoglal\u00f3",
        "tab_entry": "Adatbevitel",
        "show_table": "T\u00e1bl\u00e1zat mutat\u00e1sa",
        "hide_table": "T\u00e1bl\u00e1zat elrejt\u00e9se",
        "nutr_count": "{n} elem regisztr\u00e1lva",
        "min": "MIN",
        "max": "MAX",
        "avg": "\u00c1TL",
        "adhoc_entry": "Egy\u00e9b bevitel"
    },
}

# Column display name mapping (SQL column -> translation key)
COL_NAMES = {
    "name": "name",
    "portions": "portions",
    "grams": "grams",
    "ml": "ml",
    "energy_kcal": "energy",
    "carbs_g": "carbs",
    "sugar_g": "sugar",
    "fat_g": "fat",
    "sat_fat_g": "sat_fat",
    "saturated_fat_g": "sat_fat",
    "unsat_fat_g": "unsat_fat",
    "unsaturated_fat_g": "unsat_fat",
    "protein_g": "protein",
    "fiber_g": "fiber",
    "salt_g": "salt",
    "measurement": "measurement",
    "has_portion": "portion",
    "portion_size": "portion_size",
    "date": "date",
    "kcal": "energy",
    "carbs": "carbs",
    "sugar": "sugar",
    "fat": "fat",
    "saturated_fat": "sat_fat",
    "unsaturated_fat": "unsat_fat",
    "protein": "protein",
    "fiber": "fiber",
    "salt": "salt",
}


def col_header(col, lang):
    """Translate a SQL column name to a display name."""
    key = COL_NAMES.get(col)
    if key:
        return T[lang][key]
    return col


NAME_COLS = {"name"}


def cell_align(col):
    """Return text alignment for a table cell based on column type."""
    if col in NAME_COLS:
        return "left"
    return "right"


# ---------------------------------------------------------------------------
# Dash App
# ---------------------------------------------------------------------------
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "Health Tracker"

today_str = datetime.date.today().isoformat()

app.layout = html.Div([
    html.Div([
        html.H1("Health Tracker", id="app-title", style={"textAlign": "center", "display": "inline-block", "width": "100%"}),
        html.Div([
            dcc.RadioItems(
                id="lang-select",
                options=[{"label": "EN", "value": "en"}, {"label": "HU", "value": "hu"}],
                value="en",
                inline=True,
                style={"position": "absolute", "top": "15px", "right": "15px", "fontSize": "14px"},
            ),
        ]),
    ], style={"position": "relative"}),
    dcc.Tabs(id="tabs", value="tab-entry", children=[
        dcc.Tab(label="Data Entry", value="tab-entry", id="tab-label-entry"),
        dcc.Tab(label="Reports", value="tab-reports", id="tab-label-reports"),
    ]),
    html.Div(id="tab-content", style={"padding": "20px"}),
    dcc.Store(id="refresh-trigger", data=0),
    dcc.Download(id="download-pdf"),
], style={"maxWidth": "900px", "margin": "0 auto", "fontFamily": "sans-serif"})


# ---------------------------------------------------------------------------
# Tab rendering
# ---------------------------------------------------------------------------
@callback(
    Output("tab-content", "children"),
    Output("app-title", "children"),
    Output("tab-label-entry", "label"),
    Output("tab-label-reports", "label"),
    Input("tabs", "value"),
    Input("lang-select", "value"),
)
def render_tab(tab, lang):
    lang = lang or "en"
    title = T[lang]["app_title"]
    labels = (T[lang]["tab_entry"], T[lang]["tab_reports"])
    if tab == "tab-entry":
        return render_entry_tab(lang), title, *labels
    elif tab == "tab-reports":
        return render_reports_tab(lang), title, *labels
    return html.Div(), title, *labels


# ---------------------------------------------------------------------------
# DATA ENTRY TAB
# ---------------------------------------------------------------------------
def get_health_record(date_str):
    """Fetch existing record for a given date, return dict or None."""
    df = execute(f"SELECT * FROM {TABLE_HEALTH} WHERE date = '{date_str}' LIMIT 1", fetch=True)
    if df.empty:
        return None
    row = df.iloc[0]
    return {c: (None if pd.isna(row[c]) else row[c]) for c in df.columns}


def render_entry_tab(lang):
    """Single data entry tab: date picker, BPM form, consumption form+table, nutrition form."""
    items = get_nutrition_items()
    item_options = [{"label": n, "value": n} for n in items]
    nutr_count = len(items)
    rec = get_health_record(today_str)

    return html.Div([
        # --- Shared date picker ---
        html.Div([
            html.Label(T[lang]["date"], style={"fontWeight": "bold"}),
            dcc.DatePickerSingle(id="entry-date", date=today_str, display_format="YYYY-MM-DD",
                                 style={"marginLeft": "8px"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "20px"}),

        # --- Blood Pressure / Health section ---
        html.H4(T[lang]["log_metrics"]),
        html.Div([
            html.Label(T[lang]["weight_kg"]),
            dcc.Input(id="health-weight", type="number", step=0.01,
                      value=rec["weight_kg"] if rec and rec.get("weight_kg") else None,
                      style={"width": "100px", "marginLeft": "5px"}),
        ], style={"display": "flex", "alignItems": "center", "gap": "5px"}),
        html.Div([
            html.Strong(T[lang]["morning"] + ": "),
            html.Label("SYS"), dcc.Input(id="health-sys-am", type="number",
                value=rec["sys_morning"] if rec and rec.get("sys_morning") else None, style={"width": "65px"}),
            html.Label("DIA"), dcc.Input(id="health-dia-am", type="number",
                value=rec["dia_morning"] if rec and rec.get("dia_morning") else None, style={"width": "65px"}),
            html.Label("Pulse"), dcc.Input(id="health-pulse-am", type="number",
                value=rec["pulse_morning"] if rec and rec.get("pulse_morning") else None, style={"width": "65px"}),
        ], style={"display": "flex", "alignItems": "center", "gap": "5px", "marginTop": "8px"}),
        html.Div([
            html.Strong(T[lang]["evening"] + ": "),
            html.Label("SYS"), dcc.Input(id="health-sys-pm", type="number",
                value=rec["sys_evening"] if rec and rec.get("sys_evening") else None, style={"width": "65px"}),
            html.Label("DIA"), dcc.Input(id="health-dia-pm", type="number",
                value=rec["dia_evening"] if rec and rec.get("dia_evening") else None, style={"width": "65px"}),
            html.Label("Pulse"), dcc.Input(id="health-pulse-pm", type="number",
                value=rec["pulse_evening"] if rec and rec.get("pulse_evening") else None, style={"width": "65px"}),
        ], style={"display": "flex", "alignItems": "center", "gap": "5px", "marginTop": "5px"}),
        html.Button(T[lang]["save"], id="health-save", n_clicks=0, style={"marginTop": "10px"}),
        html.Div(id="health-msg", style={"marginTop": "5px", "color": "green"}),

        html.Hr(),

        # --- Consumption section ---
        html.H4(T[lang]["log_consumption"]),
        html.Div([
            dcc.Dropdown(id="cons-name", options=item_options, placeholder=T[lang]["food_drink"],
                         searchable=True, style={"width": "250px"}),
            html.Div(id="cons-suggestions", style={"fontSize": "12px", "color": "#555", "marginLeft": "5px"}),
            html.Label(T[lang]["portions"]), dcc.Input(id="cons-pieces", type="number", step=0.5, style={"width": "70px"}),
            html.Label(T[lang]["grams"]), dcc.Input(id="cons-grams", type="number", style={"width": "70px"}),
            html.Label(T[lang]["ml"]), dcc.Input(id="cons-ml", type="number", style={"width": "70px"}),
            html.Button(T[lang]["save"], id="cons-save", n_clicks=0),
        ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
        html.Div(id="cons-msg", style={"marginTop": "5px", "color": "green"}),
        html.Div(id="entry-cons-table", style={"marginTop": "10px"}),

        html.Hr(),

        # --- Nutrition Reference section ---
        html.H4(T[lang]["nutr_table"]),
        html.Div(T[lang]["nutr_count"].format(n=nutr_count),
                 style={"marginBottom": "8px", "color": "#555"}),
        html.Button(T[lang]["show_table"], id="nutr-toggle", n_clicks=0,
                    style={"fontSize": "12px", "marginBottom": "8px"}),
        html.Div(id="nutr-table-area", style={"display": "none"}),
        html.Details([
            html.Summary(T[lang]["add_new"]),
            html.Div([
                html.Div([
                    html.Label(T[lang]["name"]),
                    dcc.Input(id="nutr-name", type="text", style={"width": "180px"}),
                    html.Label(T[lang]["measurement"], style={"marginLeft": "10px"}),
                    dcc.Dropdown(id="nutr-measurement", options=[
                        {"label": "g", "value": "g"}, {"label": "ml", "value": "ml"},
                    ], value="g", style={"width": "80px", "display": "inline-block"}),
                    dcc.Checklist(id="nutr-has-portion", options=[{"label": f" {T[lang]['portion']}", "value": "yes"}],
                                  value=[], style={"marginLeft": "10px", "display": "inline-block"}),
                    html.Label(T[lang]["portion_size"], style={"marginLeft": "5px"}),
                    dcc.Input(id="nutr-portion-size", type="number", step=0.1, style={"width": "100px"}),
                ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
                html.P(T[lang]["nutr_per_100"], style={"marginTop": "10px", "fontWeight": "bold", "marginBottom": "4px"}),
                html.Div([
                    html.Div([html.Label(T[lang]["energy"]), dcc.Input(id="nutr-energy", type="number", step=0.1, style={"width": "75px"})], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
                    html.Div([html.Label(T[lang]["fat"]), dcc.Input(id="nutr-fat", type="number", step=0.1, style={"width": "65px"})], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
                    html.Div([html.Label(T[lang]["sat_fat"]), dcc.Input(id="nutr-sat-fat", type="number", step=0.1, style={"width": "65px"})], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
                    html.Div([html.Label(T[lang]["unsat_fat"]), dcc.Input(id="nutr-unsat-fat", type="number", step=0.1, style={"width": "65px"})], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
                    html.Div([html.Label(T[lang]["carbs"]), dcc.Input(id="nutr-carbs", type="number", step=0.1, style={"width": "65px"})], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
                    html.Div([html.Label(T[lang]["sugar"]), dcc.Input(id="nutr-sugar", type="number", step=0.1, style={"width": "65px"})], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
                    html.Div([html.Label(T[lang]["fiber"]), dcc.Input(id="nutr-fiber", type="number", step=0.1, style={"width": "65px"})], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
                    html.Div([html.Label(T[lang]["protein"]), dcc.Input(id="nutr-protein", type="number", step=0.1, style={"width": "65px"})], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"}),
                    html.Div([html.Label(T[lang]["salt"]), dcc.Input(id="nutr-salt", type="number", step=0.01, style={"width": "65px"})], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap", "gap": "5px"})
                ], style={"marginTop": "5px"}),
                html.Button(T[lang]["save"], id="nutr-save", n_clicks=0, style={"marginTop": "10px"}),
                html.Button(T[lang]["adhoc_entry"], id="adhoc-save", n_clicks=0, style={"marginTop": "10px"}),
                html.Div(id="nutr-msg", style={"marginTop": "5px", "color": "green"}),
            ], style={"marginTop": "8px"}),
        ], open=False, style={"marginTop": "10px"}),

        html.Hr(),

        # --- CSV Import section ---
        html.H4(T[lang]["import_csv"]),
        dcc.Upload(
            id="csv-upload",
            children=html.Div([
                T[lang]["import_drag"],
            ], style={"padding": "20px", "border": "2px dashed #ccc", "borderRadius": "5px",
                     "textAlign": "center", "cursor": "pointer", "color": "#555"}),
            multiple=False,
            accept=".csv",
        ),
        html.Div(id="csv-import-msg", style={"marginTop": "8px"}),
    ])


@callback(
    Output("health-weight", "value"),
    Output("health-sys-am", "value"),
    Output("health-dia-am", "value"),
    Output("health-pulse-am", "value"),
    Output("health-sys-pm", "value"),
    Output("health-dia-pm", "value"),
    Output("health-pulse-pm", "value"),
    Input("entry-date", "date"),
    prevent_initial_call=True,
)
def load_health_for_date(date_val):
    """When user picks a different date, load existing record into inputs."""
    if not date_val:
        return None, None, None, None, None, None, None
    rec = get_health_record(date_val)
    if not rec:
        return None, None, None, None, None, None, None
    return (
        rec["weight_kg"],
        rec["sys_morning"], rec["dia_morning"], rec["pulse_morning"],
        rec["sys_evening"], rec["dia_evening"], rec["pulse_evening"],
    )


@callback(
    Output("health-msg", "children"),
    Output("refresh-trigger", "data", allow_duplicate=True),
    Input("health-save", "n_clicks"),
    State("entry-date", "date"),
    State("health-weight", "value"),
    State("health-sys-am", "value"),
    State("health-dia-am", "value"),
    State("health-pulse-am", "value"),
    State("health-sys-pm", "value"),
    State("health-dia-pm", "value"),
    State("health-pulse-pm", "value"),
    State("refresh-trigger", "data"),
    prevent_initial_call=True,
)
def save_health(n, date_val, weight, sys_am, dia_am, pulse_am, sys_pm, dia_pm, pulse_pm, refresh):
    if not n:
        return no_update, no_update
    if not date_val:
        return "Please select a date. / V\u00e1lassz d\u00e1tumot.", no_update
    # Upsert logic: MERGE to keep one row per day
    def v(x):
        return str(int(x)) if x else 'NULL'
    def vf(x):
        return str(float(x)) if x else 'NULL'
    merge_sql = f"""
        MERGE INTO {TABLE_HEALTH} t
        USING (SELECT '{date_val}' AS date) s ON t.date = s.date
        WHEN MATCHED THEN UPDATE SET
            weight_kg = COALESCE({vf(weight)}, t.weight_kg),
            sys_morning = COALESCE({v(sys_am)}, t.sys_morning),
            dia_morning = COALESCE({v(dia_am)}, t.dia_morning),
            pulse_morning = COALESCE({v(pulse_am)}, t.pulse_morning),
            sys_evening = COALESCE({v(sys_pm)}, t.sys_evening),
            dia_evening = COALESCE({v(dia_pm)}, t.dia_evening),
            pulse_evening = COALESCE({v(pulse_pm)}, t.pulse_evening)
        WHEN NOT MATCHED THEN INSERT (date, weight_kg, sys_morning, dia_morning, pulse_morning, sys_evening, dia_evening, pulse_evening)
            VALUES ('{date_val}', {vf(weight)}, {v(sys_am)}, {v(dia_am)}, {v(pulse_am)}, {v(sys_pm)}, {v(dia_pm)}, {v(pulse_pm)})
    """
    execute(merge_sql)
    return "\u2714", (refresh or 0) + 1

@callback(
    Output("nutr-msg", "children"),
    Output("refresh-trigger", "data", allow_duplicate=True),
    Input("nutr-save", "n_clicks"),
    State("nutr-name", "value"),
    State("nutr-measurement", "value"),
    State("nutr-has-portion", "value"),
    State("nutr-portion-size", "value"),
    State("nutr-energy", "value"),
    State("nutr-carbs", "value"),
    State("nutr-sugar", "value"),
    State("nutr-fat", "value"),
    State("nutr-sat-fat", "value"),
    State("nutr-unsat-fat", "value"),
    State("nutr-protein", "value"),
    State("nutr-fiber", "value"),
    State("nutr-salt", "value"),
    State("refresh-trigger", "data"),
    prevent_initial_call=True,
)
def save_nutrition(n, name, measurement, has_portion_list, portion_size,
                   energy, carbs, sugar, fat, sat_fat, unsat_fat, protein, fiber, salt, refresh):
    if not n:
        return no_update, no_update
    if not name:
        return "Name is required.", no_update
    has_portion = "yes" in (has_portion_list or [])
    if has_portion and not portion_size:
        return "Portion size is required when portion is checked.", no_update
    def nv(x):
        return float(x) if x else 0
    execute(f"""
        INSERT INTO {TABLE_NUTRITION}
        (name, measurement, has_portion, portion_size, energy_kcal, carbs_g, sugar_g, fat_g, saturated_fat_g, unsaturated_fat_g, protein_g, fiber_g, salt_g)
        VALUES ('{name}', '{measurement}', {str(has_portion).lower()}, {portion_size if portion_size else 'NULL'},
                {nv(energy)}, {nv(carbs)}, {nv(sugar)}, {nv(fat)}, {nv(sat_fat)}, {nv(unsat_fat)}, {nv(protein)}, {nv(fiber)}, {nv(salt)})
    """)
    return f"\u2714 {name}", (refresh or 0) + 1

@callback(
    Output("nutr-msg", "children", allow_duplicate=True),
    Output("refresh-trigger", "data", allow_duplicate=True),
    Input("adhoc-save", "n_clicks"),
    State("entry-date", "date"),
    State("nutr-measurement", "value"),
    State("nutr-portion-size", "value"),
    State("nutr-energy", "value"),
    State("nutr-carbs", "value"),
    State("nutr-sugar", "value"),
    State("nutr-fat", "value"),
    State("nutr-sat-fat", "value"),
    State("nutr-unsat-fat", "value"),
    State("nutr-protein", "value"),
    State("nutr-fiber", "value"),
    State("nutr-salt", "value"),
    State("refresh-trigger", "data"),
    prevent_initial_call=True,
)
def save_adhoc(n, date_val, measurement, portion_size, energy, carbs, sugar, fat, sat_fat, unsat_fat, protein, fiber, salt, refresh):
    if not n:
        return no_update, no_update
    if not any([measurement, portion_size]):
        return "Enter portion type and size.", no_update

    grams_val = portion_size if measurement == "g" else 0
    ml_val = portion_size if measurement == "ml" else 0
    name = "adhoc-" + secrets.token_hex(6)

    def nv(x):
        return float(x) if x else 0
    
    # Add adhoc entry to nutrition table
    execute(f"""
        INSERT INTO {TABLE_NUTRITION}
        (name, measurement, has_portion, portion_size, energy_kcal, carbs_g, sugar_g, fat_g, saturated_fat_g, unsaturated_fat_g, protein_g, fiber_g, salt_g)
        VALUES ('{name}', '{measurement}', {'true'}, {portion_size},
                {nv(energy)}, {nv(carbs)}, {nv(sugar)}, {nv(fat)}, {nv(sat_fat)}, {nv(unsat_fat)}, {nv(protein)}, {nv(fiber)}, {nv(salt)})
    """)

    # Log consumption
    now = datetime.datetime.now()
    execute(f"""
        INSERT INTO {TABLE_CONSUMPTION} (date, time, name, pieces, grams, ml)
        VALUES ('{date_val}', '{now.isoformat()}', '{name}', 1, {grams_val}, {ml_val})
    """)
    msg = f"Logged '{name}'!"
    if grams_val and portions_val and not grams:
        msg += f" ({portions_val} portions = {grams_val}g)"
    elif ml_val and portions_val and not ml:
        msg += f" ({portions_val} portions = {ml_val}ml)"
    return msg, (refresh or 0) + 1




# ---------------------------------------------------------------------------
# Entry tab: consumption table + nutrition table toggle
# ---------------------------------------------------------------------------
@callback(
    Output("entry-cons-table", "children"),
    Input("entry-date", "date"),
    Input("refresh-trigger", "data"),
    Input("lang-select", "value"),
)
def load_entry_cons_table(date_val, _, lang):
    """Display consumption table for the selected date on the entry tab."""
    lang = lang or "en"
    if not date_val:
        return html.Div()
    table = _build_day_table(date_val, lang, show_delete=True)
    if table is None:
        return html.Div(T[lang]["no_entries"], style={"color": "#888", "fontSize": "13px"})
    return table


@callback(
    Output("nutr-table-area", "children"),
    Output("nutr-table-area", "style"),
    Output("nutr-toggle", "children"),
    Input("nutr-toggle", "n_clicks"),
    Input("refresh-trigger", "data"),
    State("lang-select", "value"),
    prevent_initial_call=True,
)
def toggle_nutr_table(n_clicks, _, lang):
    """Toggle nutrition reference table visibility."""
    lang = lang or "en"
    show = (n_clicks or 0) % 2 == 1
    if not show:
        return html.Div(), {"display": "none"}, T[lang]["show_table"]
    df = execute(f"SELECT name, measurement, has_portion, portion_size, energy_kcal, carbs_g, sugar_g, fat_g, saturated_fat_g, unsaturated_fat_g, protein_g, fiber_g, salt_g FROM {TABLE_NUTRITION} ORDER BY name", fetch=True)
    if df.empty:
        return html.Div(T[lang]["no_entries_yet"]), {"display": "block"}, T[lang]["hide_table"]
    header_cells = [html.Th(col_header(c, lang), style={"textAlign": "center"}) for c in df.columns]
    header_cells.append(html.Th("", style={"width": "40px"}))
    body_rows = []
    for _, row in df.iterrows():
        cells = [html.Td(row[c], style={"textAlign": cell_align(c)}) for c in df.columns]
        cells.append(html.Td(
            html.Button("\u2716", id={"type": "nutr-del", "index": row["name"]},
                        n_clicks=0, style={"color": "red", "border": "none", "cursor": "pointer", "background": "none", "fontSize": "14px"}),
            style={"textAlign": "center"}
        ))
        body_rows.append(html.Tr(cells))
    table = html.Table([
        html.Thead(html.Tr(header_cells)),
        html.Tbody(body_rows)
    ], style={"width": "100%", "borderCollapse": "collapse", "fontSize": "13px"})
    return html.Div(table, style={"overflowX": "auto"}), {"display": "block"}, T[lang]["hide_table"]


@callback(
    Output("refresh-trigger", "data", allow_duplicate=True),
    Input({"type": "nutr-del", "index": ALL}, "n_clicks"),
    State("refresh-trigger", "data"),
    prevent_initial_call=True,
)
def delete_nutrition_item(n_clicks_list, refresh):
    """Delete a nutrition reference item when its row delete button is clicked."""
    if not any(n_clicks_list):
        return no_update
    triggered = ctx.triggered_id
    if not triggered or not isinstance(triggered, dict):
        return no_update
    item_name = triggered["index"]
    execute(f"DELETE FROM {TABLE_NUTRITION} WHERE LOWER(name) = LOWER('{item_name}')")
    invalidate_cache()
    return (refresh or 0) + 1


@callback(
    Output("refresh-trigger", "data", allow_duplicate=True),
    Input({"type": "cons-del", "index": ALL}, "n_clicks"),
    State("refresh-trigger", "data"),
    prevent_initial_call=True,
)
def delete_consumption_entry(n_clicks_list, refresh):
    """Delete a consumption log entry by its timestamp."""
    if not any(n_clicks_list):
        return no_update
    triggered = ctx.triggered_id
    if not triggered or not isinstance(triggered, dict):
        return no_update
    entry_time = triggered["index"]
    # API returns millisecond precision (2026-08-07T09:22:32.257Z) but
    # stored values have microseconds (2026-08-07 09:22:32.257087).
    # Normalize format and use prefix match to bridge the precision gap.
    normalized = entry_time.replace('T', ' ').rstrip('Z')
    execute(f"DELETE FROM {TABLE_CONSUMPTION} WHERE CAST(time AS STRING) LIKE '{normalized}%'")
    return (refresh or 0) + 1


# ---------------------------------------------------------------------------
# CSV Import callback
# ---------------------------------------------------------------------------
@callback(
    Output("csv-import-msg", "children"),
    Output("refresh-trigger", "data", allow_duplicate=True),
    Input("csv-upload", "contents"),
    State("csv-upload", "filename"),
    State("lang-select", "value"),
    State("refresh-trigger", "data"),
    prevent_initial_call=True,
)
def import_csv(contents, filename, lang, refresh):
    """Parse uploaded CSV and upsert rows into health_metrics (batched)."""
    lang = lang or "en"
    if contents is None:
        return no_update, no_update

    try:
        # Decode base64 content
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string).decode("utf-8")
        reader = csv.reader(io.StringIO(decoded))

        rows_skipped = 0
        value_rows = []

        def sql_val(v, is_float=False):
            if v is None:
                return "NULL"
            v = v.replace(",", ".")  # handle comma decimal separator
            return str(float(v)) if is_float else str(int(float(v)))

        for i, row in enumerate(reader):
            # Skip header row
            if i == 0:
                continue
            # Need at least one field
            if not row or not row[0].strip():
                continue

            date_val = row[0].strip()

            # Check if row has any data beyond the date
            data_fields = [f.strip() for f in row[1:6]]
            if not any(data_fields):
                rows_skipped += 1
                continue

            # Parse fields (empty string -> NULL)
            weight = data_fields[0] if len(data_fields) > 0 and data_fields[0] else None
            sys_am = data_fields[1] if len(data_fields) > 1 and data_fields[1] else None
            dia_am = data_fields[2] if len(data_fields) > 2 and data_fields[2] else None
            sys_pm = data_fields[3] if len(data_fields) > 3 and data_fields[3] else None
            dia_pm = data_fields[4] if len(data_fields) > 4 and data_fields[4] else None

            value_rows.append(
                f"('{date_val}', {sql_val(weight, True)}, {sql_val(sys_am)}, {sql_val(dia_am)}, {sql_val(sys_pm)}, {sql_val(dia_pm)})"
            )

        if not value_rows:
            return html.Div(T[lang]["import_no_data"], style={"color": "orange"}), no_update

        # Batch MERGE: all rows in a single statement
        values_clause = ",\n".join(value_rows)
        merge_sql = f"""
            MERGE INTO {TABLE_HEALTH} t
            USING (
                SELECT * FROM (VALUES
                    {values_clause}
                ) AS src(date, weight_kg, sys_morning, dia_morning, sys_evening, dia_evening)
            ) s ON t.date = s.date
            WHEN MATCHED THEN UPDATE SET
                weight_kg = COALESCE(s.weight_kg, t.weight_kg),
                sys_morning = COALESCE(s.sys_morning, t.sys_morning),
                dia_morning = COALESCE(s.dia_morning, t.dia_morning),
                sys_evening = COALESCE(s.sys_evening, t.sys_evening),
                dia_evening = COALESCE(s.dia_evening, t.dia_evening)
            WHEN NOT MATCHED THEN INSERT (date, weight_kg, sys_morning, dia_morning, sys_evening, dia_evening)
                VALUES (s.date, s.weight_kg, s.sys_morning, s.dia_morning, s.sys_evening, s.dia_evening)
        """
        execute(merge_sql)

        rows_imported = len(value_rows)
        msg = T[lang]["import_success"].format(n=rows_imported)
        if rows_skipped > 0:
            msg += " " + T[lang]["import_skipped"].format(s=rows_skipped)
        return html.Div(msg, style={"color": "green"}), (refresh or 0) + 1

    except Exception as e:
        return html.Div(T[lang]["import_error"].format(e=str(e)), style={"color": "red"}), no_update


# ---------------------------------------------------------------------------
# REPORTS TAB
# ---------------------------------------------------------------------------
def render_reports_tab(lang):
    return html.Div([
        html.H3(T[lang]["tab_reports"]),
        html.Div([
            html.Button("⬇ PDF", id="report-pdf-btn", n_clicks=0,
                        style={"fontSize": "13px", "cursor": "pointer"}),
        ], style={"display": "flex", "alignItems": "center", "gap": "5px", "marginBottom": "10px"}),
        # Health chart
        html.Div(id="report-chart"),
        # Health metrics summary table
        html.H4(T[lang]["health_summary"]),
        html.Div(id="report-health-table"),
        html.Hr(),
        # Consumption tables
        html.H4(T[lang]["consumption_log"]),
        html.Div(id="report-cons-table"),
    ])


def _r1(val):
    """Round to 1 decimal if numeric, else pass through."""
    try:
        return round(float(val), 1)
    except (ValueError, TypeError):
        return val if val is not None else ''


def _build_health_table(lang):
    """Build the health metrics table from the report view (last year)."""
    df = execute(f"""
        SELECT date, weight_kg, sys_morning, dia_morning, pulse_morning,
               sys_evening, dia_evening, pulse_evening
        FROM {VIEW_HEALTH_REPORT}
        ORDER BY date
    """, fetch=True)
    if df.empty:
        return html.Div(T[lang]["no_entries"], style={"color": "#888"})
    header_map = {
        "date": T[lang]["date"],
        "weight_kg": T[lang]["weight_kg"],
        "sys_morning": f"SYS {T[lang]['morning']}",
        "dia_morning": f"DIA {T[lang]['morning']}",
        "pulse_morning": f"Pulse {T[lang]['morning']}",
        "sys_evening": f"SYS {T[lang]['evening']}",
        "dia_evening": f"DIA {T[lang]['evening']}",
        "pulse_evening": f"Pulse {T[lang]['evening']}",
    }
    # Compute min, max, avg for numeric columns
    numeric_cols = [c for c in df.columns if c != "date"]
    summary_rows = []
    for label_key in ("min", "max", "avg"):
        cells = []
        for c in df.columns:
            if c == "date":
                cells.append(html.Td(T[lang][label_key], style={"textAlign": "left", "fontWeight": "bold"}))
            elif c in numeric_cols:
                vals = pd.to_numeric(df[c], errors="coerce").dropna()
                if vals.empty:
                    cells.append(html.Td("", style={"textAlign": "right"}))
                else:
                    if label_key == "min":
                        v = vals.min()
                    elif label_key == "max":
                        v = vals.max()
                    else:
                        v = vals.mean()
                    cells.append(html.Td(round(v, 1), style={"textAlign": "right", "fontWeight": "bold"}))
            else:
                cells.append(html.Td("", style={"textAlign": "right"}))
        summary_rows.append(html.Tr(cells, style={"borderTop": "2px solid #333" if label_key == "min" else "none"}))

    body_rows = [
        html.Tr([
            html.Td(row[c] if c == "date" else _r1(row[c]), style={"textAlign": "left" if c == "date" else "right"})
            for c in df.columns
        ]) for _, row in df.iterrows()
    ]
    body_rows.extend(summary_rows)

    return html.Div([
        html.Table([
            html.Thead(html.Tr([html.Th(header_map.get(c, c), style={"textAlign": "center"}) for c in df.columns])),
            html.Tbody(body_rows)
        ], style={"width": "100%", "borderCollapse": "collapse", "fontSize": "13px"})
    ], style={"overflowX": "auto"})


def _build_day_table(date_str, lang, show_delete=False):
    """Build a consumption summary table for a single date. Returns html component or None."""
    df = execute(f"""
        SELECT c.time AS _entry_time, c.name, c.pieces AS portions, c.grams, c.ml,
            ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.energy_kcal, 1) AS energy_kcal,
            ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.carbs_g, 1) AS carbs_g,
            ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.sugar_g, 1) AS sugar_g,
            ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.fat_g, 1) AS fat_g,
            ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.saturated_fat_g, 1) AS sat_fat_g,
            ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.unsaturated_fat_g, 1) AS unsat_fat_g,
            ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.protein_g, 1) AS protein_g,
            ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.fiber_g, 1) AS fiber_g,
            ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.salt_g, 2) AS salt_g
        FROM {TABLE_CONSUMPTION} c
        LEFT JOIN {TABLE_NUTRITION} n ON LOWER(c.name) = LOWER(n.name)
        WHERE c.date = '{date_str}'
        ORDER BY c.time DESC
    """, fetch=True)
    if df.empty:
        return None
    # Separate the entry_time identifier from display columns
    display_cols = [c for c in df.columns if c != "_entry_time"]
    # Build totals row
    numeric_cols = ["portions", "grams", "ml", "energy_kcal", "carbs_g", "sugar_g",
                    "fat_g", "sat_fat_g", "unsat_fat_g", "protein_g", "fiber_g", "salt_g"]
    totals = {}
    for c in display_cols:
        if c in numeric_cols:
            try:
                totals[c] = round(sum(float(v) for v in df[c] if v is not None and str(v) != ''), 1)
            except (ValueError, TypeError):
                totals[c] = ""
        elif c == "name":
            totals[c] = T[lang]["total"]
        else:
            totals[c] = ""
    total_cells = [html.Td(totals.get(c, ""), style={"fontWeight": "bold", "textAlign": cell_align(c)}) for c in display_cols]
    if show_delete:
        total_cells.append(html.Td(""))
    total_row = html.Tr(total_cells, style={"fontWeight": "bold", "borderTop": "2px solid #333"})
    # Build header
    header_cells = [html.Th(col_header(c, lang), style={"textAlign": "center"}) for c in display_cols]
    if show_delete:
        header_cells.append(html.Th("", style={"width": "40px"}))
    # Build body rows
    body_rows = []
    for _, row in df.iterrows():
        cells = [html.Td(row[c], style={"textAlign": cell_align(c)}) for c in display_cols]
        if show_delete:
            entry_time = row["_entry_time"] or ""
            cells.append(html.Td(
                html.Button("\u2716", id={"type": "cons-del", "index": str(entry_time)},
                            n_clicks=0, style={"color": "red", "border": "none", "cursor": "pointer", "background": "none", "fontSize": "14px"}),
                style={"textAlign": "center"}
            ))
        body_rows.append(html.Tr(cells))
    body_rows.append(total_row)
    return html.Div([
        html.Table([
            html.Thead(html.Tr(header_cells)),
            html.Tbody(body_rows)
        ], style={"width": "100%", "borderCollapse": "collapse", "fontSize": "13px"})
    ], style={"overflowX": "auto"})


@callback(
    Output("report-health-table", "children"),
    Input("refresh-trigger", "data"),
    Input("lang-select", "value"),
)
def load_health_report(_, lang):
    """Load health metrics table (last year from view)."""
    lang = lang or "en"
    return _build_health_table(lang)


@callback(
    Output("report-cons-table", "children"),
    Input("refresh-trigger", "data"),
    Input("lang-select", "value"),
)
def load_consumption_report(_, lang):
    """Load daily consumption summary from the consumption_report view (last year)."""
    lang = lang or "en"

    df = execute(f"""
        SELECT date, kcal, carbs, sugar, fat, saturated_fat, unsaturated_fat,
               protein, fiber, salt
        FROM {VIEW_CONSUMPTION_REPORT}
        ORDER BY date
    """, fetch=True)
    if df.empty:
        return html.Div(T[lang]["no_entries"], style={"color": "#888"})

    display_cols = list(df.columns)
    numeric_cols = [c for c in display_cols if c != "date"]

    # Summary rows from pre-aggregated views
    summary_rows = []
    for label_key, view in [("min", VIEW_CONSUMPTION_MIN), ("max", VIEW_CONSUMPTION_MAX), ("avg", VIEW_CONSUMPTION_AVG)]:
        sdf = execute(f"SELECT * FROM {view}", fetch=True)
        cells = []
        for c in display_cols:
            if c == "date":
                cells.append(html.Td(T[lang][label_key], style={"textAlign": "left", "fontWeight": "bold"}))
            elif c in numeric_cols and not sdf.empty and c in sdf.columns:
                val = sdf.iloc[0][c]
                try:
                    cells.append(html.Td(round(float(val), 1), style={"textAlign": "right", "fontWeight": "bold"}))
                except (ValueError, TypeError):
                    cells.append(html.Td("", style={"textAlign": "right"}))
            else:
                cells.append(html.Td("", style={"textAlign": "right"}))
        summary_rows.append(html.Tr(cells, style={"borderTop": "2px solid #333" if label_key == "min" else "none"}))

    header_cells = [html.Th(col_header(c, lang), style={"textAlign": "center"}) for c in display_cols]
    body_rows = [
        html.Tr([
            html.Td(row[c] if c == "date" else _r1(row[c]), style={"textAlign": "left" if c == "date" else "right"})
            for c in display_cols
        ]) for _, row in df.iterrows()
    ]
    body_rows.extend(summary_rows)

    return html.Div([
        html.Table([
            html.Thead(html.Tr(header_cells)),
            html.Tbody(body_rows)
        ], style={"width": "100%", "borderCollapse": "collapse", "fontSize": "13px"})
    ], style={"overflowX": "auto"})


@callback(
    Output("cons-name", "options"),
    Output("cons-suggestions", "children"),
    Input("cons-name", "search_value"),
    prevent_initial_call=True,
)
def filter_items(search):
    """Filter from cached items. No DB hit on keystroke."""
    items = get_nutrition_items()
    if not search:
        return [{"label": n, "value": n} for n in items], ""
    matches = [n for n in items if n.lower().startswith(search.lower())][:10]
    if not matches:
        return [], html.Span("\u2717", style={"color": "orange"})
    return [{"label": n, "value": n} for n in matches], ""


@callback(
    Output("cons-msg", "children"),
    Output("refresh-trigger", "data", allow_duplicate=True),
    Input("cons-save", "n_clicks"),
    State("entry-date", "date"),
    State("cons-name", "value"),
    State("cons-pieces", "value"),
    State("cons-grams", "value"),
    State("cons-ml", "value"),
    State("refresh-trigger", "data"),
    prevent_initial_call=True,
)
def save_consumption(n, date_val, name, portions, grams, ml, refresh):
    if not n:
        return no_update, no_update
    if not name:
        return "Name is required.", no_update
    if not any([portions, grams, ml]):
        return "Enter a quantity (portions, grams, or ml).", no_update

    # Look up item in nutrition reference
    existing = execute(
        f"SELECT name, measurement, has_portion, portion_size FROM {TABLE_NUTRITION} WHERE LOWER(name) = LOWER('{name}') LIMIT 1",
        fetch=True
    )

    if existing.empty:
        return "Item not found in nutrition reference. Please register it first.", no_update

    # If only portions given, compute the equivalent g or ml from reference
    grams_val = grams or 0
    ml_val = ml or 0
    portions_val = portions or 0

    if portions_val and not grams and not ml:
        row = existing.iloc[0]
        portion_size = row["portion_size"]
        measurement = row["measurement"]
        if portion_size and str(portion_size) not in ('None', 'null', ''):
            scaled = float(portions_val) * float(portion_size)
            if measurement == "ml":
                ml_val = scaled
            else:
                grams_val = scaled

    # Log consumption
    now = datetime.datetime.now()
    execute(f"""
        INSERT INTO {TABLE_CONSUMPTION} (date, time, name, pieces, grams, ml)
        VALUES ('{date_val}', '{now.isoformat()}', '{name}', {portions_val}, {grams_val}, {ml_val})
    """)
    msg = f"Logged '{name}'!"
    if grams_val and portions_val and not grams:
        msg += f" ({portions_val} portions = {grams_val}g)"
    elif ml_val and portions_val and not ml:
        msg += f" ({portions_val} portions = {ml_val}ml)"
    return msg, (refresh or 0) + 1


# ---------------------------------------------------------------------------
# PDF Export
# ---------------------------------------------------------------------------
CONSUMPTION_QUERY = """
    SELECT c.name, c.pieces AS portions, c.grams, c.ml,
        ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.energy_kcal, 1) AS energy_kcal,
        ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.carbs_g, 1) AS carbs_g,
        ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.sugar_g, 1) AS sugar_g,
        ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.fat_g, 1) AS fat_g,
        ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.saturated_fat_g, 1) AS sat_fat_g,
        ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.unsaturated_fat_g, 1) AS unsat_fat_g,
        ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.protein_g, 1) AS protein_g,
        ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.fiber_g, 1) AS fiber_g,
        ROUND((COALESCE(NULLIF(c.grams, 0), c.ml, 0)) / 100.0 * n.salt_g, 2) AS salt_g
    FROM {table_c} c
    LEFT JOIN {table_n} n ON LOWER(c.name) = LOWER(n.name)
    WHERE c.date = '{{date_str}}'
    ORDER BY c.time DESC
"""


def _pdf_add_day_table(pdf, date_str, lang):
    """Add one day's consumption table to the PDF. Returns True if data was added."""
    query = CONSUMPTION_QUERY.format(table_c=TABLE_CONSUMPTION, table_n=TABLE_NUTRITION).replace("{date_str}", date_str)
    df = execute(query, fetch=True)
    if df.empty:
        return False

    headers = [col_header(c, lang) for c in df.columns]
    numeric_cols = ["portions", "grams", "ml", "energy_kcal", "carbs_g", "sugar_g",
                    "fat_g", "sat_fat_g", "unsat_fat_g", "protein_g", "fiber_g", "salt_g"]

    # Column widths: name gets more space, numeric cols share the rest
    name_w = 30
    num_w = (277 - name_w) / (len(df.columns) - 1)  # landscape A4 usable ~277mm
    col_widths = [name_w if c == "name" else num_w for c in df.columns]
    row_h = 6

    # Header row: each word on its own line, vertically centered
    pdf.set_font("DejaVu", "B", 7)
    line_h = row_h
    # Split multi-word headers into one word per line
    split_headers = []
    max_lines = 1
    for h in headers:
        words = h.split()
        if len(words) > 1:
            split_headers.append(words)
            max_lines = max(max_lines, len(words))
        else:
            split_headers.append([h])
    header_h = max_lines * line_h

    # Force page break if not enough space for header + at least one data row
    if pdf.get_y() + header_h + row_h > pdf.h - pdf.b_margin:
        pdf.add_page()

    # Disable auto page break while drawing the header block (uses manual positioning)
    pdf.set_auto_page_break(auto=False)
    x_start = pdf.get_x()
    y_start = pdf.get_y()
    for i, lines in enumerate(split_headers):
        x = x_start + sum(col_widths[:i])
        # Draw cell border
        pdf.rect(x, y_start, col_widths[i], header_h)
        # Vertically center the text block
        text_block_h = len(lines) * line_h
        y_offset = (header_h - text_block_h) / 2
        for j, line in enumerate(lines):
            pdf.set_xy(x, y_start + y_offset + j * line_h)
            pdf.cell(col_widths[i], line_h, line, border=0, align="C")
    pdf.set_xy(x_start, y_start + header_h)
    pdf.set_auto_page_break(auto=True, margin=15)

    # Data rows
    pdf.set_font("DejaVu", "", 7)
    for _, row in df.iterrows():
        for i, c in enumerate(df.columns):
            val = str(row[c]) if row[c] is not None and str(row[c]) != "None" else ""
            align = "L" if c == "name" else "R"
            pdf.cell(col_widths[i], row_h, val, border=1, align=align)
        pdf.ln()

    # Totals row
    pdf.set_font("DejaVu", "B", 7)
    for i, c in enumerate(df.columns):
        if c == "name":
            val = T[lang]["total"]
            align = "L"
        elif c in numeric_cols:
            try:
                val = str(round(sum(float(v) for v in df[c] if v is not None and str(v) != ''), 1))
            except (ValueError, TypeError):
                val = ""
            align = "R"
        else:
            val = ""
            align = "R"
        pdf.cell(col_widths[i], row_h, val, border=1, align=align)
    pdf.ln()
    return True


def _pdf_add_health_table(pdf, lang):
    """Add health metrics table to PDF (last year from view). Returns True if data was added."""
    df = execute(f"""
        SELECT date, weight_kg, sys_morning, dia_morning, pulse_morning,
               sys_evening, dia_evening, pulse_evening
        FROM {VIEW_HEALTH_REPORT}
        ORDER BY date
    """, fetch=True)
    if df.empty:
        return False
    header_map = {
        "date": T[lang]["date"],
        "weight_kg": T[lang]["weight_kg"],
        "sys_morning": f"SYS {T[lang]['morning']}",
        "dia_morning": f"DIA {T[lang]['morning']}",
        "pulse_morning": f"Pulse {T[lang]['morning']}",
        "sys_evening": f"SYS {T[lang]['evening']}",
        "dia_evening": f"DIA {T[lang]['evening']}",
        "pulse_evening": f"Pulse {T[lang]['evening']}",
    }
    headers = [header_map.get(c, c) for c in df.columns]
    col_w = 277 / len(df.columns)
    col_widths = [col_w] * len(df.columns)
    row_h = 6
    line_h = row_h

    # Header row with word wrapping
    pdf.set_font("DejaVu", "B", 7)
    split_headers = []
    max_lines = 1
    for h in headers:
        words = h.split()
        if len(words) > 1:
            split_headers.append(words)
            max_lines = max(max_lines, len(words))
        else:
            split_headers.append([h])
    header_h = max_lines * line_h

    # Force page break if not enough space for header + at least one data row
    if pdf.get_y() + header_h + row_h > pdf.h - pdf.b_margin:
        pdf.add_page()

    # Disable auto page break while drawing the header block (uses manual positioning)
    pdf.set_auto_page_break(auto=False)
    x_start = pdf.get_x()
    y_start = pdf.get_y()
    for i, lines in enumerate(split_headers):
        x = x_start + sum(col_widths[:i])
        pdf.rect(x, y_start, col_widths[i], header_h)
        text_block_h = len(lines) * line_h
        y_offset = (header_h - text_block_h) / 2
        for j, line in enumerate(lines):
            pdf.set_xy(x, y_start + y_offset + j * line_h)
            pdf.cell(col_widths[i], line_h, line, border=0, align="C")
    pdf.set_xy(x_start, y_start + header_h)
    pdf.set_auto_page_break(auto=True, margin=15)

    # Data rows
    pdf.set_font("DejaVu", "", 7)
    for _, row in df.iterrows():
        for i, c in enumerate(df.columns):
            val = str(row[c]) if row[c] is not None and str(row[c]) != "None" else ""
            if c != "date" and val:
                try:
                    val = str(round(float(val), 1))
                except (ValueError, TypeError):
                    pass
            align = "L" if c == "date" else "R"
            pdf.cell(col_widths[i], row_h, val, border=1, align=align)
        pdf.ln()
    return True


@callback(
    Output("download-pdf", "data"),
    Input("report-pdf-btn", "n_clicks"),
    State("lang-select", "value"),
    prevent_initial_call=True,
)
def export_pdf(n_clicks, lang):
    """Generate and download a PDF report (last year from views)."""
    if not n_clicks:
        return no_update
    lang = lang or "en"

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    _register_fonts(pdf)
    pdf.set_font("DejaVu", "B", 14)
    pdf.cell(0, 10, T[lang]["tab_reports"], ln=True, align="C")
    pdf.ln(4)

    # --- Health metrics table ---
    pdf.set_font("DejaVu", "B", 11)
    pdf.cell(0, 8, T[lang]["health_summary"], ln=True, align="L")
    pdf.ln(2)
    _pdf_add_health_table(pdf, lang)
    pdf.ln(6)

    # --- Consumption summary table ---
    pdf.set_font("DejaVu", "B", 11)
    pdf.cell(0, 8, T[lang]["consumption_log"], ln=True, align="L")
    pdf.ln(2)

    cons_df = execute(f"""
        SELECT date, kcal, carbs, sugar, fat, saturated_fat, unsaturated_fat,
               protein, fiber, salt
        FROM {VIEW_CONSUMPTION_REPORT}
        ORDER BY date
    """, fetch=True)
    if cons_df.empty:
        pdf.set_font("DejaVu", "", 10)
        pdf.cell(0, 10, T[lang]["no_entries"], ln=True, align="C")
    else:
        cons_headers = [col_header(c, lang) for c in cons_df.columns]
        date_w = 22
        num_w = (277 - date_w) / (len(cons_df.columns) - 1)
        cons_col_widths = [date_w if c == "date" else num_w for c in cons_df.columns]
        row_h = 6

        # Header
        pdf.set_font("DejaVu", "B", 7)
        split_headers = []
        max_lines = 1
        for h in cons_headers:
            words = h.split()
            if len(words) > 1:
                split_headers.append(words)
                max_lines = max(max_lines, len(words))
            else:
                split_headers.append([h])
        header_h = max_lines * row_h

        pdf.set_auto_page_break(auto=False)
        x_start = pdf.get_x()
        y_start = pdf.get_y()
        for i, lines in enumerate(split_headers):
            x = x_start + sum(cons_col_widths[:i])
            pdf.rect(x, y_start, cons_col_widths[i], header_h)
            text_block_h = len(lines) * row_h
            y_offset = (header_h - text_block_h) / 2
            for j, line in enumerate(lines):
                pdf.set_xy(x, y_start + y_offset + j * row_h)
                pdf.cell(cons_col_widths[i], row_h, line, border=0, align="C")
        pdf.set_xy(x_start, y_start + header_h)
        pdf.set_auto_page_break(auto=True, margin=15)

        # Data rows
        pdf.set_font("DejaVu", "", 7)
        for _, row in cons_df.iterrows():
            for i, c in enumerate(cons_df.columns):
                val = str(row[c]) if row[c] is not None and str(row[c]) != "None" else ""
                if c != "date":
                    try:
                        val = str(round(float(val), 1))
                    except (ValueError, TypeError):
                        pass
                align = "L" if c == "date" else "R"
                pdf.cell(cons_col_widths[i], row_h, val, border=1, align=align)
            pdf.ln()

        # Summary rows (min / max / avg)
        pdf.set_font("DejaVu", "B", 7)
        for label_key, view in [("min", VIEW_CONSUMPTION_MIN), ("max", VIEW_CONSUMPTION_MAX), ("avg", VIEW_CONSUMPTION_AVG)]:
            sdf = execute(f"SELECT * FROM {view}", fetch=True)
            for i, c in enumerate(cons_df.columns):
                if c == "date":
                    val = T[lang][label_key]
                    align = "L"
                elif not sdf.empty and c in sdf.columns:
                    try:
                        val = str(round(float(sdf.iloc[0][c]), 1))
                    except (ValueError, TypeError):
                        val = ""
                    align = "R"
                else:
                    val = ""
                    align = "R"
                pdf.cell(cons_col_widths[i], row_h, val, border=1, align=align)
            pdf.ln()

    # Build filename
    fname = f"report_{datetime.date.today().isoformat()}.pdf"

    buf = io.BytesIO()
    pdf.output(buf)
    buf.seek(0)
    return dcc.send_bytes(buf.getvalue(), filename=fname)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")), debug=False)
