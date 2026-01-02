#!/usr/bin/env python
"""
Google Spreadsheet MCP Server
A Model Context Protocol (MCP) server built with FastMCP for interacting with Google Sheets.

Overhauled for simplicity - supports A1 notation, named colors, and high-level formatting.
"""

import base64
import os
import sys
import re
from typing import List, Dict, Any, Optional, Tuple
import json
from dataclasses import dataclass
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

# MCP imports
from mcp.server.fastmcp import FastMCP, Context

# Google API imports
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import google.auth

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_CONFIG = os.environ.get('CREDENTIALS_CONFIG')
TOKEN_PATH = os.environ.get('TOKEN_PATH', 'token.json')
CREDENTIALS_PATH = os.environ.get('CREDENTIALS_PATH', 'credentials.json')
SERVICE_ACCOUNT_PATH = os.environ.get('SERVICE_ACCOUNT_PATH', 'service_account.json')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '')

# =============================================================================
# COLOR PALETTE - Named colors for easy use
# =============================================================================
COLORS = {
    # Basic colors
    "black": {"red": 0, "green": 0, "blue": 0},
    "white": {"red": 1, "green": 1, "blue": 1},
    "red": {"red": 0.92, "green": 0.26, "blue": 0.21},
    "green": {"red": 0.2, "green": 0.66, "blue": 0.33},
    "blue": {"red": 0.26, "green": 0.52, "blue": 0.96},
    "yellow": {"red": 1, "green": 0.95, "blue": 0.0},
    "orange": {"red": 0.98, "green": 0.74, "blue": 0.02},
    "purple": {"red": 0.67, "green": 0.28, "blue": 0.74},
    "cyan": {"red": 0, "green": 0.74, "blue": 0.83},
    "pink": {"red": 0.91, "green": 0.12, "blue": 0.39},
    "brown": {"red": 0.55, "green": 0.27, "blue": 0.07},

    # Grays
    "gray": {"red": 0.62, "green": 0.62, "blue": 0.62},
    "grey": {"red": 0.62, "green": 0.62, "blue": 0.62},
    "light_gray": {"red": 0.85, "green": 0.85, "blue": 0.85},
    "light_grey": {"red": 0.85, "green": 0.85, "blue": 0.85},
    "dark_gray": {"red": 0.4, "green": 0.4, "blue": 0.4},
    "dark_grey": {"red": 0.4, "green": 0.4, "blue": 0.4},

    # Light variants (for backgrounds)
    "light_red": {"red": 0.96, "green": 0.80, "blue": 0.78},
    "light_green": {"red": 0.85, "green": 0.92, "blue": 0.83},
    "light_blue": {"red": 0.81, "green": 0.89, "blue": 0.95},
    "light_yellow": {"red": 1, "green": 0.98, "blue": 0.8},
    "light_orange": {"red": 0.99, "green": 0.91, "blue": 0.79},
    "light_purple": {"red": 0.90, "green": 0.84, "blue": 0.93},
    "light_cyan": {"red": 0.80, "green": 0.94, "blue": 0.96},
    "light_pink": {"red": 0.97, "green": 0.80, "blue": 0.86},
}

# =============================================================================
# INTERNAL UTILITIES
# =============================================================================

def _parse_color(color: str) -> Optional[Dict[str, float]]:
    """
    Parse a color string into RGB dict (0-1 scale).
    Supports: named colors ("blue"), hex ("#4285F4", "F00"), rgb("rgb(66,133,244)")
    """
    if not color:
        return None

    color = color.strip().lower()

    # Named colors
    if color in COLORS:
        return COLORS[color]

    # Hex colors
    hex_match = re.match(r'^#?([0-9a-f]{6}|[0-9a-f]{3})$', color)
    if hex_match:
        hex_str = hex_match.group(1)
        if len(hex_str) == 3:
            hex_str = ''.join([c*2 for c in hex_str])
        return {
            "red": int(hex_str[0:2], 16) / 255,
            "green": int(hex_str[2:4], 16) / 255,
            "blue": int(hex_str[4:6], 16) / 255
        }

    # RGB format
    rgb_match = re.match(r'rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)', color)
    if rgb_match:
        return {
            "red": int(rgb_match.group(1)) / 255,
            "green": int(rgb_match.group(2)) / 255,
            "blue": int(rgb_match.group(3)) / 255
        }

    raise ValueError(f"Unknown color: {color}. Use named (blue, light_green), hex (#4285F4), or rgb(66,133,244)")


def _col_to_index(col: str) -> int:
    """Convert column letter(s) to 0-based index. A=0, B=1, Z=25, AA=26"""
    result = 0
    for char in col.upper():
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1


def _parse_a1(a1_range: str) -> Tuple[int, Optional[int], int, Optional[int]]:
    """
    Parse A1 notation to 0-based indexes: (start_row, end_row, start_col, end_col)
    Examples: "A1" -> (0,1,0,1), "A1:B5" -> (0,5,0,2), "B:D" -> (0,None,1,4)
    """
    if '!' in a1_range:
        a1_range = a1_range.split('!')[1]

    if ':' in a1_range:
        start, end = a1_range.split(':')
    else:
        start = end = a1_range

    start_match = re.match(r'^([A-Za-z]*)(\d*)$', start)
    end_match = re.match(r'^([A-Za-z]*)(\d*)$', end)

    if not start_match or not end_match:
        raise ValueError(f"Invalid A1 notation: {a1_range}")

    start_col_str, start_row_str = start_match.groups()
    end_col_str, end_row_str = end_match.groups()

    start_row = int(start_row_str) - 1 if start_row_str else 0
    end_row = int(end_row_str) if end_row_str else None
    start_col = _col_to_index(start_col_str) if start_col_str else 0
    end_col = _col_to_index(end_col_str) + 1 if end_col_str else None

    return (start_row, end_row, start_col, end_col)


def _get_sheet_id(sheets_service, spreadsheet_id: str, sheet_name: str) -> int:
    """Get numeric sheet ID from sheet name."""
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == sheet_name:
            return sheet['properties']['sheetId']
    raise ValueError(f"Sheet '{sheet_name}' not found")


def _grid_range(sheet_id: int, a1_range: str) -> Dict[str, Any]:
    """Build GridRange from sheet ID and A1 notation."""
    start_row, end_row, start_col, end_col = _parse_a1(a1_range)
    gr = {"sheetId": sheet_id}
    if start_row is not None:
        gr["startRowIndex"] = start_row
    if end_row is not None:
        gr["endRowIndex"] = end_row
    if start_col is not None:
        gr["startColumnIndex"] = start_col
    if end_col is not None:
        gr["endColumnIndex"] = end_col
    return gr


# =============================================================================
# LIFESPAN & SERVER SETUP
# =============================================================================

@dataclass
class SpreadsheetContext:
    sheets_service: Any
    drive_service: Any
    folder_id: Optional[str] = None


@asynccontextmanager
async def spreadsheet_lifespan(server: FastMCP) -> AsyncIterator[SpreadsheetContext]:
    """Manage Google API connection lifecycle"""
    creds = None

    if CREDENTIALS_CONFIG:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(base64.b64decode(CREDENTIALS_CONFIG)), scopes=SCOPES)

    if not creds and SERVICE_ACCOUNT_PATH and os.path.exists(SERVICE_ACCOUNT_PATH):
        try:
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH, scopes=SCOPES)
            print("Using service account authentication")
        except Exception as e:
            print(f"Service account auth failed: {e}")

    if not creds:
        if os.path.exists(TOKEN_PATH):
            with open(TOKEN_PATH, 'r') as token:
                creds = Credentials.from_authorized_user_info(json.load(token), SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    with open(TOKEN_PATH, 'w') as token:
                        token.write(creds.to_json())
                except Exception:
                    creds = None

            if not creds:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
                    creds = flow.run_local_server(port=0)
                    with open(TOKEN_PATH, 'w') as token:
                        token.write(creds.to_json())
                except Exception:
                    creds = None

    if not creds:
        try:
            creds, _ = google.auth.default(scopes=SCOPES)
        except Exception as e:
            raise Exception(f"All auth methods failed: {e}")

    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    try:
        yield SpreadsheetContext(
            sheets_service=sheets_service,
            drive_service=drive_service,
            folder_id=DRIVE_FOLDER_ID or None
        )
    finally:
        pass


_host = os.environ.get('HOST') or os.environ.get('FASTMCP_HOST') or "0.0.0.0"
_port = int(os.environ.get('PORT') or os.environ.get('FASTMCP_PORT') or "8000")

mcp = FastMCP("Google Spreadsheet",
              dependencies=["google-auth", "google-auth-oauthlib", "google-api-python-client"],
              lifespan=spreadsheet_lifespan,
              host=_host,
              port=_port)


# =============================================================================
# HIGH-LEVEL FORMATTING TOOLS
# =============================================================================

@mcp.tool()
def format_cells(spreadsheet_id: str,
                 sheet: str,
                 range: str,
                 bold: Optional[bool] = None,
                 italic: Optional[bool] = None,
                 underline: Optional[bool] = None,
                 strikethrough: Optional[bool] = None,
                 font_size: Optional[int] = None,
                 font_color: Optional[str] = None,
                 bg_color: Optional[str] = None,
                 h_align: Optional[str] = None,
                 v_align: Optional[str] = None,
                 wrap: Optional[str] = None,
                 number_format: Optional[str] = None,
                 ctx: Context = None) -> Dict[str, Any]:
    """
    Format cells using A1 notation and named colors.

    Args:
        spreadsheet_id: The spreadsheet ID
        sheet: Sheet name (e.g., "Sheet1")
        range: Cell range in A1 notation (e.g., "A1:B5", "C3")
        bold: Make text bold
        italic: Make text italic
        underline: Underline text
        strikethrough: Strikethrough text
        font_size: Font size in points
        font_color: Text color - "blue", "red", "#4285F4"
        bg_color: Background color - "light_yellow", "#FFEB3B"
        h_align: Horizontal align - "left", "center", "right"
        v_align: Vertical align - "top", "middle", "bottom"
        wrap: Text wrap - "overflow", "clip", "wrap"
        number_format: Number format pattern (e.g., "#,##0.00", "0%")

    Example:
        format_cells(id, "Sheet1", "A1:B5", bold=True, font_color="blue", bg_color="light_yellow")
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

    cell_format = {}
    text_format = {}

    if bold is not None:
        text_format["bold"] = bold
    if italic is not None:
        text_format["italic"] = italic
    if underline is not None:
        text_format["underline"] = underline
    if strikethrough is not None:
        text_format["strikethrough"] = strikethrough
    if font_size is not None:
        text_format["fontSize"] = font_size
    if font_color:
        text_format["foregroundColor"] = _parse_color(font_color)

    if text_format:
        cell_format["textFormat"] = text_format

    if bg_color:
        cell_format["backgroundColor"] = _parse_color(bg_color)

    if h_align:
        cell_format["horizontalAlignment"] = {"left": "LEFT", "center": "CENTER", "right": "RIGHT"}.get(h_align.lower(), h_align.upper())
    if v_align:
        cell_format["verticalAlignment"] = {"top": "TOP", "middle": "MIDDLE", "bottom": "BOTTOM"}.get(v_align.lower(), v_align.upper())
    if wrap:
        cell_format["wrapStrategy"] = {"overflow": "OVERFLOW_CELL", "clip": "CLIP", "wrap": "WRAP"}.get(wrap.lower(), wrap.upper())
    if number_format:
        cell_format["numberFormat"] = {"type": "NUMBER", "pattern": number_format}

    if not cell_format:
        return {"error": "No formatting options specified"}

    fields = []
    if text_format:
        fields.append("userEnteredFormat.textFormat")
    if bg_color:
        fields.append("userEnteredFormat.backgroundColor")
    if h_align:
        fields.append("userEnteredFormat.horizontalAlignment")
    if v_align:
        fields.append("userEnteredFormat.verticalAlignment")
    if wrap:
        fields.append("userEnteredFormat.wrapStrategy")
    if number_format:
        fields.append("userEnteredFormat.numberFormat")

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{
            "repeatCell": {
                "range": _grid_range(sheet_id, range),
                "cell": {"userEnteredFormat": cell_format},
                "fields": ",".join(fields)
            }
        }]}
    ).execute()


@mcp.tool()
def set_borders(spreadsheet_id: str,
                sheet: str,
                range: str,
                style: str = "solid",
                color: str = "black",
                borders: str = "all",
                ctx: Context = None) -> Dict[str, Any]:
    """
    Set borders on cells.

    Args:
        spreadsheet_id: The spreadsheet ID
        sheet: Sheet name
        range: Cell range in A1 notation
        style: "solid", "dashed", "dotted", "double", "none"
        color: Border color (named or hex)
        borders: "all", "outer", "inner", or comma-separated: "top,bottom,left,right"
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

    border_style = {"solid": "SOLID", "dashed": "DASHED", "dotted": "DOTTED", "double": "DOUBLE", "none": "NONE"}.get(style.lower(), "SOLID")
    border = {"style": border_style, "color": _parse_color(color)}

    req = {"range": _grid_range(sheet_id, range)}
    b = borders.lower()

    if b == "all":
        req.update({"top": border, "bottom": border, "left": border, "right": border, "innerHorizontal": border, "innerVertical": border})
    elif b == "outer":
        req.update({"top": border, "bottom": border, "left": border, "right": border})
    elif b == "inner":
        req.update({"innerHorizontal": border, "innerVertical": border})
    else:
        for side in b.split(","):
            side = side.strip()
            if side in ["top", "bottom", "left", "right"]:
                req[side] = border

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"updateBorders": req}]}
    ).execute()


@mcp.tool()
def merge_cells(spreadsheet_id: str, sheet: str, range: str, merge_type: str = "all", ctx: Context = None) -> Dict[str, Any]:
    """
    Merge cells. merge_type: "all" (single cell), "columns", "rows"
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
    mt = {"all": "MERGE_ALL", "columns": "MERGE_COLUMNS", "rows": "MERGE_ROWS"}.get(merge_type.lower(), "MERGE_ALL")

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"mergeCells": {"range": _grid_range(sheet_id, range), "mergeType": mt}}]}
    ).execute()


@mcp.tool()
def unmerge_cells(spreadsheet_id: str, sheet: str, range: str, ctx: Context = None) -> Dict[str, Any]:
    """Unmerge previously merged cells."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"unmergeCells": {"range": _grid_range(sheet_id, range)}}]}
    ).execute()


@mcp.tool()
def auto_resize(spreadsheet_id: str, sheet: str, columns: Optional[str] = None, rows: Optional[str] = None, ctx: Context = None) -> Dict[str, Any]:
    """
    Auto-resize columns/rows to fit content.
    columns: "A:D", "B:B", or "all" to resize all columns
    rows: "1:10", "5:5", or "all" to resize all rows
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
    requests = []

    if columns:
        if columns.lower() == "all":
            # Get sheet properties to find column count
            spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            for s in spreadsheet['sheets']:
                if s['properties']['sheetId'] == sheet_id:
                    col_count = s['properties'].get('gridProperties', {}).get('columnCount', 26)
                    requests.append({"autoResizeDimensions": {"dimensions": {
                        "sheetId": sheet_id, "dimension": "COLUMNS",
                        "startIndex": 0, "endIndex": col_count
                    }}})
                    break
        else:
            col_match = re.match(r'^([A-Za-z]+):([A-Za-z]+)$', columns)
            if col_match:
                requests.append({"autoResizeDimensions": {"dimensions": {
                    "sheetId": sheet_id, "dimension": "COLUMNS",
                    "startIndex": _col_to_index(col_match.group(1)),
                    "endIndex": _col_to_index(col_match.group(2)) + 1
                }}})

    if rows:
        if rows.lower() == "all":
            # Get sheet properties to find row count
            spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            for s in spreadsheet['sheets']:
                if s['properties']['sheetId'] == sheet_id:
                    row_count = s['properties'].get('gridProperties', {}).get('rowCount', 1000)
                    requests.append({"autoResizeDimensions": {"dimensions": {
                        "sheetId": sheet_id, "dimension": "ROWS",
                        "startIndex": 0, "endIndex": row_count
                    }}})
                    break
        else:
            row_match = re.match(r'^(\d+):(\d+)$', rows)
            if row_match:
                requests.append({"autoResizeDimensions": {"dimensions": {
                    "sheetId": sheet_id, "dimension": "ROWS",
                    "startIndex": int(row_match.group(1)) - 1,
                    "endIndex": int(row_match.group(2))
                }}})

    if not requests:
        return {"error": "Specify columns (A:D or 'all') and/or rows (1:10 or 'all')"}

    return sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()


@mcp.tool()
def freeze(spreadsheet_id: str, sheet: str, rows: int = 0, columns: int = 0, ctx: Context = None) -> Dict[str, Any]:
    """Freeze rows and/or columns. Use 0 to unfreeze."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": rows, "frozenColumnCount": columns}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
        }}]}
    ).execute()


@mcp.tool()
def delete_rows(spreadsheet_id: str, sheet: str, start_row: int, end_row: int, ctx: Context = None) -> Dict[str, Any]:
    """Delete rows. start_row and end_row are 1-based (like Sheets UI), inclusive."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"deleteDimension": {"range": {
            "sheetId": sheet_id, "dimension": "ROWS",
            "startIndex": start_row - 1, "endIndex": end_row
        }}}]}
    ).execute()


@mcp.tool()
def delete_columns(spreadsheet_id: str, sheet: str, start_column: str, end_column: str, ctx: Context = None) -> Dict[str, Any]:
    """Delete columns. Use letters like "B" to "D" (inclusive)."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"deleteDimension": {"range": {
            "sheetId": sheet_id, "dimension": "COLUMNS",
            "startIndex": _col_to_index(start_column), "endIndex": _col_to_index(end_column) + 1
        }}}]}
    ).execute()


@mcp.tool()
def clear_formatting(spreadsheet_id: str, sheet: str, range: str, ctx: Context = None) -> Dict[str, Any]:
    """Clear formatting from cells (keeps values)."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"repeatCell": {
            "range": _grid_range(sheet_id, range),
            "cell": {"userEnteredFormat": {}},
            "fields": "userEnteredFormat"
        }}]}
    ).execute()


@mcp.tool()
def set_column_width(spreadsheet_id: str, sheet: str, column: str, width: int, ctx: Context = None) -> Dict[str, Any]:
    """Set column width in pixels. column: letter like "A" or "BC"."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
    col_idx = _col_to_index(column)

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": col_idx, "endIndex": col_idx + 1},
            "properties": {"pixelSize": width}, "fields": "pixelSize"
        }}]}
    ).execute()


@mcp.tool()
def set_row_height(spreadsheet_id: str, sheet: str, row: int, height: int, ctx: Context = None) -> Dict[str, Any]:
    """Set row height in pixels. row: 1-based row number."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": row - 1, "endIndex": row},
            "properties": {"pixelSize": height}, "fields": "pixelSize"
        }}]}
    ).execute()


@mcp.tool()
def add_dropdown(spreadsheet_id: str, sheet: str, range: str, options: List[str], strict: bool = True, ctx: Context = None) -> Dict[str, Any]:
    """Add dropdown (data validation) to cells."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"setDataValidation": {
            "range": _grid_range(sheet_id, range),
            "rule": {
                "condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": opt} for opt in options]},
                "showCustomUi": True, "strict": strict
            }
        }}]}
    ).execute()


@mcp.tool()
def get_colors(ctx: Context = None) -> Dict[str, Dict[str, float]]:
    """Get available named colors."""
    return COLORS


@mcp.tool()
def batch_format(spreadsheet_id: str,
                 sheet: str,
                 formats: List[Dict[str, Any]],
                 ctx: Context = None) -> Dict[str, Any]:
    """
    Apply multiple formatting operations in a single API call.
    Much more efficient than calling format_cells multiple times.

    Args:
        spreadsheet_id: The spreadsheet ID
        sheet: Sheet name (e.g., "Sheet1")
        formats: List of format specs, each containing:
            - range: Cell range in A1 notation (e.g., "A1:B5")
            - bold, italic, underline, strikethrough: bool
            - font_size: int
            - font_color, bg_color: color string (named, hex, or rgb)
            - h_align: "left", "center", "right"
            - v_align: "top", "middle", "bottom"
            - wrap: "overflow", "clip", "wrap"
            - number_format: pattern string
            - border: border style - "solid", "dashed", "dotted", "double", "none"
            - border_color: border color (default: "black")
            - border_sides: "all", "outer", "inner", or "top,bottom,left,right"

    Example:
        batch_format(id, "Sheet1", [
            {"range": "A1:Z1", "bold": True, "bg_color": "light_blue", "border": "solid", "border_sides": "bottom"},
            {"range": "A3:A100", "font_color": "blue"},
            {"range": "B3:B100", "font_color": "green", "border": "dashed", "border_color": "gray"}
        ])
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
    requests = []

    for fmt in formats:
        cell_range = fmt.get('range')
        if not cell_range:
            continue

        cell_format = {}
        text_format = {}
        fields = []

        # Text formatting
        if fmt.get('bold') is not None:
            text_format["bold"] = fmt['bold']
        if fmt.get('italic') is not None:
            text_format["italic"] = fmt['italic']
        if fmt.get('underline') is not None:
            text_format["underline"] = fmt['underline']
        if fmt.get('strikethrough') is not None:
            text_format["strikethrough"] = fmt['strikethrough']
        if fmt.get('font_size') is not None:
            text_format["fontSize"] = fmt['font_size']
        if fmt.get('font_color'):
            text_format["foregroundColor"] = _parse_color(fmt['font_color'])

        if text_format:
            cell_format["textFormat"] = text_format
            fields.append("userEnteredFormat.textFormat")

        # Background
        if fmt.get('bg_color'):
            cell_format["backgroundColor"] = _parse_color(fmt['bg_color'])
            fields.append("userEnteredFormat.backgroundColor")

        # Alignment
        if fmt.get('h_align'):
            cell_format["horizontalAlignment"] = {"left": "LEFT", "center": "CENTER", "right": "RIGHT"}.get(fmt['h_align'].lower(), fmt['h_align'].upper())
            fields.append("userEnteredFormat.horizontalAlignment")
        if fmt.get('v_align'):
            cell_format["verticalAlignment"] = {"top": "TOP", "middle": "MIDDLE", "bottom": "BOTTOM"}.get(fmt['v_align'].lower(), fmt['v_align'].upper())
            fields.append("userEnteredFormat.verticalAlignment")

        # Wrap
        if fmt.get('wrap'):
            cell_format["wrapStrategy"] = {"overflow": "OVERFLOW_CELL", "clip": "CLIP", "wrap": "WRAP"}.get(fmt['wrap'].lower(), fmt['wrap'].upper())
            fields.append("userEnteredFormat.wrapStrategy")

        # Number format
        if fmt.get('number_format'):
            cell_format["numberFormat"] = {"type": "NUMBER", "pattern": fmt['number_format']}
            fields.append("userEnteredFormat.numberFormat")

        # Add cell format request if any formatting specified
        if cell_format and fields:
            requests.append({
                "repeatCell": {
                    "range": _grid_range(sheet_id, cell_range),
                    "cell": {"userEnteredFormat": cell_format},
                    "fields": ",".join(fields)
                }
            })

        # Border formatting (separate request type)
        if fmt.get('border'):
            style = fmt.get('border', 'solid')
            color = fmt.get('border_color', 'black')
            sides = fmt.get('border_sides', 'all')

            border_style = {"solid": "SOLID", "dashed": "DASHED", "dotted": "DOTTED", "double": "DOUBLE", "none": "NONE"}.get(style.lower(), "SOLID")
            border = {"style": border_style, "color": _parse_color(color)}

            req = {"range": _grid_range(sheet_id, cell_range)}
            s = sides.lower()

            if s == "all":
                req.update({"top": border, "bottom": border, "left": border, "right": border, "innerHorizontal": border, "innerVertical": border})
            elif s == "outer":
                req.update({"top": border, "bottom": border, "left": border, "right": border})
            elif s == "inner":
                req.update({"innerHorizontal": border, "innerVertical": border})
            else:
                for side in s.split(","):
                    side = side.strip()
                    if side in ["top", "bottom", "left", "right"]:
                        req[side] = border

            requests.append({"updateBorders": req})

    if not requests:
        return {"error": "No valid formatting operations specified"}

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()


@mcp.tool()
def write_sheet(spreadsheet_id: str,
                sheet: str,
                range: str,
                data: List[List[Any]],
                formats: Optional[List[Dict[str, Any]]] = None,
                auto_resize_columns: bool = False,
                ctx: Context = None) -> Dict[str, Any]:
    """
    Write data and optionally apply formatting in a single operation.
    Combines update_cells + batch_format + auto_resize.

    Args:
        spreadsheet_id: The spreadsheet ID
        sheet: Sheet name (e.g., "Sheet1")
        range: Starting cell in A1 notation (e.g., "A1")
        data: 2D array of values to write
        formats: Optional list of format specs (same as batch_format, including borders)
        auto_resize_columns: Auto-resize columns after writing (default: False)

    Example:
        write_sheet(id, "Sheet1", "A1",
            data=[["Name", "Value"], ["Foo", 100], ["Bar", 200]],
            formats=[
                {"range": "A1:B1", "bold": True, "bg_color": "light_blue", "border": "solid", "border_sides": "bottom"},
                {"range": "A1:B3", "border": "solid", "border_sides": "outer"}
            ],
            auto_resize_columns=True
        )
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    results = {}

    # Step 1: Write data
    write_result = sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f"{sheet}!{range}",
        valueInputOption='USER_ENTERED', body={'values': data}
    ).execute()
    results['write'] = write_result

    # Step 2: Apply formatting if specified
    if formats or auto_resize_columns:
        sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
        requests = []

        # Format requests (reuse batch_format logic)
        if formats:
            for fmt in formats:
                cell_range = fmt.get('range')
                if not cell_range:
                    continue

                cell_format = {}
                text_format = {}
                fields = []

                if fmt.get('bold') is not None:
                    text_format["bold"] = fmt['bold']
                if fmt.get('italic') is not None:
                    text_format["italic"] = fmt['italic']
                if fmt.get('underline') is not None:
                    text_format["underline"] = fmt['underline']
                if fmt.get('strikethrough') is not None:
                    text_format["strikethrough"] = fmt['strikethrough']
                if fmt.get('font_size') is not None:
                    text_format["fontSize"] = fmt['font_size']
                if fmt.get('font_color'):
                    text_format["foregroundColor"] = _parse_color(fmt['font_color'])

                if text_format:
                    cell_format["textFormat"] = text_format
                    fields.append("userEnteredFormat.textFormat")

                if fmt.get('bg_color'):
                    cell_format["backgroundColor"] = _parse_color(fmt['bg_color'])
                    fields.append("userEnteredFormat.backgroundColor")

                if fmt.get('h_align'):
                    cell_format["horizontalAlignment"] = {"left": "LEFT", "center": "CENTER", "right": "RIGHT"}.get(fmt['h_align'].lower(), fmt['h_align'].upper())
                    fields.append("userEnteredFormat.horizontalAlignment")
                if fmt.get('v_align'):
                    cell_format["verticalAlignment"] = {"top": "TOP", "middle": "MIDDLE", "bottom": "BOTTOM"}.get(fmt['v_align'].lower(), fmt['v_align'].upper())
                    fields.append("userEnteredFormat.verticalAlignment")

                if fmt.get('wrap'):
                    cell_format["wrapStrategy"] = {"overflow": "OVERFLOW_CELL", "clip": "CLIP", "wrap": "WRAP"}.get(fmt['wrap'].lower(), fmt['wrap'].upper())
                    fields.append("userEnteredFormat.wrapStrategy")

                if fmt.get('number_format'):
                    cell_format["numberFormat"] = {"type": "NUMBER", "pattern": fmt['number_format']}
                    fields.append("userEnteredFormat.numberFormat")

                if cell_format and fields:
                    requests.append({
                        "repeatCell": {
                            "range": _grid_range(sheet_id, cell_range),
                            "cell": {"userEnteredFormat": cell_format},
                            "fields": ",".join(fields)
                        }
                    })

                # Border formatting
                if fmt.get('border'):
                    style = fmt.get('border', 'solid')
                    color = fmt.get('border_color', 'black')
                    sides = fmt.get('border_sides', 'all')

                    border_style = {"solid": "SOLID", "dashed": "DASHED", "dotted": "DOTTED", "double": "DOUBLE", "none": "NONE"}.get(style.lower(), "SOLID")
                    border = {"style": border_style, "color": _parse_color(color)}

                    req = {"range": _grid_range(sheet_id, cell_range)}
                    s = sides.lower()

                    if s == "all":
                        req.update({"top": border, "bottom": border, "left": border, "right": border, "innerHorizontal": border, "innerVertical": border})
                    elif s == "outer":
                        req.update({"top": border, "bottom": border, "left": border, "right": border})
                    elif s == "inner":
                        req.update({"innerHorizontal": border, "innerVertical": border})
                    else:
                        for side in s.split(","):
                            side = side.strip()
                            if side in ["top", "bottom", "left", "right"]:
                                req[side] = border

                    requests.append({"updateBorders": req})

        # Auto-resize columns
        if auto_resize_columns:
            if data and data[0]:
                start_col = _col_to_index(re.match(r'^([A-Za-z]+)', range).group(1))
                end_col = start_col + len(data[0])
                requests.append({"autoResizeDimensions": {"dimensions": {
                    "sheetId": sheet_id, "dimension": "COLUMNS",
                    "startIndex": start_col, "endIndex": end_col
                }}})

        if requests:
            format_result = sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests}
            ).execute()
            results['format'] = format_result

    return results


# =============================================================================
# DATA TOOLS (ORIGINAL, PRESERVED)
# =============================================================================

@mcp.tool()
def get_sheet_data(spreadsheet_id: str, sheet: str, range: Optional[str] = None, include_grid_data: bool = False, ctx: Context = None) -> Dict[str, Any]:
    """Get data from a sheet. range: A1 notation like "A1:C10". include_grid_data: also return formatting."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    full_range = f"{sheet}!{range}" if range else sheet

    if include_grid_data:
        return sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id, ranges=[full_range], includeGridData=True).execute()
    else:
        result = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=full_range).execute()
        return {'spreadsheetId': spreadsheet_id, 'valueRanges': [{'range': full_range, 'values': result.get('values', [])}]}


@mcp.tool()
def get_sheet_formulas(spreadsheet_id: str, sheet: str, range: Optional[str] = None, ctx: Context = None) -> List[List[Any]]:
    """Get formulas from a sheet."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    full_range = f"{sheet}!{range}" if range else sheet
    result = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=full_range, valueRenderOption='FORMULA').execute()
    return result.get('values', [])


@mcp.tool()
def update_cells(spreadsheet_id: str, sheet: str, range: str, data: List[List[Any]], ctx: Context = None) -> Dict[str, Any]:
    """Update cells. data: 2D array of values."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    return sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f"{sheet}!{range}",
        valueInputOption='USER_ENTERED', body={'values': data}
    ).execute()


@mcp.tool()
def batch_update_cells(spreadsheet_id: str, sheet: str, ranges: Dict[str, List[List[Any]]], ctx: Context = None) -> Dict[str, Any]:
    """Batch update multiple ranges. ranges: {"A1:B2": [[1,2],[3,4]], "D1:E2": [["a","b"],["c","d"]]}"""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    data = [{'range': f"{sheet}!{r}", 'values': v} for r, v in ranges.items()]
    return sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id, body={'valueInputOption': 'USER_ENTERED', 'data': data}
    ).execute()


@mcp.tool()
def add_rows(spreadsheet_id: str, sheet: str, count: int, start_row: Optional[int] = None, ctx: Context = None) -> Dict[str, Any]:
    """Add rows. start_row: 1-based row to insert before (default: beginning)."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
    start_idx = (start_row - 1) if start_row else 0

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"insertDimension": {"range": {
            "sheetId": sheet_id, "dimension": "ROWS", "startIndex": start_idx, "endIndex": start_idx + count
        }, "inheritFromBefore": start_idx > 0}}]}
    ).execute()


@mcp.tool()
def add_columns(spreadsheet_id: str, sheet: str, count: int, start_column: Optional[str] = None, ctx: Context = None) -> Dict[str, Any]:
    """Add columns. start_column: letter to insert before (default: beginning)."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
    start_idx = _col_to_index(start_column) if start_column else 0

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"insertDimension": {"range": {
            "sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": start_idx, "endIndex": start_idx + count
        }, "inheritFromBefore": start_idx > 0}}]}
    ).execute()


# =============================================================================
# QUERY/FILTER OPERATIONS
# =============================================================================

@mcp.tool()
def filter_rows(spreadsheet_id: str, sheet: str, column: str, operator: str, value: str,
                range: Optional[str] = None, include_header: bool = True, ctx: Context = None) -> Dict[str, Any]:
    """
    Filter rows by column value.

    column: Column letter (A, B, C...) or header name to filter on
    operator: equals, not_equals, contains, not_contains, starts_with, ends_with, gt, gte, lt, lte, empty, not_empty, regex
    value: Value to compare against (ignored for empty/not_empty)
    range: Optional A1 range to filter within (default: entire sheet)
    include_header: Include header row in results (default: True)

    Returns: {filtered_rows: [[...]], total_rows: N, matched_rows: N, column_index: N}
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    full_range = f"{sheet}!{range}" if range else sheet

    result = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=full_range).execute()
    rows = result.get('values', [])

    if not rows:
        return {'filtered_rows': [], 'total_rows': 0, 'matched_rows': 0, 'column_index': -1}

    # Determine column index - either by letter or by header name
    header = rows[0] if rows else []
    col_idx = -1

    if len(column) <= 2 and column.isalpha():
        col_idx = _col_to_index(column.upper())
    else:
        # Try to find by header name (case-insensitive)
        for i, h in enumerate(header):
            if str(h).lower() == column.lower():
                col_idx = i
                break

    if col_idx == -1:
        return {'error': f'Column "{column}" not found', 'filtered_rows': [], 'total_rows': len(rows), 'matched_rows': 0, 'column_index': -1}

    def matches(cell_value: str) -> bool:
        cell_str = str(cell_value).strip() if cell_value else ''
        val = str(value).strip() if value else ''
        cell_lower = cell_str.lower()
        val_lower = val.lower()

        if operator == 'equals':
            return cell_lower == val_lower
        elif operator == 'not_equals':
            return cell_lower != val_lower
        elif operator == 'contains':
            return val_lower in cell_lower
        elif operator == 'not_contains':
            return val_lower not in cell_lower
        elif operator == 'starts_with':
            return cell_lower.startswith(val_lower)
        elif operator == 'ends_with':
            return cell_lower.endswith(val_lower)
        elif operator == 'empty':
            return cell_str == ''
        elif operator == 'not_empty':
            return cell_str != ''
        elif operator == 'regex':
            import re
            return bool(re.search(val, cell_str, re.IGNORECASE))
        elif operator in ('gt', 'gte', 'lt', 'lte'):
            try:
                cell_num = float(cell_str.replace(',', ''))
                val_num = float(val.replace(',', ''))
                if operator == 'gt':
                    return cell_num > val_num
                elif operator == 'gte':
                    return cell_num >= val_num
                elif operator == 'lt':
                    return cell_num < val_num
                elif operator == 'lte':
                    return cell_num <= val_num
            except (ValueError, TypeError):
                return False
        return False

    # Filter rows (skip header for matching, but include in output if requested)
    filtered = []
    if include_header and rows:
        filtered.append(rows[0])

    data_rows = rows[1:] if rows else []
    for row in data_rows:
        cell_val = row[col_idx] if col_idx < len(row) else ''
        if matches(cell_val):
            filtered.append(row)

    matched_count = len(filtered) - (1 if include_header and filtered else 0)
    return {
        'filtered_rows': filtered,
        'total_rows': len(rows),
        'matched_rows': matched_count,
        'column_index': col_idx
    }


@mcp.tool()
def search_cells(spreadsheet_id: str, sheet: str, search_term: str,
                 case_sensitive: bool = False, range: Optional[str] = None,
                 max_results: int = 100, ctx: Context = None) -> Dict[str, Any]:
    """
    Search for cells containing text.

    search_term: Text to search for
    case_sensitive: Match case exactly (default: False)
    range: Optional A1 range to search within
    max_results: Maximum number of matches to return (default: 100)

    Returns: {matches: [{row: N, column: "A", cell: "A1", value: "..."}], total_matches: N}
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    full_range = f"{sheet}!{range}" if range else sheet

    result = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=full_range).execute()
    rows = result.get('values', [])

    matches = []
    search = search_term if case_sensitive else search_term.lower()

    for row_idx, row in enumerate(rows):
        for col_idx, cell in enumerate(row):
            cell_str = str(cell) if cell else ''
            compare_str = cell_str if case_sensitive else cell_str.lower()

            if search in compare_str:
                col_letter = ''
                idx = col_idx
                while idx >= 0:
                    col_letter = chr(65 + idx % 26) + col_letter
                    idx = idx // 26 - 1

                matches.append({
                    'row': row_idx + 1,
                    'column': col_letter,
                    'cell': f"{col_letter}{row_idx + 1}",
                    'value': cell_str
                })

                if len(matches) >= max_results:
                    return {'matches': matches, 'total_matches': len(matches), 'truncated': True}

    return {'matches': matches, 'total_matches': len(matches), 'truncated': False}


@mcp.tool()
def query_rows(spreadsheet_id: str, sheet: str, filters: List[Dict[str, str]],
               match_all: bool = True, range: Optional[str] = None,
               include_header: bool = True, ctx: Context = None) -> Dict[str, Any]:
    """
    Query rows with multiple filter conditions.

    filters: List of filter conditions, each with {column, operator, value}
             Example: [{"column": "A", "operator": "contains", "value": "DIM_"},
                       {"column": "B", "operator": "gt", "value": "100"}]
    match_all: If True, all conditions must match (AND). If False, any condition matches (OR).
    range: Optional A1 range to query within
    include_header: Include header row in results

    Supported operators: equals, not_equals, contains, not_contains, starts_with, ends_with,
                         gt, gte, lt, lte, empty, not_empty, regex

    Returns: {filtered_rows: [[...]], total_rows: N, matched_rows: N}
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    full_range = f"{sheet}!{range}" if range else sheet

    result = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=full_range).execute()
    rows = result.get('values', [])

    if not rows:
        return {'filtered_rows': [], 'total_rows': 0, 'matched_rows': 0}

    header = rows[0] if rows else []

    # Resolve column indices for all filters
    resolved_filters = []
    for f in filters:
        col = f.get('column', '')
        col_idx = -1

        if len(col) <= 2 and col.isalpha():
            col_idx = _col_to_index(col.upper())
        else:
            for i, h in enumerate(header):
                if str(h).lower() == col.lower():
                    col_idx = i
                    break

        if col_idx == -1:
            return {'error': f'Column "{col}" not found', 'filtered_rows': [], 'total_rows': len(rows), 'matched_rows': 0}

        resolved_filters.append({
            'col_idx': col_idx,
            'operator': f.get('operator', 'equals'),
            'value': f.get('value', '')
        })

    def check_condition(row: List, flt: Dict) -> bool:
        col_idx = flt['col_idx']
        operator = flt['operator']
        value = flt['value']

        cell_val = row[col_idx] if col_idx < len(row) else ''
        cell_str = str(cell_val).strip() if cell_val else ''
        val = str(value).strip() if value else ''
        cell_lower = cell_str.lower()
        val_lower = val.lower()

        if operator == 'equals':
            return cell_lower == val_lower
        elif operator == 'not_equals':
            return cell_lower != val_lower
        elif operator == 'contains':
            return val_lower in cell_lower
        elif operator == 'not_contains':
            return val_lower not in cell_lower
        elif operator == 'starts_with':
            return cell_lower.startswith(val_lower)
        elif operator == 'ends_with':
            return cell_lower.endswith(val_lower)
        elif operator == 'empty':
            return cell_str == ''
        elif operator == 'not_empty':
            return cell_str != ''
        elif operator == 'regex':
            import re
            return bool(re.search(val, cell_str, re.IGNORECASE))
        elif operator in ('gt', 'gte', 'lt', 'lte'):
            try:
                cell_num = float(cell_str.replace(',', ''))
                val_num = float(val.replace(',', ''))
                if operator == 'gt':
                    return cell_num > val_num
                elif operator == 'gte':
                    return cell_num >= val_num
                elif operator == 'lt':
                    return cell_num < val_num
                elif operator == 'lte':
                    return cell_num <= val_num
            except (ValueError, TypeError):
                return False
        return False

    def row_matches(row: List) -> bool:
        if match_all:
            return all(check_condition(row, f) for f in resolved_filters)
        else:
            return any(check_condition(row, f) for f in resolved_filters)

    filtered = []
    if include_header and rows:
        filtered.append(rows[0])

    data_rows = rows[1:] if rows else []
    for row in data_rows:
        if row_matches(row):
            filtered.append(row)

    matched_count = len(filtered) - (1 if include_header and filtered else 0)
    return {
        'filtered_rows': filtered,
        'total_rows': len(rows),
        'matched_rows': matched_count
    }


# =============================================================================
# SHEET/SPREADSHEET MANAGEMENT (ORIGINAL, PRESERVED)
# =============================================================================

@mcp.tool()
def list_sheets(spreadsheet_id: str, ctx: Context = None) -> List[str]:
    """List all sheet names in a spreadsheet."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return [s['properties']['title'] for s in spreadsheet['sheets']]


@mcp.tool()
def copy_sheet(src_spreadsheet: str, src_sheet: str, dst_spreadsheet: str, dst_sheet: str, ctx: Context = None) -> Dict[str, Any]:
    """Copy a sheet to another spreadsheet."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    src_sheet_id = _get_sheet_id(sheets_service, src_spreadsheet, src_sheet)

    copy_result = sheets_service.spreadsheets().sheets().copyTo(
        spreadsheetId=src_spreadsheet, sheetId=src_sheet_id,
        body={"destinationSpreadsheetId": dst_spreadsheet}
    ).execute()

    if copy_result.get('title') != dst_sheet:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=dst_spreadsheet,
            body={"requests": [{"updateSheetProperties": {
                "properties": {"sheetId": copy_result['sheetId'], "title": dst_sheet}, "fields": "title"
            }}]}
        ).execute()

    return copy_result


@mcp.tool()
def rename_sheet(spreadsheet: str, sheet: str, new_name: str, ctx: Context = None) -> Dict[str, Any]:
    """Rename a sheet."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet, sheet)

    return sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet,
        body={"requests": [{"updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "title": new_name}, "fields": "title"
        }}]}
    ).execute()


@mcp.tool()
def get_multiple_sheet_data(queries: List[Dict[str, str]], ctx: Context = None) -> List[Dict[str, Any]]:
    """Get data from multiple ranges. queries: [{"spreadsheet_id": "x", "sheet": "y", "range": "A1:B5"}, ...]"""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    results = []

    for q in queries:
        try:
            if not all([q.get('spreadsheet_id'), q.get('sheet'), q.get('range')]):
                results.append({**q, 'error': 'Missing spreadsheet_id, sheet, or range'})
                continue
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=q['spreadsheet_id'], range=f"{q['sheet']}!{q['range']}"
            ).execute()
            results.append({**q, 'data': result.get('values', [])})
        except Exception as e:
            results.append({**q, 'error': str(e)})

    return results


@mcp.tool()
def get_multiple_spreadsheet_summary(spreadsheet_ids: List[str], rows_to_fetch: int = 5, ctx: Context = None) -> List[Dict[str, Any]]:
    """Get summary of multiple spreadsheets (sheet names, headers, first rows)."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    summaries = []

    for sid in spreadsheet_ids:
        summary = {'spreadsheet_id': sid, 'title': None, 'sheets': [], 'error': None}
        try:
            ss = sheets_service.spreadsheets().get(spreadsheetId=sid, fields='properties.title,sheets(properties(title,sheetId))').execute()
            summary['title'] = ss.get('properties', {}).get('title', 'Unknown')

            for sheet in ss.get('sheets', []):
                sheet_title = sheet.get('properties', {}).get('title')
                sheet_summary = {'title': sheet_title, 'headers': [], 'first_rows': [], 'error': None}
                try:
                    result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=sid, range=f"{sheet_title}!A1:{rows_to_fetch}"
                    ).execute()
                    values = result.get('values', [])
                    if values:
                        sheet_summary['headers'] = values[0]
                        sheet_summary['first_rows'] = values[1:] if len(values) > 1 else []
                except Exception as e:
                    sheet_summary['error'] = str(e)
                summary['sheets'].append(sheet_summary)
        except Exception as e:
            summary['error'] = str(e)
        summaries.append(summary)

    return summaries


@mcp.resource("spreadsheet://{spreadsheet_id}/info")
def get_spreadsheet_info(spreadsheet_id: str) -> str:
    """Get spreadsheet info as JSON."""
    context = mcp.get_lifespan_context()
    ss = context.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return json.dumps({
        "title": ss.get('properties', {}).get('title', 'Unknown'),
        "sheets": [{"title": s['properties']['title'], "sheetId": s['properties']['sheetId']} for s in ss.get('sheets', [])]
    }, indent=2)


@mcp.tool()
def create_spreadsheet(title: str, folder_id: Optional[str] = None, ctx: Context = None) -> Dict[str, Any]:
    """Create a new spreadsheet."""
    drive_service = ctx.request_context.lifespan_context.drive_service
    target_folder = folder_id or ctx.request_context.lifespan_context.folder_id

    body = {'name': title, 'mimeType': 'application/vnd.google-apps.spreadsheet'}
    if target_folder:
        body['parents'] = [target_folder]

    result = drive_service.files().create(supportsAllDrives=True, body=body, fields='id, name, parents').execute()
    return {'spreadsheetId': result['id'], 'title': result['name'], 'folder': result.get('parents', ['root'])[0]}


@mcp.tool()
def create_sheet(spreadsheet_id: str, title: str, ctx: Context = None) -> Dict[str, Any]:
    """Create a new sheet tab."""
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": title}}}]}
    ).execute()
    props = result['replies'][0]['addSheet']['properties']
    return {'sheetId': props['sheetId'], 'title': props['title'], 'spreadsheetId': spreadsheet_id}


@mcp.tool()
def list_spreadsheets(folder_id: Optional[str] = None, ctx: Context = None) -> List[Dict[str, str]]:
    """List spreadsheets in a folder."""
    drive_service = ctx.request_context.lifespan_context.drive_service
    target_folder = folder_id or ctx.request_context.lifespan_context.folder_id

    query = "mimeType='application/vnd.google-apps.spreadsheet'"
    if target_folder:
        query += f" and '{target_folder}' in parents"

    result = drive_service.files().list(
        q=query, spaces='drive', includeItemsFromAllDrives=True, supportsAllDrives=True,
        fields='files(id, name)', orderBy='modifiedTime desc'
    ).execute()

    return [{'id': f['id'], 'title': f['name']} for f in result.get('files', [])]


@mcp.tool()
def share_spreadsheet(spreadsheet_id: str, recipients: List[Dict[str, str]], send_notification: bool = True, ctx: Context = None) -> Dict[str, List]:
    """Share spreadsheet. recipients: [{"email_address": "x@y.com", "role": "writer"}, ...]"""
    drive_service = ctx.request_context.lifespan_context.drive_service
    successes, failures = [], []

    for r in recipients:
        email, role = r.get('email_address'), r.get('role', 'writer')
        if not email:
            failures.append({'email': None, 'error': 'Missing email_address'})
            continue
        if role not in ['reader', 'commenter', 'writer']:
            failures.append({'email': email, 'error': f"Invalid role: {role}"})
            continue
        try:
            result = drive_service.permissions().create(
                fileId=spreadsheet_id, body={'type': 'user', 'role': role, 'emailAddress': email},
                sendNotificationEmail=send_notification, fields='id'
            ).execute()
            successes.append({'email': email, 'role': role, 'permissionId': result['id']})
        except Exception as e:
            failures.append({'email': email, 'error': str(e)})

    return {"successes": successes, "failures": failures}


@mcp.tool()
def list_folders(parent_folder_id: Optional[str] = None, ctx: Context = None) -> List[Dict[str, str]]:
    """List folders in Drive."""
    drive_service = ctx.request_context.lifespan_context.drive_service

    query = "mimeType='application/vnd.google-apps.folder'"
    query += f" and '{parent_folder_id}' in parents" if parent_folder_id else " and 'root' in parents"

    result = drive_service.files().list(
        q=query, spaces='drive', includeItemsFromAllDrives=True, supportsAllDrives=True,
        fields='files(id, name, parents)', orderBy='name'
    ).execute()

    return [{'id': f['id'], 'name': f['name'], 'parent': f.get('parents', ['root'])[0]} for f in result.get('files', [])]


@mcp.tool()
def batch_update(spreadsheet_id: str, requests: List[Dict[str, Any]], ctx: Context = None) -> Dict[str, Any]:
    """
    Low-level batch update for advanced operations.
    For common operations, prefer: format_cells, set_borders, merge_cells, etc.
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    if not requests:
        return {"error": "requests cannot be empty"}
    return sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()


def main():
    transport = "stdio"
    for i, arg in enumerate(sys.argv):
        if arg == "--transport" and i + 1 < len(sys.argv):
            transport = sys.argv[i + 1]
            break
    mcp.run(transport=transport)
