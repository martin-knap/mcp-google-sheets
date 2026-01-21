#!/usr/bin/env python
"""
Google Spreadsheet MCP Server - v2 (Consolidated)

A Model Context Protocol (MCP) server with 6 powerful tools + semantic presets.
Reduced from 38 tools to 6, with added chart/pivot/conditional formatting support.
"""

import base64
import os
import sys
import re
import time
from typing import List, Dict, Any, Optional, Tuple, Union
from enum import Enum
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
# SEMANTIC PRESETS LIBRARY
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

    # Dark variants (for headers)
    "dark_blue": {"red": 0.10, "green": 0.27, "blue": 0.53},
    "dark_green": {"red": 0.15, "green": 0.40, "blue": 0.20},
    "dark_red": {"red": 0.60, "green": 0.15, "blue": 0.15},

    # Native table theme colors (Google Sheets defaults)
    "table_gray": {"red": 0.384, "green": 0.431, "blue": 0.478},      # #626e7a
    "table_green": {"red": 0.286, "green": 0.337, "blue": 0.298},    # #49564c (default)
    "table_red": {"red": 0.608, "green": 0.212, "blue": 0.259},      # #9b3642
}

# Text/Cell Style Presets
STYLES = {
    # Headings
    "h1": {"bold": True, "font_size": 14, "bg_color": "dark_blue", "font_color": "white", "h_align": "center"},
    "h2": {"bold": True, "font_size": 12, "bg_color": "light_blue"},
    "h3": {"bold": True, "font_size": 11},
    "title": {"bold": True, "font_size": 18, "h_align": "center"},
    "subtitle": {"italic": True, "font_size": 14, "font_color": "gray"},

    # Status indicators
    "success": {"font_color": "green", "bold": True},
    "error": {"font_color": "red", "bold": True},
    "warning": {"font_color": "orange", "bold": True},
    "info": {"font_color": "blue"},
    "muted": {"font_color": "gray"},
    "highlight": {"bg_color": "light_yellow"},

    # Quick formatting
    "bold": {"bold": True},
    "italic": {"italic": True},
    "underline": {"underline": True},
    "strikethrough": {"strikethrough": True},
    "center": {"h_align": "center"},
    "right": {"h_align": "right"},
    "left": {"h_align": "left"},
    "wrap": {"wrap": "wrap"},
    "code": {"font_color": "dark_gray", "bg_color": "light_gray"},

    # Data-specific
    "header": {"bold": True, "bg_color": "light_blue", "h_align": "center"},
    "total": {"bold": True, "bg_color": "light_gray"},
    "negative": {"font_color": "red"},
    "positive": {"font_color": "green"},

    # ASCII art / diagrams (monospace)
    "mono": {"font_family": "Courier New", "font_size": 10},
    "ascii": {"font_family": "Courier New", "font_size": 10, "bg_color": "white"},
    "diagram": {"font_family": "Courier New", "font_size": 11, "bg_color": "white"},
}

# Native Google Sheets Table Presets (using addTable API)
# Native tables use Google's default styling with auto-expand and filters
TABLE_STYLES = {
    # Default - native Google Sheets table with default green theme
    "table": {"native": True},  # Uses Google's default green theme

    # Native tables with custom color themes
    "table_green": {
        "native": True,
        "header_color": "table_green",      # #49564c
        "band_color": {"red": 0.965, "green": 0.973, "blue": 0.976},  # Light green-gray
    },
    "table_gray": {
        "native": True,
        "header_color": "table_gray",       # #626e7a
        "band_color": {"red": 0.953, "green": 0.957, "blue": 0.965},  # Light gray
    },
    "table_red": {
        "native": True,
        "header_color": "table_red",        # #9b3642
        "band_color": {"red": 0.976, "green": 0.957, "blue": 0.961},  # Light red
    },

    # Legacy manual styling (no auto-expand, no filters, custom colors)
    "basic": {
        "header": {"bold": True, "bg_color": "light_blue", "h_align": "center"},
        "header_border": {"style": "solid", "sides": "bottom"},
        "body_border": {"style": "solid", "sides": "outer"},
        "auto_resize": True,
        "native": False,
    },
    "basic_striped": {
        "header": {"bold": True, "bg_color": "dark_blue", "font_color": "white", "h_align": "center"},
        "header_border": {"style": "solid", "sides": "bottom", "color": "dark_blue"},
        "odd_rows": {"bg_color": "light_gray"},
        "body_border": {"style": "solid", "sides": "all"},
        "auto_resize": True,
        "native": False,
    },
    "basic_bordered": {
        "header": {"bold": True, "bg_color": "light_gray", "h_align": "center"},
        "body_border": {"style": "solid", "sides": "all"},
        "auto_resize": True,
        "native": False,
    },
}

# Number Format Presets
NUMBER_FORMATS = {
    "currency": "$#,##0.00",
    "currency_whole": "$#,##0",
    "currency_neg_red": "$#,##0.00;[Red]($#,##0.00)",
    "accounting": '_($* #,##0.00_)',
    "euro": "€#,##0.00",
    "pound": "£#,##0.00",
    "percent": "0.00%",
    "percent_whole": "0%",
    "number": "#,##0",
    "decimal": "#,##0.00",
    "decimal_4": "#,##0.0000",
    "integer": "0",
    "date": "yyyy-mm-dd",
    "date_short": "mm/dd/yy",
    "date_long": "mmmm d, yyyy",
    "date_eu": "dd/mm/yyyy",
    "datetime": "yyyy-mm-dd hh:mm:ss",
    "time": "hh:mm:ss",
    "time_short": "hh:mm",
    "phone": "(###) ###-####",
    "zip": "00000",
    "scientific": "0.00E+00",
}

# Chart Style Presets
CHART_STYLES = {
    "default": {
        "legend_position": "BOTTOM_LEGEND",
        "title_text_position": {"horizontalAlignment": "CENTER"},
    },
    "minimal": {
        "legend_position": "NO_LEGEND",
        "background_color": {"red": 1, "green": 1, "blue": 1, "alpha": 0},
    },
    "presentation": {
        "legend_position": "RIGHT_LEGEND",
        "title_text_format": {"bold": True, "fontSize": 14},
    },
}

# =============================================================================
# ASCII DIAGRAM BUILDER UTILITIES
# =============================================================================

# Box-drawing characters
ASCII = {
    "tl": "┌", "tr": "┐", "bl": "└", "br": "┘",  # corners
    "h": "─", "v": "│",                            # lines
    "arrow_r": "►", "arrow_l": "◄", "arrow_d": "▼", "arrow_u": "▲",
    "line_r": "─►", "line_l": "◄─", "line_d": "─▼", "line_u": "▲─",
    "t_down": "┬", "t_up": "┴", "t_right": "├", "t_left": "┤", "cross": "┼",
}

# Chart rendering characters
CHART_BLOCKS = "█▉▊▋▌▍▎▏"  # Full to 1/8 blocks (8 levels)
SPARKLINE_CHARS = "▁▂▃▄▅▆▇█"  # Bottom to top (8 levels)

# Shading palettes (light to dark)
SHADING_PALETTES = {
    "ascii": " .:-=+*#%@",      # Classic ASCII art
    "blocks": " ░▒▓█",           # Smooth block gradients
    "dots": " ·∘○●◉",            # Geometric dots
    "density": " .,;!lI$@",      # High detail
    "braille": "⠀⠁⠃⠇⠏⠟⠿⣿",       # Ultra-fine braille
}

# Box drawing character sets
BOX_STYLES = {
    "light": {"h": "─", "v": "│", "tl": "┌", "tr": "┐", "bl": "└", "br": "┘", "t_down": "┬", "t_up": "┴", "t_right": "├", "t_left": "┤", "cross": "┼"},
    "heavy": {"h": "━", "v": "┃", "tl": "┏", "tr": "┓", "bl": "┗", "br": "┛", "t_down": "┳", "t_up": "┻", "t_right": "┣", "t_left": "┫", "cross": "╋"},
    "double": {"h": "═", "v": "║", "tl": "╔", "tr": "╗", "bl": "╚", "br": "╝", "t_down": "╦", "t_up": "╩", "t_right": "╠", "t_left": "╣", "cross": "╬"},
    "rounded": {"h": "─", "v": "│", "tl": "╭", "tr": "╮", "bl": "╰", "br": "╯", "t_down": "┬", "t_up": "┴", "t_right": "├", "t_left": "┤", "cross": "┼"},
}


def _ascii_center(text: str, width: int) -> str:
    """Center text within given width."""
    if len(text) >= width:
        return text[:width]
    padding = width - len(text)
    left = padding // 2
    right = padding - left
    return " " * left + text + " " * right


def _ascii_box(content: Union[str, List[str]], width: int = None, padding: int = 1,
               bottom_connector: bool = False, top_connector: bool = False) -> List[str]:
    """
    Create a box around content.

    Args:
        content: Single string or list of strings (lines)
        width: Total box width (auto-calculated if None)
        padding: Internal padding on each side
        bottom_connector: Add ┴ connector at bottom center (for arrows going out)
        top_connector: Add ┬ connector at top center (for arrows coming in)

    Returns:
        List of strings representing the box
    """
    lines = [content] if isinstance(content, str) else content
    inner_width = max(len(line) for line in lines) if not width else width - 2 - (padding * 2)
    total_width = inner_width + 2 + (padding * 2)

    result = []
    # Top border (with optional connector)
    if top_connector:
        half = (total_width - 3) // 2
        remainder = (total_width - 3) - half
        result.append(ASCII["tl"] + ASCII["h"] * half + ASCII["t_down"] + ASCII["h"] * remainder + ASCII["tr"])
    else:
        result.append(ASCII["tl"] + ASCII["h"] * (total_width - 2) + ASCII["tr"])
    # Content lines
    pad = " " * padding
    for line in lines:
        centered = _ascii_center(line, inner_width)
        result.append(ASCII["v"] + pad + centered + pad + ASCII["v"])
    # Bottom border (with optional connector)
    if bottom_connector:
        half = (total_width - 3) // 2
        remainder = (total_width - 3) - half
        result.append(ASCII["bl"] + ASCII["h"] * half + ASCII["t_up"] + ASCII["h"] * remainder + ASCII["br"])
    else:
        result.append(ASCII["bl"] + ASCII["h"] * (total_width - 2) + ASCII["br"])
    return result


def _ascii_box_row(boxes: List[Dict[str, Any]], spacing: int = 4, merge_bottom: bool = False) -> List[str]:
    """
    Create multiple boxes side by side.

    Args:
        boxes: List of box definitions with "text" or "lines"
        spacing: Space between boxes (default 4)
        merge_bottom: Add merge connector lines below boxes

    Returns:
        List of strings representing the row of boxes
    """
    # Render each box (no connectors - regular corners)
    rendered_boxes = []
    for box_def in boxes:
        content = box_def.get("lines", [box_def.get("text", "")])
        box_lines = _ascii_box(content)
        rendered_boxes.append(box_lines)

    # Find max height
    max_height = max(len(b) for b in rendered_boxes)

    # Pad boxes to same height
    for i, box in enumerate(rendered_boxes):
        while len(box) < max_height:
            box.insert(-1, ASCII["v"] + " " * (len(box[0]) - 2) + ASCII["v"])

    # Combine horizontally
    result = []
    spacer = " " * spacing
    for row_idx in range(max_height):
        row_parts = [box[row_idx] for box in rendered_boxes]
        result.append(spacer.join(row_parts))

    # Add merge lines if requested
    if merge_bottom and len(rendered_boxes) > 1:
        # Calculate positions for merge connectors
        box_centers = []
        pos = 0
        for box in rendered_boxes:
            box_width = len(box[0])
            box_centers.append(pos + box_width // 2)
            pos += box_width + spacing

        # Create merge lines
        total_width = pos - spacing

        # Vertical lines down from each box
        merge_line1 = [" "] * total_width
        for center in box_centers:
            if center < total_width:
                merge_line1[center] = ASCII["v"]
        result.append("".join(merge_line1))

        # Horizontal connector line
        merge_line2 = [" "] * total_width
        left_center = box_centers[0]
        right_center = box_centers[-1]
        for i in range(left_center, right_center + 1):
            merge_line2[i] = ASCII["h"]
        merge_line2[left_center] = ASCII["bl"]
        merge_line2[right_center] = ASCII["br"]
        # Determine the down connector position - use row center for alignment
        mid = total_width // 2

        # Middle boxes just merge into the horizontal line (no t_up connectors)
        # This creates a cleaner look: ──┼── at center instead of ┴┬
        merge_line2[mid] = ASCII["t_down"]
        result.append("".join(merge_line2))

        # Vertical line down from center
        center_line = [" "] * total_width
        center_line[mid] = ASCII["v"]
        result.append("".join(center_line))

    return result


def _ascii_title_box(title: str, width: int = 77) -> List[str]:
    """Create a title box (header bar) with centered title."""
    inner = width - 2
    return [
        ASCII["tl"] + ASCII["h"] * inner + ASCII["tr"],
        ASCII["v"] + _ascii_center(title, inner) + ASCII["v"],
        ASCII["bl"] + ASCII["h"] * inner + ASCII["br"],
    ]


def _ascii_frame(content_lines: List[str], width: int = 77, padding: int = 1, dashed: bool = True) -> List[str]:
    """
    Wrap content lines with a frame/border.

    Args:
        content_lines: Lines of content to frame
        width: Total frame width
        padding: Vertical padding (empty lines) inside frame
        dashed: Use dashed border style (─ ─ ─) vs solid (───)

    Returns:
        List of strings with frame around content
    """
    inner_width = width - 4  # 2 for borders + 2 for spacing
    result = []

    # Top border
    if dashed:
        border_char = ASCII["h"] + " "
        border = (border_char * ((inner_width + 2) // 2))[:inner_width + 2]
    else:
        border = ASCII["h"] * (inner_width + 2)
    result.append(ASCII["tl"] + border + ASCII["tr"])

    # Top padding
    for _ in range(padding):
        result.append(ASCII["v"] + " " * (inner_width + 2) + ASCII["v"])

    # Content lines
    for line in content_lines:
        # Pad or truncate line to fit
        if len(line) < inner_width:
            padded = line + " " * (inner_width - len(line))
        else:
            padded = line[:inner_width]
        result.append(ASCII["v"] + " " + padded + " " + ASCII["v"])

    # Bottom padding
    for _ in range(padding):
        result.append(ASCII["v"] + " " * (inner_width + 2) + ASCII["v"])

    # Bottom border
    result.append(ASCII["bl"] + border + ASCII["br"])

    return result


def _ascii_comment(text: str) -> str:
    """Create a comment annotation like: ◄── Comment text"""
    return f"  {ASCII['arrow_l']}{ASCII['h']}{ASCII['h']} {text}"


def _ascii_arrow(direction: str = "right", length: int = 8) -> str:
    """Create an arrow line. Direction: right, left, down, up."""
    if direction == "right":
        return ASCII["h"] * (length - 1) + ASCII["arrow_r"]
    elif direction == "left":
        return ASCII["arrow_l"] + ASCII["h"] * (length - 1)
    elif direction == "down":
        return ASCII["v"]  # Vertical arrows are single char per line
    elif direction == "up":
        return ASCII["v"]
    return ASCII["h"] * length


def _ascii_bar(value: float, max_value: float, bar_width: int = 20) -> str:
    """
    Create a horizontal bar using block characters.

    Args:
        value: The value to represent
        max_value: Maximum value for scaling
        bar_width: Total width of the bar in characters

    Returns:
        String of block characters representing the value
    """
    if max_value <= 0 or value <= 0:
        return ""

    # Calculate how many "eighths" to fill
    ratio = min(value / max_value, 1.0)
    total_eighths = int(ratio * bar_width * 8)

    full_blocks = total_eighths // 8
    remainder = total_eighths % 8

    bar = CHART_BLOCKS[0] * full_blocks  # Full blocks (█)
    if remainder > 0:
        bar += CHART_BLOCKS[8 - remainder]  # Partial block

    return bar


def _ascii_bar_chart(
    data: List[Tuple[str, float]],
    bar_width: int = 20,
    show_values: bool = True,
    label_width: int = None
) -> List[str]:
    """
    Create a horizontal bar chart.

    Args:
        data: List of (label, value) tuples
        bar_width: Width of the bar area in characters
        show_values: Whether to show numeric values after bars
        label_width: Fixed label width (auto-calculated if None)

    Returns:
        List of strings representing the chart lines
    """
    if not data:
        return []

    max_value = max(v for _, v in data)
    if label_width is None:
        label_width = max(len(label) for label, _ in data)

    result = []
    for label, value in data:
        bar = _ascii_bar(value, max_value, bar_width)
        line = f"{label:<{label_width}} │{bar}"
        if show_values:
            line += f" {value:,.0f}" if isinstance(value, (int, float)) and value == int(value) else f" {value:,.2f}"
        result.append(line)

    return result


def _ascii_bar_chart_vertical(
    data: List[Tuple[str, float]],
    bar_height: int = 10,
    bar_width: int = 3,
    show_values: bool = True,
    gap: int = 1
) -> List[str]:
    """
    Create a vertical bar chart.

    Args:
        data: List of (label, value) tuples
        bar_height: Maximum height of bars in characters
        bar_width: Width of each bar in characters
        show_values: Whether to show numeric values above bars
        gap: Space between bars

    Returns:
        List of strings representing the chart lines
    """
    if not data:
        return []

    max_value = max(v for _, v in data)
    if max_value <= 0:
        max_value = 1

    # Calculate bar heights (in eighths for smooth rendering)
    bar_heights = []
    for _, value in data:
        ratio = value / max_value
        total_eighths = int(ratio * bar_height * 8)
        bar_heights.append(total_eighths)

    result = []

    # Value labels row (if enabled)
    if show_values:
        value_line = ""
        for i, (_, value) in enumerate(data):
            val_str = f"{value:,.0f}" if value == int(value) else f"{value:,.1f}"
            val_str = val_str.center(bar_width)
            value_line += val_str + " " * gap
        result.append(value_line.rstrip())
        result.append("")  # spacer

    # Build chart from top to bottom
    for row in range(bar_height, 0, -1):
        line = ""
        row_threshold = row * 8  # This row starts at this many eighths

        for i, eighths in enumerate(bar_heights):
            if eighths >= row_threshold:
                # Full block for this row
                line += CHART_BLOCKS[0] * bar_width
            elif eighths > (row - 1) * 8:
                # Partial block - calculate which character
                partial = eighths - (row - 1) * 8
                # Use vertical block characters (▁▂▃▄▅▆▇█)
                char = SPARKLINE_CHARS[partial - 1] if partial > 0 else " "
                line += char * bar_width
            else:
                # Empty
                line += " " * bar_width
            line += " " * gap

        result.append(line.rstrip())

    # Baseline
    total_width = len(data) * (bar_width + gap) - gap
    result.append("─" * total_width)

    # Labels row
    label_line = ""
    for label, _ in data:
        # Truncate or pad label to fit bar width
        lbl = label[:bar_width].center(bar_width)
        label_line += lbl + " " * gap
    result.append(label_line.rstrip())

    return result


def _ascii_sparkline(values: List[float]) -> str:
    """
    Create a sparkline from a list of values.

    Args:
        values: List of numeric values

    Returns:
        String of sparkline characters
    """
    if not values:
        return ""

    min_val = min(values)
    max_val = max(values)

    if max_val == min_val:
        return SPARKLINE_CHARS[4] * len(values)  # Middle char if all same

    result = []
    for v in values:
        # Scale to 0-7 range
        idx = int((v - min_val) / (max_val - min_val) * 7)
        idx = max(0, min(7, idx))
        result.append(SPARKLINE_CHARS[idx])

    return "".join(result)


def _ascii_progress_bar(value: float, max_value: float = 100, width: int = 20, show_percent: bool = True) -> str:
    """
    Create a progress bar.

    Args:
        value: Current value
        max_value: Maximum value (default 100 for percentage)
        width: Width of the bar
        show_percent: Whether to show percentage label

    Returns:
        Progress bar string like [████████░░░░░░░░] 50%
    """
    ratio = min(value / max_value, 1.0) if max_value > 0 else 0
    filled = int(ratio * width)
    empty = width - filled

    bar = f"[{CHART_BLOCKS[0] * filled}{'░' * empty}]"
    if show_percent:
        bar += f" {ratio * 100:.0f}%"

    return bar


def _ascii_shade_value(x: int, y: int, width: int, height: int, direction: str = "radial") -> float:
    """
    Calculate shading value (0.0-1.0) based on position and direction.

    Args:
        x, y: Current position
        width, height: Total dimensions
        direction: horizontal, vertical, radial, diagonal

    Returns:
        Float 0.0 (light) to 1.0 (dark)
    """
    import math

    if width <= 0 or height <= 0:
        return 0.5

    center_x = width / 2
    center_y = height / 2

    if direction == "horizontal":
        return x / width
    elif direction == "vertical":
        return y / height
    elif direction == "radial":
        dx = (x - center_x) / center_x if center_x > 0 else 0
        dy = (y - center_y) / center_y if center_y > 0 else 0
        dist = math.sqrt(dx * dx + dy * dy)
        return min(dist, 1.0)
    elif direction == "diagonal":
        return (x + y) / (width + height)
    elif direction == "diagonal_reverse":
        return (width - x + y) / (width + height)
    else:
        return 0.5


def _ascii_apply_contrast(value: float, contrast: float = 0.7) -> float:
    """
    Apply contrast adjustment to shade value.

    Args:
        value: Input value 0.0-1.0
        contrast: 0.0 (flat) to 1.0 (high contrast)

    Returns:
        Adjusted value 0.0-1.0
    """
    if contrast < 0.5:
        # Reduce contrast - compress to middle
        range_compress = contrast * 2
        result = 0.5 + (value - 0.5) * range_compress
    else:
        # Increase contrast - expand from middle
        range_expand = (contrast - 0.5) * 2 + 1
        if value < 0.5:
            result = 0.5 - (0.5 - value) * range_expand
        else:
            result = 0.5 + (value - 0.5) * range_expand

    return max(0.0, min(1.0, result))


def _ascii_shaded_box(
    width: int = 40,
    height: int = 10,
    title: str = None,
    palette: str = "blocks",
    direction: str = "radial",
    contrast: float = 0.7,
    box_style: str = "light"
) -> List[str]:
    """
    Create a shaded box with gradient fill.

    Args:
        width: Box width
        height: Box height
        title: Optional title in top border
        palette: Shading palette (ascii, blocks, dots, density, braille)
        direction: Gradient direction (horizontal, vertical, radial, diagonal)
        contrast: Contrast level 0.0-1.0
        box_style: Border style (light, heavy, double, rounded)

    Returns:
        List of strings representing the shaded box
    """
    chars = BOX_STYLES.get(box_style, BOX_STYLES["light"])
    shade_chars = SHADING_PALETTES.get(palette, SHADING_PALETTES["blocks"])

    lines = []
    inner_width = width - 2
    inner_height = height - 2

    # Top border with optional title
    if title:
        title_display = f" {title} "
        title_len = len(title_display)
        left_pad = (inner_width - title_len) // 2
        right_pad = inner_width - title_len - left_pad
        top = chars["tl"] + chars["h"] * left_pad + title_display + chars["h"] * right_pad + chars["tr"]
    else:
        top = chars["tl"] + chars["h"] * inner_width + chars["tr"]
    lines.append(top)

    # Middle rows with shading
    for y in range(inner_height):
        row = chars["v"]
        for x in range(inner_width):
            shade = _ascii_shade_value(x, y, inner_width, inner_height, direction)
            shade = _ascii_apply_contrast(shade, contrast)
            char_idx = int(shade * (len(shade_chars) - 1))
            char_idx = max(0, min(char_idx, len(shade_chars) - 1))
            row += shade_chars[char_idx]
        row += chars["v"]
        lines.append(row)

    # Bottom border
    bottom = chars["bl"] + chars["h"] * inner_width + chars["br"]
    lines.append(bottom)

    return lines


def _ascii_table(
    headers: List[str],
    rows: List[List[str]],
    box_style: str = "light"
) -> List[str]:
    """
    Create a bordered table.

    Args:
        headers: List of column headers
        rows: List of rows (each row is list of cell values)
        box_style: Border style (light, heavy, double, rounded)

    Returns:
        List of strings representing the table
    """
    chars = BOX_STYLES.get(box_style, BOX_STYLES["light"])

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(cell)))

    lines = []

    # Top border
    top = chars["tl"]
    for i, w in enumerate(col_widths):
        top += chars["h"] * (w + 2)
        top += chars["t_down"] if i < len(col_widths) - 1 else chars["tr"]
    lines.append(top)

    # Header row
    header_row = chars["v"]
    for i, h in enumerate(headers):
        header_row += f" {h.ljust(col_widths[i])} " + chars["v"]
    lines.append(header_row)

    # Header separator
    sep = chars["t_right"]
    for i, w in enumerate(col_widths):
        sep += chars["h"] * (w + 2)
        sep += chars["cross"] if i < len(col_widths) - 1 else chars["t_left"]
    lines.append(sep)

    # Data rows
    for row in rows:
        data_row = chars["v"]
        for i, cell in enumerate(row):
            if i < len(col_widths):
                data_row += f" {str(cell).ljust(col_widths[i])} " + chars["v"]
        lines.append(data_row)

    # Bottom border
    bottom = chars["bl"]
    for i, w in enumerate(col_widths):
        bottom += chars["h"] * (w + 2)
        bottom += chars["t_up"] if i < len(col_widths) - 1 else chars["br"]
    lines.append(bottom)

    return lines


def _ascii_diagram(elements: List[Dict[str, Any]], width: int = 77, frame: bool = False) -> str:
    """
    Build a complete diagram from elements.

    Elements can be:
        {"type": "title", "text": "TITLE"}
        {"type": "box", "text": "Content", "x": 4, "comment": "← annotation"}
        {"type": "box", "lines": ["Line1", "Line2"], "x": 4}
        {"type": "row", "boxes": [{"text": "A"}, {"text": "B"}], "merge": true, "spacing": 4}
        {"type": "line", "text": "───────►"}
        {"type": "arrow", "direction": "down", "length": 2}
        {"type": "text", "text": "Raw text", "comment": "Optional comment"}
        {"type": "spacer"}
        {"type": "bar_chart", "data": [("Label", 100), ("Label2", 50)], "bar_width": 20}
        {"type": "sparkline", "data": [1, 5, 3, 8, 4], "label": "Trend:"}
        {"type": "progress", "value": 75, "max": 100, "width": 20}
        {"type": "shaded_box", "width": 40, "height": 10, "palette": "blocks", "direction": "radial"}
        {"type": "table", "headers": ["A", "B"], "rows": [["1", "2"]], "box_style": "light"}

    Args:
        elements: List of element dictionaries
        width: Total diagram width
        frame: Wrap entire diagram in a dashed border

    Returns:
        Multi-line string of the diagram
    """
    # If frame is enabled, reduce inner width to account for border
    inner_width = width - 4 if frame else width
    result = []

    # Use index-based iteration for lookahead
    for idx, elem in enumerate(elements):
        t = elem.get("type", "text")
        x = elem.get("x", 0)  # indent
        indent = " " * x
        comment = elem.get("comment", "")
        comment_str = _ascii_comment(comment) if comment else ""

        if t == "title":
            for line in _ascii_title_box(elem["text"], inner_width):
                result.append(line)

        elif t == "row":
            # Horizontal row of boxes
            boxes = elem.get("boxes", [])
            merge = elem.get("merge", False)
            spacing = elem.get("spacing", 4)  # Default 4 spaces between boxes
            row_lines = _ascii_box_row(boxes, spacing=spacing, merge_bottom=merge)
            # Center the row
            if row_lines:
                row_width = len(row_lines[0])
                row_offset = (inner_width - row_width) // 2 if row_width < inner_width else 0
                row_indent = " " * row_offset
                for line in row_lines:
                    result.append(row_indent + line)

        elif t == "box":
            box_width = elem.get("width")
            lines = elem.get("lines", [elem.get("text", "")])

            # Check for explicit connector settings or auto-detect from context
            bottom_conn = elem.get("bottom_connector", False)
            top_conn = elem.get("top_connector", False)

            # Auto-detect: if next element is a down arrow, add bottom connector (arrow exits box)
            if idx + 1 < len(elements):
                next_elem = elements[idx + 1]
                if next_elem.get("type") == "arrow" and next_elem.get("direction", "down") == "down":
                    bottom_conn = True

            # NOTE: We don't auto-add top_connector - arrows point TO boxes without modifying them

            box_lines = _ascii_box(lines, box_width, bottom_connector=bottom_conn, top_connector=top_conn)
            middle_idx = len(box_lines) // 2  # Middle line (where content is)
            # Calculate centering: place box so its center aligns with inner_width // 2
            actual_box_width = len(box_lines[0]) if box_lines else 0
            center_pos = inner_width // 2
            center_offset = center_pos - actual_box_width // 2 if x == 0 and actual_box_width < inner_width else 0
            center_offset = max(0, center_offset)  # Ensure non-negative
            center_indent = " " * center_offset
            for i, line in enumerate(box_lines):
                full_line = (center_indent if x == 0 else indent) + line
                if comment_str and i == middle_idx:
                    full_line += comment_str
                result.append(full_line)

        elif t == "spacer":
            result.append("")

        elif t == "text":
            line = indent + elem.get("text", "")
            if comment_str:
                line += comment_str
            result.append(line)

        elif t == "arrow":
            direction = elem.get("direction", "down")
            length = elem.get("length", 1)  # Number of vertical line segments
            if direction in ("down", "up"):
                arrow_char = ASCII["arrow_d"] if direction == "down" else ASCII["arrow_u"]
                line_char = ASCII["v"]  # │
                # Center vertical arrows at inner_width // 2 (same as box center)
                if x == 0:
                    # No explicit x position - center at inner_width // 2
                    center_pos = inner_width // 2
                    arrow_indent = " " * center_pos
                    if direction == "down":
                        for _ in range(length):
                            result.append(arrow_indent + line_char)
                        result.append(arrow_indent + arrow_char)
                    else:  # up
                        result.append(arrow_indent + arrow_char)
                        for _ in range(length):
                            result.append(arrow_indent + line_char)
                else:
                    if direction == "down":
                        for _ in range(length):
                            result.append(indent + line_char)
                        result.append(indent + arrow_char)
                    else:  # up
                        result.append(indent + arrow_char)
                        for _ in range(length):
                            result.append(indent + line_char)
            else:
                result.append(indent + _ascii_arrow(direction, elem.get("length", 8)))

        elif t == "bar_chart":
            data = elem.get("data", [])
            bar_width = elem.get("bar_width", 20)
            show_values = elem.get("show_values", True)
            label_width = elem.get("label_width")
            chart_lines = _ascii_bar_chart(data, bar_width, show_values, label_width)
            for line in chart_lines:
                result.append(indent + line)

        elif t == "bar_chart_vertical":
            data = elem.get("data", [])
            bar_height = elem.get("bar_height", 10)
            bar_width = elem.get("bar_width", 5)
            show_values = elem.get("show_values", True)
            gap = elem.get("gap", 2)
            chart_lines = _ascii_bar_chart_vertical(data, bar_height, bar_width, show_values, gap)
            for line in chart_lines:
                result.append(indent + line)

        elif t == "sparkline":
            data = elem.get("data", [])
            label = elem.get("label", "")
            spark = _ascii_sparkline(data)
            line = indent + (f"{label} " if label else "") + spark
            if comment_str:
                line += comment_str
            result.append(line)

        elif t == "progress":
            value = elem.get("value", 0)
            max_val = elem.get("max", 100)
            bar_width = elem.get("width", 20)
            show_percent = elem.get("show_percent", True)
            label = elem.get("label", "")
            bar = _ascii_progress_bar(value, max_val, bar_width, show_percent)
            line = indent + (f"{label} " if label else "") + bar
            if comment_str:
                line += comment_str
            result.append(line)

        elif t == "shaded_box":
            box_width = elem.get("width", 40)
            box_height = elem.get("height", 10)
            title = elem.get("title")
            palette = elem.get("palette", "blocks")
            direction = elem.get("direction", "radial")
            contrast = elem.get("contrast", 0.7)
            box_style = elem.get("box_style", "light")
            shaded_lines = _ascii_shaded_box(box_width, box_height, title, palette, direction, contrast, box_style)
            for line in shaded_lines:
                result.append(indent + line)

        elif t == "table":
            headers = elem.get("headers", [])
            rows = elem.get("rows", [])
            box_style = elem.get("box_style", "light")
            table_lines = _ascii_table(headers, rows, box_style)
            for line in table_lines:
                result.append(indent + line)

    # Wrap in frame if requested
    if frame:
        result = _ascii_frame(result, width, padding=1, dashed=True)

    return "\n".join(result)


# =============================================================================
# INTERNAL UTILITIES
# =============================================================================

def _parse_color(color: str) -> Optional[Dict[str, float]]:
    """Parse a color string into RGB dict (0-1 scale)."""
    if not color:
        return None
    color = color.strip().lower()
    if color in COLORS:
        return COLORS[color]
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
    rgb_match = re.match(r'rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)', color)
    if rgb_match:
        return {
            "red": int(rgb_match.group(1)) / 255,
            "green": int(rgb_match.group(2)) / 255,
            "blue": int(rgb_match.group(3)) / 255
        }
    raise ValueError(f"Unknown color: {color}")


def _col_to_index(col: str) -> int:
    """Convert column letter(s) to 0-based index. A=0, B=1, Z=25, AA=26"""
    result = 0
    for char in col.upper():
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1


def _index_to_col(idx: int) -> str:
    """Convert 0-based index to column letter(s)."""
    col = ''
    idx += 1
    while idx > 0:
        idx, remainder = divmod(idx - 1, 26)
        col = chr(65 + remainder) + col
    return col


def _parse_a1(a1_range: str) -> Tuple[int, Optional[int], int, Optional[int]]:
    """Parse A1 notation to 0-based indexes."""
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


def _resolve_number_format(fmt: str) -> str:
    """Resolve a number format preset or return as-is."""
    return NUMBER_FORMATS.get(fmt.lower(), fmt) if fmt else fmt


def _resolve_style(style_name: str) -> Dict[str, Any]:
    """Resolve a style preset name to its properties."""
    return STYLES.get(style_name.lower(), {}) if style_name else {}


def _build_cell_format(
    bold: bool = None,
    italic: bool = None,
    underline: bool = None,
    strikethrough: bool = None,
    font_size: int = None,
    font_color: str = None,
    font_family: str = None,
    bg_color: str = None,
    h_align: str = None,
    v_align: str = None,
    wrap: str = None,
    number_format: str = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """Build cell format dict and fields list from parameters."""
    cell_format = {}
    text_format = {}
    fields = []

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
    if font_family:
        text_format["fontFamily"] = font_family

    if text_format:
        cell_format["textFormat"] = text_format
        fields.append("userEnteredFormat.textFormat")

    if bg_color:
        cell_format["backgroundColor"] = _parse_color(bg_color)
        fields.append("userEnteredFormat.backgroundColor")

    if h_align:
        cell_format["horizontalAlignment"] = {"left": "LEFT", "center": "CENTER", "right": "RIGHT"}.get(h_align.lower(), h_align.upper())
        fields.append("userEnteredFormat.horizontalAlignment")
    if v_align:
        cell_format["verticalAlignment"] = {"top": "TOP", "middle": "MIDDLE", "bottom": "BOTTOM"}.get(v_align.lower(), v_align.upper())
        fields.append("userEnteredFormat.verticalAlignment")
    if wrap:
        wrap_val = wrap if isinstance(wrap, str) else ("wrap" if wrap else "overflow")
        cell_format["wrapStrategy"] = {"overflow": "OVERFLOW_CELL", "clip": "CLIP", "wrap": "WRAP"}.get(wrap_val.lower(), wrap_val.upper())
        fields.append("userEnteredFormat.wrapStrategy")
    if number_format:
        resolved = _resolve_number_format(number_format)
        cell_format["numberFormat"] = {"type": "NUMBER", "pattern": resolved}
        fields.append("userEnteredFormat.numberFormat")

    return cell_format, fields


def _build_border(style: str = "solid", color: str = "black") -> Dict[str, Any]:
    """Build a border specification."""
    border_style = {"solid": "SOLID", "dashed": "DASHED", "dotted": "DOTTED", "double": "DOUBLE", "none": "NONE"}.get(style.lower(), "SOLID")
    return {"style": border_style, "color": _parse_color(color)}


def _parse_column_range(col_spec: str) -> List[int]:
    """Parse column specification like 'B', 'B:D', 'B,D,F' to list of indices."""
    indices = []
    for part in col_spec.replace(' ', '').split(','):
        if ':' in part:
            start, end = part.split(':')
            start_idx = _col_to_index(start)
            end_idx = _col_to_index(end)
            indices.extend(range(start_idx, end_idx + 1))
        else:
            indices.append(_col_to_index(part))
    return indices


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
        except Exception:
            pass

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
# TOOL 1: sheets_data - Read/Write/Search/Replace/Sort
# =============================================================================

@mcp.tool()
def sheets_data(
    spreadsheet_id: str,
    sheet: str,
    action: str,
    range: Optional[str] = None,
    # Read options
    include_formulas: bool = False,
    # Write options (also accepts string for diagram action)
    data: Optional[Union[List[List[Any]], str]] = None,
    style: Optional[str] = None,
    column_types: Optional[Dict[str, str]] = None,
    totals: Optional[List[str]] = None,  # Column names to add SUM totals for (native tables only)
    # Search options
    filters: Optional[List[Dict[str, str]]] = None,
    match_all: bool = True,
    include_header: bool = True,
    # Replace options
    find: Optional[str] = None,
    replace_with: Optional[str] = None,
    use_regex: bool = False,
    match_case: bool = False,
    # Sort options
    sort_by: Optional[List[Dict[str, str]]] = None,
    # Diagram options (builds diagram with proper alignment via helpers)
    elements: Optional[List[Dict[str, Any]]] = None,
    width: int = 60,
    frame: bool = False,  # Wrap diagram in dashed border
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Unified data operations for Google Sheets.

    Actions:
        read: Get cell values (add include_formulas=True for formulas)
        write: Write data with optional table styling and column type formatting
        clear: Clear cell contents (and optionally formatting)
        search: Filter/query rows with conditions
        replace: Find and replace text across the sheet
        sort: Sort a range by specified columns
        diagram: Create ASCII art/text diagrams with monospace font (use style="clean" to hide gridlines)

    Table styles:
        Native tables (auto-expand, filters): "table" (default green), "table_green", "table_gray", "table_red"
        Styled ranges (no auto-expand): "basic", "basic_striped", "basic_bordered"

    IMPORTANT - When to use native tables vs no styling:
        - DOCUMENT/REPORT MODE: When writing multiple sections, mixed content, or demonstrating results
          with several tables in ONE call → Do NOT use style parameter. Just write raw data.
          After writing, use sheets_format to bold section headers and table headers.
          Add =SUM(range) or =AVERAGE(range) formulas inline for totals.
        - SINGLE TABLE MODE: When creating ONE structured table with consistent columns where the
          FIRST ROW IS THE HEADER → Use style="table". First row MUST contain column headers.

    Column types: {"B": "currency", "C:E": "percent", "F": "date"} - see NUMBER_FORMATS for options

    Examples:
        # Read data
        sheets_data(id, "Sheet1", "read", "A1:D10")

        # DOCUMENT MODE - Multiple sections, no table styling (just raw data)
        sheets_data(id, "Sheet1", "write", "A1", data=[
            ["REPORT TITLE", "", ""],
            ["", "", ""],
            ["Section 1", "", ""],
            ["Item", "Value", "Note"],
            ["A", 100, "..."],
            ["B", 200, "..."],
            ["Total", "=SUM(B4:B5)", ""],  # Inline SUM formula for totals
            ["", "", ""],
            ["Section 2", "", ""],
            ...
        ])  # No style parameter!

        # SINGLE TABLE MODE - One structured table with headers in first row
        sheets_data(id, "Sheet1", "write", "A1", data=[["Name","Sales"],["John",1000]], style="table", column_types={"B": "currency"})

        # SINGLE TABLE with totals row - adds SUM formulas using structured references
        sheets_data(id, "Sheet1", "write", "A1", data=[["Product","Qty","Price"],["A",10,100],["B",20,200]],
                   style="table", totals=["Qty", "Price"])  # Adds row with =SUM(TableName[Qty]), =SUM(TableName[Price])

        # Write with basic styled range (no filters, no auto-expand)
        sheets_data(id, "Sheet1", "write", "A1", data=[["Name","Sales"],["John",1000]], style="basic_striped")

        # Search/filter rows
        sheets_data(id, "Sheet1", "search", filters=[{"column": "Status", "op": "equals", "value": "Active"}])

        # Find and replace
        sheets_data(id, "Sheet1", "replace", find="old", replace_with="new")

        # Sort by column
        sheets_data(id, "Sheet1", "sort", "A1:D100", sort_by=[{"column": "B", "order": "desc"}])

        # Create ASCII diagram with elements (auto-calculated alignment)
        sheets_data(id, "Sheet1", "diagram", "A1", style="clean", width=60, elements=[
            {"type": "title", "text": "MY SYSTEM"},
            {"type": "box", "text": "API", "x": 20, "comment": "Main service"},
        ])

        # Or with raw string (for simple/pre-built diagrams)
        sheets_data(id, "Sheet1", "diagram", "A1", data="┌───┐\\n│ X │\\n└───┘", style="clean")

        # Create diagram with bar chart (uses █▉▊▋▌▍▎▏ block elements)
        sheets_data(id, "Sheet1", "diagram", "A1", style="clean", width=60, elements=[
            {"type": "title", "text": "SALES REPORT"},
            {"type": "spacer"},
            {"type": "bar_chart", "data": [["Q1", 100], ["Q2", 150], ["Q3", 120]], "bar_width": 25},
        ])

        # Create diagram with sparkline (uses ▁▂▃▄▅▆▇█ characters)
        sheets_data(id, "Sheet1", "diagram", "A1", style="clean", elements=[
            {"type": "text", "text": "Weekly trend:"},
            {"type": "sparkline", "data": [10, 15, 12, 18, 14, 20, 16], "label": "Views"},
        ])

        # Create diagram with progress bar
        sheets_data(id, "Sheet1", "diagram", "A1", style="clean", elements=[
            {"type": "progress", "value": 75, "max": 100, "width": 20, "label": "Complete:"},
        ])

        # Create diagram with shaded box (gradient fill)
        # Palettes: ascii, blocks, dots, density, braille
        # Directions: horizontal, vertical, radial, diagonal
        # Box styles: light, heavy, double, rounded
        sheets_data(id, "Sheet1", "diagram", "A1", style="clean", elements=[
            {"type": "shaded_box", "width": 50, "height": 10, "title": "Status", "palette": "blocks", "direction": "radial", "box_style": "double"},
        ])

        # Create diagram with bordered table
        sheets_data(id, "Sheet1", "diagram", "A1", style="clean", elements=[
            {"type": "table", "headers": ["Task", "Status", "Progress"], "rows": [["Deploy", "✓", "100%"], ["Test", "⏳", "65%"]], "box_style": "heavy"},
        ])
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    action = action.lower()

    # === READ ===
    if action == "read":
        full_range = f"{sheet}!{range}" if range else sheet
        if include_formulas:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=full_range, valueRenderOption='FORMULA'
            ).execute()
        else:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=full_range
            ).execute()
        return {'range': full_range, 'values': result.get('values', [])}

    # === WRITE ===
    elif action == "write":
        if not data:
            return {"error": "data is required for write action"}
        if not range:
            return {"error": "range is required for write action"}

        results = {}
        full_range = f"{sheet}!{range}"

        # Write the data
        write_result = sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=full_range,
            valueInputOption='USER_ENTERED', body={'values': data}
        ).execute()
        results['write'] = write_result

        # Apply table style and column types if specified
        if style or column_types:
            sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
            requests = []

            # Parse range for data bounds
            start_row, _, start_col, _ = _parse_a1(range)
            num_rows = len(data)
            num_cols = len(data[0]) if data else 0
            end_row = start_row + num_rows
            end_col = start_col + num_cols

            # Apply table style
            if style and style.lower() in TABLE_STYLES:
                ts = TABLE_STYLES[style.lower()]
                grid = {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                        "startColumnIndex": start_col, "endColumnIndex": end_col}

                # Check if this is a native table style (uses addTable API)
                use_native = ts.get('native', False)

                if use_native:
                    # Validate that data has proper columnar structure for native table
                    # Build column properties from header row (first row of data)
                    header_row = data[0] if data else []

                    # Count non-empty header cells
                    non_empty_headers = sum(1 for h in header_row if h and str(h).strip())
                    total_cols = len(header_row)

                    # Native tables require at least 2 columns with defined headers
                    # and more than half of headers should be non-empty
                    if not (total_cols >= 2 and
                            non_empty_headers >= 2 and
                            non_empty_headers > total_cols // 2):
                        # Fall back to basic styling when data isn't proper tabular format
                        use_native = False
                        ts = TABLE_STYLES.get('basic', {})

                if use_native:
                    header_row = data[0] if data else []
                    column_properties = []
                    for i, col_name in enumerate(header_row):
                        column_properties.append({
                            "columnIndex": i,
                            "columnName": str(col_name) if col_name else f"Column{i+1}"
                        })

                    # Generate unique table name (sanitize sheet name for valid references)
                    safe_sheet = sheet.replace(' ', '_').replace('-', '_')
                    table_name = f"Table_{safe_sheet}_{int(time.time())}"

                    # Build table spec
                    table_spec = {
                        "name": table_name,
                        "range": grid,
                        "columnProperties": column_properties
                    }

                    # Add custom colors if specified
                    if 'header_color' in ts:
                        header_color = _parse_color(ts['header_color'])
                        band_color = ts.get('band_color', {"red": 1, "green": 1, "blue": 1})
                        table_spec["rowsProperties"] = {
                            "headerColorStyle": {"rgbColor": header_color},
                            "firstBandColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}},
                            "secondBandColorStyle": {"rgbColor": band_color}
                        }

                    # Add native table (like Format > Convert to Table)
                    requests.append({"addTable": {"table": table_spec}})

                    # Auto-resize columns
                    requests.append({"autoResizeDimensions": {"dimensions": {
                        "sheetId": sheet_id, "dimension": "COLUMNS",
                        "startIndex": start_col, "endIndex": end_col
                    }}})

                    results['native_table'] = True
                    results['table_name'] = table_name

                    # Store totals info for later (after table is created)
                    if totals and header_row:
                        results['_pending_totals'] = {
                            'table_name': table_name,
                            'header_row': header_row,
                            'totals': totals,
                            'totals_row': end_row,
                            'start_col': start_col,
                            'end_col': end_col
                        }

                else:
                    # Legacy/basic table styling (manual formatting)
                    # Header formatting
                    if 'header' in ts and num_rows > 0:
                        hdr_format, hdr_fields = _build_cell_format(**ts['header'])
                        if hdr_format:
                            requests.append({
                                "repeatCell": {
                                    "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": start_row + 1,
                                              "startColumnIndex": start_col, "endColumnIndex": end_col},
                                    "cell": {"userEnteredFormat": hdr_format},
                                    "fields": ",".join(hdr_fields)
                                }
                            })

                    # Header border
                    if 'header_border' in ts:
                        hb = ts['header_border']
                        border = _build_border(hb.get('style', 'solid'), hb.get('color', 'black'))
                        border_req = {"range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": start_row + 1,
                                                "startColumnIndex": start_col, "endColumnIndex": end_col}}
                        sides = hb.get('sides', 'bottom')
                        if sides == 'bottom':
                            border_req['bottom'] = border
                        elif sides == 'all':
                            border_req.update({"top": border, "bottom": border, "left": border, "right": border})
                        requests.append({"updateBorders": border_req})

                    # Body border
                    if 'body_border' in ts:
                        bb = ts['body_border']
                        border = _build_border(bb.get('style', 'solid'), bb.get('color', 'light_gray'))
                        border_req = {"range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                                                "startColumnIndex": start_col, "endColumnIndex": end_col}}
                        sides = bb.get('sides', 'outer')
                        if sides == 'outer':
                            border_req.update({"top": border, "bottom": border, "left": border, "right": border})
                        elif sides == 'all':
                            border_req.update({"top": border, "bottom": border, "left": border, "right": border,
                                               "innerHorizontal": border, "innerVertical": border})
                        requests.append({"updateBorders": border_req})

                    # Striped rows
                    if 'odd_rows' in ts and num_rows > 1:
                        stripe_format, stripe_fields = _build_cell_format(**ts['odd_rows'])
                        for row_idx in range(start_row + 2, end_row, 2):  # Every other row after header
                            requests.append({
                                "repeatCell": {
                                    "range": {"sheetId": sheet_id, "startRowIndex": row_idx, "endRowIndex": row_idx + 1,
                                              "startColumnIndex": start_col, "endColumnIndex": end_col},
                                    "cell": {"userEnteredFormat": stripe_format},
                                    "fields": ",".join(stripe_fields)
                                }
                            })

                    # Auto-resize
                    if ts.get('auto_resize'):
                        requests.append({"autoResizeDimensions": {"dimensions": {
                            "sheetId": sheet_id, "dimension": "COLUMNS",
                            "startIndex": start_col, "endIndex": end_col
                        }}})

            # Apply column types
            if column_types:
                for col_spec, fmt in column_types.items():
                    col_indices = _parse_column_range(col_spec)
                    resolved_fmt = _resolve_number_format(fmt)
                    for col_idx in col_indices:
                        abs_col = start_col + col_idx if col_idx < num_cols else col_idx
                        requests.append({
                            "repeatCell": {
                                "range": {"sheetId": sheet_id, "startRowIndex": start_row + 1, "endRowIndex": end_row,
                                          "startColumnIndex": abs_col, "endColumnIndex": abs_col + 1},
                                "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": resolved_fmt}}},
                                "fields": "userEnteredFormat.numberFormat"
                            }
                        })

            if requests:
                format_result = sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id, body={"requests": requests}
                ).execute()
                results['format'] = {'requests_executed': len(requests)}

            # Add totals row AFTER table is created (so structured references work)
            if '_pending_totals' in results:
                pt = results.pop('_pending_totals')
                table_name = pt['table_name']
                header_row = pt['header_row']
                totals_cols = pt['totals']
                totals_row = pt['totals_row']
                t_start_col = pt['start_col']
                t_end_col = pt['end_col']

                # Build totals row - map column names to formulas
                totals_data = []
                for col_name in header_row:
                    col_str = str(col_name) if col_name else ""
                    if col_str in totals_cols:
                        # Use structured reference: =SUM(TableName[ColumnName])
                        totals_data.append(f'=SUM({table_name}[{col_str}])')
                    else:
                        totals_data.append("")  # Empty for non-total columns

                # First cell can have "Total" label if first column isn't being summed
                if totals_data and not totals_data[0]:
                    totals_data[0] = "Total"

                # Write totals row below the table
                totals_range = f"{sheet}!{_index_to_col(t_start_col)}{totals_row + 1}"
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id, range=totals_range,
                    valueInputOption="USER_ENTERED", body={"values": [totals_data]}
                ).execute()

                # Bold the totals row
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id, body={"requests": [{
                        "repeatCell": {
                            "range": {"sheetId": sheet_id, "startRowIndex": totals_row, "endRowIndex": totals_row + 1,
                                      "startColumnIndex": t_start_col, "endColumnIndex": t_end_col},
                            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                            "fields": "userEnteredFormat.textFormat.bold"
                        }
                    }]}
                ).execute()
                results['totals_row'] = totals_row + 1

        return results

    # === CLEAR ===
    elif action == "clear":
        if not range:
            return {"error": "range is required for clear action"}
        full_range = f"{sheet}!{range}"
        result = sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=full_range, body={}
        ).execute()
        return {'cleared': full_range, 'result': result}

    # === SEARCH ===
    elif action == "search":
        full_range = f"{sheet}!{range}" if range else sheet
        result = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=full_range).execute()
        rows = result.get('values', [])

        if not rows or not filters:
            return {'filtered_rows': rows if include_header else rows[1:], 'total_rows': len(rows), 'matched_rows': len(rows) - 1}

        header = rows[0] if rows else []

        # Resolve column references
        resolved_filters = []
        for f in filters:
            col = f.get('column', '')
            col_idx = -1
            if len(col) <= 3 and col.isalpha():
                col_idx = _col_to_index(col.upper())
            else:
                for i, h in enumerate(header):
                    if str(h).lower() == col.lower():
                        col_idx = i
                        break
            if col_idx == -1:
                return {'error': f'Column "{col}" not found'}
            resolved_filters.append({'col_idx': col_idx, 'op': f.get('op', 'equals'), 'value': f.get('value', '')})

        def check_condition(row, flt):
            col_idx, op, value = flt['col_idx'], flt['op'], flt['value']
            cell_val = str(row[col_idx]).strip() if col_idx < len(row) and row[col_idx] else ''
            val = str(value).strip() if value else ''
            cell_lower, val_lower = cell_val.lower(), val.lower()

            if op == 'equals': return cell_lower == val_lower
            elif op == 'not_equals': return cell_lower != val_lower
            elif op == 'contains': return val_lower in cell_lower
            elif op == 'not_contains': return val_lower not in cell_lower
            elif op == 'starts_with': return cell_lower.startswith(val_lower)
            elif op == 'ends_with': return cell_lower.endswith(val_lower)
            elif op == 'empty': return cell_val == ''
            elif op == 'not_empty': return cell_val != ''
            elif op == 'regex': return bool(re.search(val, cell_val, re.IGNORECASE))
            elif op in ('gt', 'gte', 'lt', 'lte'):
                try:
                    cell_num, val_num = float(cell_val.replace(',', '')), float(val.replace(',', ''))
                    if op == 'gt': return cell_num > val_num
                    elif op == 'gte': return cell_num >= val_num
                    elif op == 'lt': return cell_num < val_num
                    elif op == 'lte': return cell_num <= val_num
                except: return False
            return False

        def row_matches(row):
            if match_all:
                return all(check_condition(row, f) for f in resolved_filters)
            return any(check_condition(row, f) for f in resolved_filters)

        filtered = ([header] if include_header else []) + [r for r in rows[1:] if row_matches(r)]
        return {'filtered_rows': filtered, 'total_rows': len(rows), 'matched_rows': len(filtered) - (1 if include_header else 0)}

    # === REPLACE ===
    elif action == "replace":
        if not find:
            return {"error": "find is required for replace action"}
        sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
        request = {
            "findReplace": {
                "find": find,
                "replacement": replace_with or "",
                "matchCase": match_case,
                "searchByRegex": use_regex,
                "sheetId": sheet_id
            }
        }
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()
        replies = result.get('replies', [])
        if replies and 'findReplace' in replies[0]:
            fr = replies[0]['findReplace']
            return {'occurrences_changed': fr.get('occurrencesChanged', 0), 'values_changed': fr.get('valuesChanged', 0)}
        return {'occurrences_changed': 0}

    # === SORT ===
    elif action == "sort":
        if not range:
            return {"error": "range is required for sort action"}
        if not sort_by:
            return {"error": "sort_by is required for sort action"}

        sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
        start_row, end_row, start_col, end_col = _parse_a1(range)

        # Build sort specs
        sort_specs = []
        for spec in sort_by:
            col = spec.get('column', 'A')
            col_idx = _col_to_index(col) if col.isalpha() else int(col)
            order = spec.get('order', 'asc').upper()
            sort_specs.append({
                "dimensionIndex": col_idx,
                "sortOrder": "DESCENDING" if order == "DESC" else "ASCENDING"
            })

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"sortRange": {
                "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                          "startColumnIndex": start_col, "endColumnIndex": end_col},
                "sortSpecs": sort_specs
            }}]}
        ).execute()
        return {'sorted': f"{sheet}!{range}", 'sort_specs': sort_specs}

    # === DIAGRAM (ASCII art / text diagrams) ===
    elif action == "diagram":
        if not data and not elements:
            return {"error": "Either data (string) or elements (list) is required for diagram action"}

        sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)

        # Parse starting position
        start_row, _, start_col, _ = _parse_a1(range) if range else (0, None, 0, None)

        # Build diagram from elements (uses helpers for proper alignment)
        if elements:
            diagram_text = _ascii_diagram(elements, width, frame=frame)
            lines = diagram_text.split('\n')
        # Or use raw data as-is
        elif isinstance(data, str):
            lines = data.split('\n')
        elif isinstance(data, list) and all(isinstance(item, str) for item in data):
            lines = data
        elif isinstance(data, list) and all(isinstance(item, list) for item in data):
            lines = [row[0] if row else '' for row in data]
        else:
            return {"error": "data must be a multi-line string or list of strings"}

        # Write each line as a row (single column)
        values = [[line] for line in lines]
        write_range = f"{sheet}!{_index_to_col(start_col)}{start_row + 1}"
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=write_range,
            valueInputOption="RAW", body={"values": values}
        ).execute()

        num_rows = len(lines)
        max_line_length = max(len(line) for line in lines) if lines else 0

        # Build formatting requests
        requests = []

        # Apply monospace font formatting
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": start_row + num_rows,
                          "startColumnIndex": start_col, "endColumnIndex": start_col + 1},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"fontFamily": "Courier New", "fontSize": 10},
                    "backgroundColor": {"red": 1, "green": 1, "blue": 1},
                    "wrapStrategy": "CLIP"
                }},
                "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor,userEnteredFormat.wrapStrategy"
            }
        })

        # Set column width for monospace font (Courier New 10pt ≈ 8px per char)
        col_width = max(150, max_line_length * 8 + 16)
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": start_col, "endIndex": start_col + 1},
                "properties": {"pixelSize": col_width},
                "fields": "pixelSize"
            }
        })

        # Set row height for consistent spacing (18 pixels works well for 10pt font)
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": start_row, "endIndex": start_row + num_rows},
                "properties": {"pixelSize": 18},
                "fields": "pixelSize"
            }
        })

        # Optionally hide gridlines (if style contains 'clean' or hide_gridlines flag)
        hide_gridlines = style and 'clean' in style.lower() if style else False
        if hide_gridlines:
            requests.append({
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id, "gridProperties": {"hideGridlines": True}},
                    "fields": "gridProperties.hideGridlines"
                }
            })

        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()

        return {
            'diagram_created': f"{sheet}!{_index_to_col(start_col)}{start_row + 1}:{_index_to_col(start_col)}{start_row + num_rows}",
            'lines': num_rows,
            'max_width': max_line_length,
            'gridlines_hidden': hide_gridlines
        }

    return {"error": f"Unknown action: {action}"}


# =============================================================================
# TOOL 2: sheets_format - Style/Border/Merge/Conditional/Clear
# =============================================================================

@mcp.tool()
def sheets_format(
    spreadsheet_id: str,
    sheet: str,
    action: str,
    range: str,
    # Style options
    style: Optional[str] = None,
    bold: Optional[bool] = None,
    italic: Optional[bool] = None,
    underline: Optional[bool] = None,
    strikethrough: Optional[bool] = None,
    font_size: Optional[int] = None,
    font_color: Optional[str] = None,
    font_family: Optional[str] = None,
    bg_color: Optional[str] = None,
    align: Optional[str] = None,
    valign: Optional[str] = None,
    wrap: Optional[str] = None,
    number_format: Optional[str] = None,
    # Border options
    border_style: str = "solid",
    border_color: str = "black",
    border_sides: str = "all",
    # Conditional format options
    rule: Optional[str] = None,
    condition: Optional[Dict[str, Any]] = None,
    # Batch mode
    formats: Optional[List[Dict[str, Any]]] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Unified formatting operations for Google Sheets.

    Actions:
        style: Apply text/cell formatting (use presets like "h1", "header", "success" or explicit params)
        border: Set cell borders
        merge: Merge cells in range
        unmerge: Unmerge cells in range
        conditional: Apply conditional formatting rules
        clear: Clear all formatting (keeps values)

    Style presets: h1, h2, h3, title, subtitle, header, bold, italic, center, right, success, error, warning, muted, highlight, code

    Number format presets: currency, percent, date, number, decimal, accounting, etc.

    Border sides: "all", "outer", "inner", "top", "bottom", "left", "right" (comma-separated)

    Conditional rules: "color_scale", "data_bar", "greater_than", "less_than", "between", "text_contains", "custom"

    Examples:
        # Apply header style
        sheets_format(id, "Sheet1", "style", "A1:F1", style="h1")

        # Custom formatting
        sheets_format(id, "Sheet1", "style", "B2:B100", bold=True, font_color="green", number_format="currency")

        # Add borders
        sheets_format(id, "Sheet1", "border", "A1:F10", border_style="solid", border_sides="outer")

        # Merge cells
        sheets_format(id, "Sheet1", "merge", "A1:C1")

        # Conditional formatting - color scale
        sheets_format(id, "Sheet1", "conditional", "B2:B100", rule="color_scale", condition={"min_color": "red", "max_color": "green"})

        # Batch formatting
        sheets_format(id, "Sheet1", "style", "A1", formats=[{"range": "A1:F1", "style": "header"}, {"range": "B2:B100", "number_format": "currency"}])
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
    action = action.lower()

    # === STYLE ===
    if action == "style":
        requests = []

        # Batch mode
        if formats:
            for fmt in formats:
                fmt_range = fmt.get('range', range)
                # Merge preset style with explicit params
                merged = {}
                if fmt.get('style'):
                    merged.update(_resolve_style(fmt['style']))
                for k in ['bold', 'italic', 'underline', 'strikethrough', 'font_size', 'font_color', 'font_family', 'bg_color', 'h_align', 'v_align', 'wrap', 'number_format']:
                    if fmt.get(k) is not None:
                        merged[k] = fmt[k]

                cell_format, fields = _build_cell_format(**merged)
                if cell_format:
                    requests.append({
                        "repeatCell": {
                            "range": _grid_range(sheet_id, fmt_range),
                            "cell": {"userEnteredFormat": cell_format},
                            "fields": ",".join(fields)
                        }
                    })
        else:
            # Single range mode - merge preset with explicit params
            merged = {}
            if style:
                merged.update(_resolve_style(style))
            for k, v in [('bold', bold), ('italic', italic), ('underline', underline), ('strikethrough', strikethrough),
                         ('font_size', font_size), ('font_color', font_color), ('font_family', font_family), ('bg_color', bg_color),
                         ('h_align', align), ('v_align', valign), ('wrap', wrap), ('number_format', number_format)]:
                if v is not None:
                    merged[k] = v

            cell_format, fields = _build_cell_format(**merged)
            if cell_format:
                requests.append({
                    "repeatCell": {
                        "range": _grid_range(sheet_id, range),
                        "cell": {"userEnteredFormat": cell_format},
                        "fields": ",".join(fields)
                    }
                })

        if not requests:
            return {"error": "No formatting options specified"}

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()
        return {'formatted': range, 'requests_executed': len(requests)}

    # === BORDER ===
    elif action == "border":
        border = _build_border(border_style, border_color)
        req = {"range": _grid_range(sheet_id, range)}
        sides = border_sides.lower()

        if sides == "all":
            req.update({"top": border, "bottom": border, "left": border, "right": border, "innerHorizontal": border, "innerVertical": border})
        elif sides == "outer":
            req.update({"top": border, "bottom": border, "left": border, "right": border})
        elif sides == "inner":
            req.update({"innerHorizontal": border, "innerVertical": border})
        else:
            for side in sides.split(","):
                side = side.strip()
                if side in ["top", "bottom", "left", "right"]:
                    req[side] = border

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [{"updateBorders": req}]}
        ).execute()
        return {'bordered': range, 'style': border_style, 'sides': border_sides}

    # === MERGE ===
    elif action == "merge":
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"mergeCells": {"range": _grid_range(sheet_id, range), "mergeType": "MERGE_ALL"}}]}
        ).execute()
        return {'merged': range}

    # === UNMERGE ===
    elif action == "unmerge":
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"unmergeCells": {"range": _grid_range(sheet_id, range)}}]}
        ).execute()
        return {'unmerged': range}

    # === CONDITIONAL ===
    elif action == "conditional":
        if not rule:
            return {"error": "rule is required for conditional formatting"}

        condition = condition or {}
        grid = _grid_range(sheet_id, range)
        requests = []

        if rule == "color_scale":
            min_color = _parse_color(condition.get('min_color', 'red'))
            mid_color = _parse_color(condition.get('mid_color')) if condition.get('mid_color') else None
            max_color = _parse_color(condition.get('max_color', 'green'))

            gradient = {
                "minpoint": {"color": min_color, "type": "MIN"},
                "maxpoint": {"color": max_color, "type": "MAX"}
            }
            if mid_color:
                gradient["midpoint"] = {"color": mid_color, "type": "PERCENTILE", "value": "50"}

            requests.append({
                "addConditionalFormatRule": {
                    "rule": {"ranges": [grid], "gradientRule": gradient},
                    "index": 0
                }
            })

        elif rule == "data_bar":
            bar_color = _parse_color(condition.get('color', 'blue'))
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [grid],
                        "gradientRule": {
                            "minpoint": {"color": {"red": 1, "green": 1, "blue": 1}, "type": "MIN"},
                            "maxpoint": {"color": bar_color, "type": "MAX"}
                        }
                    },
                    "index": 0
                }
            })

        elif rule in ("greater_than", "less_than", "between", "text_contains"):
            fmt = {"backgroundColor": _parse_color(condition.get('bg_color', 'light_yellow'))}
            if condition.get('font_color'):
                fmt["textFormat"] = {"foregroundColor": _parse_color(condition['font_color'])}

            bool_condition = {}
            if rule == "greater_than":
                bool_condition = {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": str(condition.get('value', 0))}]}
            elif rule == "less_than":
                bool_condition = {"type": "NUMBER_LESS", "values": [{"userEnteredValue": str(condition.get('value', 0))}]}
            elif rule == "between":
                bool_condition = {"type": "NUMBER_BETWEEN", "values": [
                    {"userEnteredValue": str(condition.get('min', 0))},
                    {"userEnteredValue": str(condition.get('max', 100))}
                ]}
            elif rule == "text_contains":
                bool_condition = {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": str(condition.get('text', ''))}]}

            requests.append({
                "addConditionalFormatRule": {
                    "rule": {"ranges": [grid], "booleanRule": {"condition": bool_condition, "format": fmt}},
                    "index": 0
                }
            })

        elif rule == "custom":
            # Custom formula-based rule
            formula = condition.get('formula', '')
            fmt = {"backgroundColor": _parse_color(condition.get('bg_color', 'light_yellow'))}
            if condition.get('font_color'):
                fmt["textFormat"] = {"foregroundColor": _parse_color(condition['font_color'])}

            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [grid],
                        "booleanRule": {
                            "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]},
                            "format": fmt
                        }
                    },
                    "index": 0
                }
            })

        if not requests:
            return {"error": f"Unknown conditional rule: {rule}"}

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()
        return {'conditional_format_applied': range, 'rule': rule}

    # === CLEAR ===
    elif action == "clear":
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"repeatCell": {
                "range": _grid_range(sheet_id, range),
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat"
            }}]}
        ).execute()
        return {'formatting_cleared': range}

    return {"error": f"Unknown action: {action}"}


# =============================================================================
# TOOL 3: sheets_structure - Resize/Freeze/Rows/Cols/Validate
# =============================================================================

@mcp.tool()
def sheets_structure(
    spreadsheet_id: str,
    sheet: str,
    action: str,
    # Resize options
    columns: Optional[str] = None,
    rows: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    auto: bool = True,
    # Freeze options
    freeze_rows: int = 0,
    freeze_cols: int = 0,
    # Row/Column modification
    start: Optional[Union[int, str]] = None,
    end: Optional[Union[int, str]] = None,
    count: int = 1,
    # Validation options
    range: Optional[str] = None,
    validation: Optional[str] = None,
    options: Optional[List[str]] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    custom_formula: Optional[str] = None,
    allow_invalid: bool = False,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Structural operations for Google Sheets.

    Actions:
        resize: Auto-resize or set explicit width/height for columns/rows
        freeze: Freeze rows and/or columns
        add_rows: Insert new rows
        add_cols: Insert new columns
        delete_rows: Delete rows
        delete_cols: Delete columns
        table: Convert range to native Google Sheets Table (Format > Convert to Table)
        validate: Add data validation (dropdown, number range, date, checkbox, custom)

    Table styles (via validation param): "table" (default), "table_green", "table_gray", "table_red"
    Or pass custom table name: validation="MyTableName"

    IMPORTANT - table action requirements:
        - First row of range MUST be column headers (not a title or merged content)
        - At least 2 columns with defined headers required
        - Use for SINGLE structured datasets only, not for document-style layouts

    Examples:
        # Auto-resize all columns
        sheets_structure(id, "Sheet1", "resize", columns="all")

        # Set specific column width
        sheets_structure(id, "Sheet1", "resize", columns="A:C", width=150, auto=False)

        # Freeze header row
        sheets_structure(id, "Sheet1", "freeze", freeze_rows=1)

        # Insert 5 rows at row 10
        sheets_structure(id, "Sheet1", "add_rows", start=10, count=5)

        # Delete columns B through D
        sheets_structure(id, "Sheet1", "delete_cols", start="B", end="D")

        # Convert existing range to native table (like Format > Convert to Table)
        sheets_structure(id, "Sheet1", "table", range="A1:F100")

        # Convert to table with gray color theme
        sheets_structure(id, "Sheet1", "table", range="A1:F100", validation="table_gray")

        # Convert to table with custom name
        sheets_structure(id, "Sheet1", "table", range="A1:F100", validation="SalesData")

        # Add dropdown validation
        sheets_structure(id, "Sheet1", "validate", range="D2:D100", validation="dropdown", options=["Active", "Inactive", "Pending"])

        # Add number validation
        sheets_structure(id, "Sheet1", "validate", range="E2:E100", validation="number", min_value=0, max_value=100)

        # Add checkbox
        sheets_structure(id, "Sheet1", "validate", range="F2:F100", validation="checkbox")
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
    action = action.lower()

    # === RESIZE ===
    if action == "resize":
        requests = []

        if columns:
            if auto and not width:
                # Auto-resize columns
                if columns.lower() == "all":
                    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                    for s in spreadsheet['sheets']:
                        if s['properties']['sheetId'] == sheet_id:
                            col_count = s['properties'].get('gridProperties', {}).get('columnCount', 26)
                            requests.append({"autoResizeDimensions": {"dimensions": {
                                "sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": col_count
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
                    elif columns.isalpha():
                        col_idx = _col_to_index(columns)
                        requests.append({"autoResizeDimensions": {"dimensions": {
                            "sheetId": sheet_id, "dimension": "COLUMNS",
                            "startIndex": col_idx, "endIndex": col_idx + 1
                        }}})
            elif width:
                # Set explicit width
                if columns.lower() == "all":
                    return {"error": "Cannot set explicit width for 'all' columns"}
                col_match = re.match(r'^([A-Za-z]+):([A-Za-z]+)$', columns)
                if col_match:
                    start_idx, end_idx = _col_to_index(col_match.group(1)), _col_to_index(col_match.group(2)) + 1
                else:
                    start_idx = _col_to_index(columns)
                    end_idx = start_idx + 1
                requests.append({"updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": start_idx, "endIndex": end_idx},
                    "properties": {"pixelSize": width}, "fields": "pixelSize"
                }})

        if rows:
            if auto and not height:
                # Auto-resize rows
                if rows.lower() == "all":
                    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                    for s in spreadsheet['sheets']:
                        if s['properties']['sheetId'] == sheet_id:
                            row_count = s['properties'].get('gridProperties', {}).get('rowCount', 1000)
                            requests.append({"autoResizeDimensions": {"dimensions": {
                                "sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": row_count
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
            elif height:
                # Set explicit height
                row_match = re.match(r'^(\d+):(\d+)$', rows)
                if row_match:
                    start_idx, end_idx = int(row_match.group(1)) - 1, int(row_match.group(2))
                else:
                    start_idx = int(rows) - 1
                    end_idx = start_idx + 1
                requests.append({"updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": start_idx, "endIndex": end_idx},
                    "properties": {"pixelSize": height}, "fields": "pixelSize"
                }})

        if not requests:
            return {"error": "Specify columns and/or rows to resize"}

        result = sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()
        return {'resized': {'columns': columns, 'rows': rows, 'width': width, 'height': height}}

    # === FREEZE ===
    elif action == "freeze":
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": freeze_rows, "frozenColumnCount": freeze_cols}},
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
            }}]}
        ).execute()
        return {'frozen': {'rows': freeze_rows, 'columns': freeze_cols}}

    # === ADD_ROWS ===
    elif action == "add_rows":
        start_idx = (int(start) - 1) if start else 0
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"insertDimension": {"range": {
                "sheetId": sheet_id, "dimension": "ROWS", "startIndex": start_idx, "endIndex": start_idx + count
            }, "inheritFromBefore": start_idx > 0}}]}
        ).execute()
        return {'added_rows': count, 'at_row': start_idx + 1}

    # === ADD_COLS ===
    elif action == "add_cols":
        start_idx = _col_to_index(str(start)) if start else 0
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"insertDimension": {"range": {
                "sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": start_idx, "endIndex": start_idx + count
            }, "inheritFromBefore": start_idx > 0}}]}
        ).execute()
        return {'added_columns': count, 'at_column': _index_to_col(start_idx)}

    # === DELETE_ROWS ===
    elif action == "delete_rows":
        start_idx = int(start) - 1 if start else 0
        end_idx = int(end) if end else start_idx + count
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"deleteDimension": {"range": {
                "sheetId": sheet_id, "dimension": "ROWS", "startIndex": start_idx, "endIndex": end_idx
            }}}]}
        ).execute()
        return {'deleted_rows': f"{start_idx + 1}:{end_idx}"}

    # === DELETE_COLS ===
    elif action == "delete_cols":
        start_idx = _col_to_index(str(start)) if start else 0
        end_idx = _col_to_index(str(end)) + 1 if end else start_idx + count
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"deleteDimension": {"range": {
                "sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": start_idx, "endIndex": end_idx
            }}}]}
        ).execute()
        return {'deleted_columns': f"{_index_to_col(start_idx)}:{_index_to_col(end_idx - 1)}"}

    # === TABLE (Convert to Native Google Sheets Table) ===
    elif action == "table":
        if not range:
            return {"error": "range is required for table action"}

        # Check if validation is a style name or custom table name
        table_style = "table"  # default
        table_name = None
        if validation:
            if validation.lower() in TABLE_STYLES and TABLE_STYLES[validation.lower()].get('native', False):
                table_style = validation.lower()
            else:
                table_name = validation  # Use as custom table name

        ts = TABLE_STYLES[table_style]

        start_row, end_row, start_col, end_col = _parse_a1(range)
        grid = {"sheetId": sheet_id, "startRowIndex": start_row, "endRowIndex": end_row,
                "startColumnIndex": start_col, "endColumnIndex": end_col}

        # Read header row to get column names
        header_range = f"{sheet}!{_index_to_col(start_col)}{start_row + 1}:{_index_to_col(end_col - 1)}{start_row + 1}"
        header_result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=header_range
        ).execute()
        header_row = header_result.get('values', [[]])[0] if header_result.get('values') else []

        # Validate that data has proper columnar structure for native table
        num_cols = end_col - start_col if end_col else len(header_row)
        non_empty_headers = sum(1 for h in header_row if h and str(h).strip())

        # Native tables require at least 2 columns with defined headers
        # and more than half of headers should be non-empty
        if not (num_cols >= 2 and non_empty_headers >= 2 and non_empty_headers > num_cols // 2):
            return {"error": "Cannot create native table: data must have at least 2 columns with defined headers. Use style='basic' for non-tabular data."}

        # Build column properties from header row
        column_properties = []
        for i in range(num_cols):
            col_name = header_row[i] if i < len(header_row) and header_row[i] else f"Column{i+1}"
            column_properties.append({
                "columnIndex": i,
                "columnName": str(col_name)
            })

        # Generate table name (sanitize sheet name for valid references)
        if not table_name:
            safe_sheet = sheet.replace(' ', '_').replace('-', '_')
            table_name = f"Table_{safe_sheet}_{int(time.time())}"

        # Build table spec
        table_spec = {
            "name": table_name,
            "range": grid,
            "columnProperties": column_properties
        }

        # Add custom colors if specified in style
        if 'header_color' in ts:
            header_color = _parse_color(ts['header_color'])
            band_color = ts.get('band_color', {"red": 1, "green": 1, "blue": 1})
            table_spec["rowsProperties"] = {
                "headerColorStyle": {"rgbColor": header_color},
                "firstBandColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}},
                "secondBandColorStyle": {"rgbColor": band_color}
            }

        requests = []

        # Add native table (like Format > Convert to Table)
        requests.append({"addTable": {"table": table_spec}})

        # Auto-resize
        requests.append({"autoResizeDimensions": {"dimensions": {
            "sheetId": sheet_id, "dimension": "COLUMNS",
            "startIndex": start_col, "endIndex": end_col
        }}})

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()
        return {'table_created': range, 'table_name': table_name, 'style': table_style, 'columns': len(column_properties)}

    # === VALIDATE ===
    elif action == "validate":
        if not range:
            return {"error": "range is required for validate action"}
        if not validation:
            return {"error": "validation type is required"}

        rule = {"showCustomUi": True, "strict": not allow_invalid}

        if validation == "dropdown":
            if not options:
                return {"error": "options are required for dropdown validation"}
            rule["condition"] = {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": opt} for opt in options]}

        elif validation == "checkbox":
            rule["condition"] = {"type": "BOOLEAN"}

        elif validation == "number":
            if min_value is not None and max_value is not None:
                rule["condition"] = {"type": "NUMBER_BETWEEN", "values": [
                    {"userEnteredValue": str(min_value)}, {"userEnteredValue": str(max_value)}
                ]}
            elif min_value is not None:
                rule["condition"] = {"type": "NUMBER_GREATER_THAN_EQ", "values": [{"userEnteredValue": str(min_value)}]}
            elif max_value is not None:
                rule["condition"] = {"type": "NUMBER_LESS_THAN_EQ", "values": [{"userEnteredValue": str(max_value)}]}
            else:
                rule["condition"] = {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "-999999999999"}]}

        elif validation == "date":
            rule["condition"] = {"type": "DATE_IS_VALID"}

        elif validation == "custom":
            if not custom_formula:
                return {"error": "custom_formula is required for custom validation"}
            rule["condition"] = {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": custom_formula}]}

        else:
            return {"error": f"Unknown validation type: {validation}"}

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"setDataValidation": {"range": _grid_range(sheet_id, range), "rule": rule}}]}
        ).execute()
        return {'validation_added': range, 'type': validation}

    return {"error": f"Unknown action: {action}"}


# =============================================================================
# TOOL 4: sheets_visualize - Charts/Pivots/Sparklines
# =============================================================================

@mcp.tool()
def sheets_visualize(
    spreadsheet_id: str,
    sheet: str,
    action: str,
    # Chart options
    chart_type: Optional[str] = None,
    data_range: Optional[str] = None,
    title: Optional[str] = None,
    position: Optional[str] = None,
    width: int = 600,
    height: int = 400,
    style: Optional[str] = None,
    # Chart styling options
    chart_id: Optional[int] = None,
    colors: Optional[List[str]] = None,
    donut: bool = False,
    smooth_lines: bool = False,
    legend: Optional[str] = None,
    show_gridlines: bool = True,
    # Pivot options
    source_range: Optional[str] = None,
    pivot_rows: Optional[List[str]] = None,
    pivot_cols: Optional[List[str]] = None,
    pivot_values: Optional[List[Dict[str, str]]] = None,
    # Sparkline options
    sparkline_type: Optional[str] = None,
    sparkline_range: Optional[str] = None,
    target_cell: Optional[str] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Visualization operations for Google Sheets (Charts, Pivots, Sparklines).

    Actions:
        chart: Create a chart
        update_chart: Update an existing chart's style/colors
        delete_chart: Delete a chart by ID
        pivot: Create a pivot table
        sparkline: Insert sparkline formula into a cell

    Chart types: line, bar, column, pie, area, scatter, combo, stacked_bar, stacked_column, donut

    Chart styling options:
        colors: List of colors for series (e.g., ["blue", "red", "green"] or ["#3366cc", "#dc3912"])
        donut: Convert pie chart to donut (75% hole)
        smooth_lines: Enable line smoothing for line charts
        legend: Position - "bottom", "top", "right", "left", "none"
        show_gridlines: Show/hide gridlines (default: True)

    Examples:
        # Create a column chart with custom colors
        sheets_visualize(id, "Sheet1", "chart", chart_type="column", data_range="A1:E10",
                        title="Sales", position="G1", colors=["blue", "red", "green"])

        # Create a donut chart
        sheets_visualize(id, "Sheet1", "chart", chart_type="pie", data_range="A1:B5",
                        title="Distribution", donut=True)

        # Create a smooth line chart
        sheets_visualize(id, "Sheet1", "chart", chart_type="line", data_range="A1:D10",
                        smooth_lines=True, legend="bottom")

        # Update existing chart colors
        sheets_visualize(id, "Sheet1", "update_chart", chart_id=123456,
                        colors=["#3366cc", "#dc3912", "#109618"])

        # Delete a chart
        sheets_visualize(id, "Sheet1", "delete_chart", chart_id=123456)

        # Create a pivot table
        sheets_visualize(id, "Sheet1", "pivot", source_range="A1:F100",
                        pivot_rows=["Category"], pivot_cols=["Region"],
                        pivot_values=[{"field": "Sales", "summarize": "SUM"}])

        # Add sparkline
        sheets_visualize(id, "Sheet1", "sparkline", sparkline_type="line", sparkline_range="B2:M2", target_cell="N2")
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
    action = action.lower()

    # === CHART ===
    if action == "chart":
        if not chart_type:
            return {"error": "chart_type is required"}
        if not data_range:
            return {"error": "data_range is required"}

        # Map chart types
        chart_type_map = {
            "line": "LINE",
            "bar": "BAR",
            "column": "COLUMN",
            "pie": "PIE",
            "area": "AREA",
            "scatter": "SCATTER",
            "combo": "COMBO",
            "stacked_bar": "BAR",
            "stacked_column": "COLUMN",
        }
        basic_chart_type = chart_type_map.get(chart_type.lower(), "COLUMN")

        # Parse position
        anchor_col, anchor_row = 0, 0
        if position:
            pos_match = re.match(r'^([A-Za-z]+)(\d+)$', position)
            if pos_match:
                anchor_col = _col_to_index(pos_match.group(1))
                anchor_row = int(pos_match.group(2)) - 1

        # Build source range
        full_range = f"{sheet}!{data_range}"

        # Parse the data range to extract domain (first column) and series (other columns)
        range_match = re.match(r'^([A-Za-z]+)(\d+):([A-Za-z]+)(\d+)$', data_range)
        if not range_match:
            return {"error": f"Invalid data_range format: {data_range}"}

        start_col = range_match.group(1).upper()
        start_row = int(range_match.group(2))
        end_col = range_match.group(3).upper()
        end_row = int(range_match.group(4))

        # Domain is the first column (X-axis labels)
        domain_range = f"{start_col}{start_row}:{start_col}{end_row}"

        # Series are the subsequent columns
        start_col_idx = _col_to_index(start_col)
        end_col_idx = _col_to_index(end_col)

        # Bar charts use BOTTOM_AXIS, others use LEFT_AXIS
        target_axis = "BOTTOM_AXIS" if chart_type.lower() in ("bar", "stacked_bar") else "LEFT_AXIS"

        # Parse colors if provided
        parsed_colors = []
        if colors:
            for c in colors:
                parsed = _parse_color(c)
                if parsed:
                    parsed_colors.append(parsed)

        series_list = []
        for i, col_idx in enumerate(range(start_col_idx + 1, end_col_idx + 1)):
            col_letter = _index_to_col(col_idx)
            series_range = f"{col_letter}{start_row}:{col_letter}{end_row}"
            series_item = {
                "series": {"sourceRange": {"sources": [_grid_range(sheet_id, series_range)]}},
                "targetAxis": target_axis
            }
            # Apply color if provided
            if i < len(parsed_colors):
                series_item["color"] = parsed_colors[i]
            series_list.append(series_item)

        # Legend position mapping
        legend_map = {
            "bottom": "BOTTOM_LEGEND", "top": "TOP_LEGEND",
            "right": "RIGHT_LEGEND", "left": "LEFT_LEGEND",
            "none": "NO_LEGEND"
        }
        legend_pos = legend_map.get(legend.lower() if legend else "bottom", "BOTTOM_LEGEND")

        # Basic chart spec
        chart_spec = {
            "title": title or "",
            "basicChart": {
                "chartType": basic_chart_type,
                "legendPosition": legend_pos,
                "headerCount": 1,
                "domains": [{"domain": {"sourceRange": {"sources": [_grid_range(sheet_id, domain_range)]}}}],
                "series": series_list,
            }
        }

        # Line smoothing
        if smooth_lines and chart_type.lower() == "line":
            chart_spec["basicChart"]["lineSmoothing"] = True

        # Stacked options
        if chart_type.lower() in ("stacked_bar", "stacked_column"):
            chart_spec["basicChart"]["stackedType"] = "STACKED"

        # Pie/Donut chart uses different structure (only first series)
        if chart_type.lower() in ("pie", "donut") or (chart_type.lower() == "pie" and donut):
            first_series_col = _index_to_col(start_col_idx + 1)
            first_series_range = f"{first_series_col}{start_row}:{first_series_col}{end_row}"
            pie_legend = legend_map.get(legend.lower() if legend else "right", "RIGHT_LEGEND")
            chart_spec = {
                "title": title or "",
                "pieChart": {
                    "legendPosition": pie_legend,
                    "domain": {"sourceRange": {"sources": [_grid_range(sheet_id, domain_range)]}},
                    "series": {"sourceRange": {"sources": [_grid_range(sheet_id, first_series_range)]}},
                }
            }
            # Donut: 75% hole
            if donut or chart_type.lower() == "donut":
                chart_spec["pieChart"]["pieHole"] = 0.75

        # Apply style preset
        if style and style.lower() in CHART_STYLES:
            cs = CHART_STYLES[style.lower()]
            if "legend_position" in cs:
                if "basicChart" in chart_spec:
                    chart_spec["basicChart"]["legendPosition"] = cs["legend_position"]
                elif "pieChart" in chart_spec:
                    chart_spec["pieChart"]["legendPosition"] = cs["legend_position"]

        request = {
            "addChart": {
                "chart": {
                    "spec": chart_spec,
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {"sheetId": sheet_id, "rowIndex": anchor_row, "columnIndex": anchor_col},
                            "widthPixels": width,
                            "heightPixels": height
                        }
                    }
                }
            }
        }

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()

        new_chart_id = result.get('replies', [{}])[0].get('addChart', {}).get('chart', {}).get('chartId')
        return {'chart_created': True, 'chart_id': new_chart_id, 'type': chart_type, 'position': position}

    # === DELETE CHART ===
    elif action == "delete_chart":
        if not chart_id:
            return {"error": "chart_id is required for delete_chart"}
        request = {"deleteEmbeddedObject": {"objectId": chart_id}}
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()
        return {"deleted": True, "chart_id": chart_id}

    # === UPDATE CHART (simplified - colors only for now) ===
    elif action == "update_chart":
        if not chart_id:
            return {"error": "chart_id is required for update_chart"}

        # Get current chart spec
        spreadsheet = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id, includeGridData=False
        ).execute()

        # Find the chart
        current_spec = None
        for s in spreadsheet.get('sheets', []):
            for chart in s.get('charts', []):
                if chart.get('chartId') == chart_id:
                    current_spec = chart.get('spec', {})
                    break

        if not current_spec:
            return {"error": f"Chart with ID {chart_id} not found"}

        # Update colors in series
        if colors:
            parsed_colors = [_parse_color(c) for c in colors if _parse_color(c)]
            if "basicChart" in current_spec:
                for i, series in enumerate(current_spec["basicChart"].get("series", [])):
                    if i < len(parsed_colors):
                        series["color"] = parsed_colors[i]
            elif "pieChart" in current_spec:
                # Pie charts don't have series colors the same way
                pass

        # Update legend
        if legend:
            legend_map = {
                "bottom": "BOTTOM_LEGEND", "top": "TOP_LEGEND",
                "right": "RIGHT_LEGEND", "left": "LEFT_LEGEND",
                "none": "NO_LEGEND"
            }
            legend_pos = legend_map.get(legend.lower(), "BOTTOM_LEGEND")
            if "basicChart" in current_spec:
                current_spec["basicChart"]["legendPosition"] = legend_pos
            elif "pieChart" in current_spec:
                current_spec["pieChart"]["legendPosition"] = legend_pos

        # Update donut hole
        if donut and "pieChart" in current_spec:
            current_spec["pieChart"]["pieHole"] = 0.75

        # Update line smoothing
        if smooth_lines and "basicChart" in current_spec:
            current_spec["basicChart"]["lineSmoothing"] = True

        # Update title if provided
        if title:
            current_spec["title"] = title

        request = {"updateChartSpec": {"chartId": chart_id, "spec": current_spec}}
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()
        return {"updated": True, "chart_id": chart_id}

    # === PIVOT ===
    elif action == "pivot":
        if not source_range:
            return {"error": "source_range is required for pivot"}

        # Get headers from source range to map field names to indices
        full_source = f"{sheet}!{source_range}"
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=full_source
        ).execute()
        headers = result.get('values', [[]])[0]
        header_map = {h.lower(): i for i, h in enumerate(headers)}

        # Build pivot table definition
        pivot_def = {
            "source": {"sheetId": sheet_id, **_grid_range(sheet_id, source_range)},
            "rows": [],
            "columns": [],
            "values": []
        }

        # Add row groups
        if pivot_rows:
            for field in pivot_rows:
                idx = header_map.get(field.lower(), 0)
                pivot_def["rows"].append({
                    "sourceColumnOffset": idx,
                    "showTotals": True,
                    "sortOrder": "ASCENDING"
                })

        # Add column groups
        if pivot_cols:
            for field in pivot_cols:
                idx = header_map.get(field.lower(), 0)
                pivot_def["columns"].append({
                    "sourceColumnOffset": idx,
                    "showTotals": True,
                    "sortOrder": "ASCENDING"
                })

        # Add values
        if pivot_values:
            summarize_map = {
                "sum": "SUM", "count": "COUNTA", "average": "AVERAGE",
                "max": "MAX", "min": "MIN", "countunique": "COUNTUNIQUE"
            }
            for v in pivot_values:
                field = v.get('field', '')
                summarize = v.get('summarize', 'SUM').upper()
                idx = header_map.get(field.lower(), 0)
                pivot_def["values"].append({
                    "sourceColumnOffset": idx,
                    "summarizeFunction": summarize_map.get(summarize.lower(), "SUM")
                })

        # Create new sheet for pivot
        pivot_sheet_name = f"Pivot_{sheet}"
        try:
            add_result = sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": pivot_sheet_name}}}]}
            ).execute()
            pivot_sheet_id = add_result['replies'][0]['addSheet']['properties']['sheetId']
        except:
            # Sheet might already exist
            pivot_sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, pivot_sheet_name)

        # Create pivot table
        request = {
            "updateCells": {
                "rows": [{"values": [{"pivotTable": pivot_def}]}],
                "start": {"sheetId": pivot_sheet_id, "rowIndex": 0, "columnIndex": 0},
                "fields": "pivotTable"
            }
        }

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()

        return {'pivot_created': True, 'pivot_sheet': pivot_sheet_name, 'source': source_range}

    # === SPARKLINE ===
    elif action == "sparkline":
        if not sparkline_range:
            return {"error": "sparkline_range is required"}
        if not target_cell:
            return {"error": "target_cell is required"}

        sparkline_type = (sparkline_type or "line").lower()
        type_map = {"line": "line", "bar": "bar", "column": "column", "winloss": "winloss"}
        sl_type = type_map.get(sparkline_type, "line")

        # Build sparkline formula
        full_range = f"{sheet}!{sparkline_range}"
        if sl_type == "line":
            formula = f'=SPARKLINE({full_range})'
        else:
            formula = f'=SPARKLINE({full_range}, {{"charttype", "{sl_type}"}})'

        # Write formula to target cell
        target_full = f"{sheet}!{target_cell}"
        result = sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=target_full,
            valueInputOption='USER_ENTERED', body={'values': [[formula]]}
        ).execute()

        return {'sparkline_created': True, 'target': target_cell, 'type': sl_type, 'formula': formula}

    return {"error": f"Unknown action: {action}"}


# =============================================================================
# TOOL 5: sheets_manage - Sheet Tab Operations
# =============================================================================

@mcp.tool()
def sheets_manage(
    spreadsheet_id: str,
    action: str,
    sheet: Optional[str] = None,
    new_name: Optional[str] = None,
    destination_spreadsheet: Optional[str] = None,
    rows: int = 1000,
    cols: int = 26,
    tab_color: Optional[str] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Sheet tab management operations.

    Actions:
        list: List all sheet tabs in the spreadsheet
        create: Create a new sheet tab
        rename: Rename an existing sheet tab
        copy: Copy a sheet to same or different spreadsheet
        delete: Delete a sheet tab
        duplicate: Duplicate a sheet within the same spreadsheet

    Examples:
        # List all sheets
        sheets_manage(id, "list")

        # Create new sheet
        sheets_manage(id, "create", sheet="Sales Data", rows=500, cols=20, tab_color="blue")

        # Rename sheet
        sheets_manage(id, "rename", sheet="Sheet1", new_name="Raw Data")

        # Copy to another spreadsheet
        sheets_manage(id, "copy", sheet="Sales", destination_spreadsheet="other_id", new_name="Sales Backup")

        # Delete sheet
        sheets_manage(id, "delete", sheet="Temp")

        # Duplicate sheet
        sheets_manage(id, "duplicate", sheet="Template", new_name="January")
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    action = action.lower()

    # === LIST ===
    if action == "list":
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets_list = []
        for s in spreadsheet['sheets']:
            props = s['properties']
            sheets_list.append({
                'title': props['title'],
                'sheet_id': props['sheetId'],
                'index': props['index'],
                'row_count': props.get('gridProperties', {}).get('rowCount'),
                'column_count': props.get('gridProperties', {}).get('columnCount')
            })
        return {'sheets': sheets_list, 'count': len(sheets_list)}

    # === CREATE ===
    elif action == "create":
        if not sheet:
            return {"error": "sheet name is required for create action"}

        props = {"title": sheet, "gridProperties": {"rowCount": rows, "columnCount": cols}}
        if tab_color:
            props["tabColor"] = _parse_color(tab_color)

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": props}}]}
        ).execute()
        new_props = result['replies'][0]['addSheet']['properties']
        return {'created': sheet, 'sheet_id': new_props['sheetId']}

    # === RENAME ===
    elif action == "rename":
        if not sheet or not new_name:
            return {"error": "sheet and new_name are required for rename action"}

        sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "title": new_name}, "fields": "title"
            }}]}
        ).execute()
        return {'renamed': sheet, 'new_name': new_name}

    # === COPY ===
    elif action == "copy":
        if not sheet:
            return {"error": "sheet name is required for copy action"}

        sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
        dest_id = destination_spreadsheet or spreadsheet_id

        copy_result = sheets_service.spreadsheets().sheets().copyTo(
            spreadsheetId=spreadsheet_id, sheetId=sheet_id,
            body={"destinationSpreadsheetId": dest_id}
        ).execute()

        # Rename if new_name provided
        if new_name and copy_result.get('title') != new_name:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=dest_id,
                body={"requests": [{"updateSheetProperties": {
                    "properties": {"sheetId": copy_result['sheetId'], "title": new_name}, "fields": "title"
                }}]}
            ).execute()
            copy_result['title'] = new_name

        return {'copied': sheet, 'destination': dest_id, 'new_sheet_id': copy_result['sheetId'], 'new_title': copy_result.get('title', new_name)}

    # === DELETE ===
    elif action == "delete":
        if not sheet:
            return {"error": "sheet name is required for delete action"}

        sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"deleteSheet": {"sheetId": sheet_id}}]}
        ).execute()
        return {'deleted': sheet, 'sheet_id': sheet_id}

    # === DUPLICATE ===
    elif action == "duplicate":
        if not sheet:
            return {"error": "sheet name is required for duplicate action"}

        sheet_id = _get_sheet_id(sheets_service, spreadsheet_id, sheet)
        dup_name = new_name or f"{sheet} (Copy)"

        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"duplicateSheet": {
                "sourceSheetId": sheet_id,
                "newSheetName": dup_name
            }}]}
        ).execute()
        new_props = result['replies'][0]['duplicateSheet']['properties']
        return {'duplicated': sheet, 'new_sheet': dup_name, 'new_sheet_id': new_props['sheetId']}

    return {"error": f"Unknown action: {action}"}


# =============================================================================
# TOOL 6: drive - Spreadsheet & Folder Operations
# =============================================================================

@mcp.tool()
def drive(
    action: str,
    spreadsheet_id: Optional[str] = None,
    title: Optional[str] = None,
    folder_id: Optional[str] = None,
    recipients: Optional[List[Dict[str, str]]] = None,
    spreadsheet_ids: Optional[List[str]] = None,
    preview_rows: int = 5,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Google Drive operations for spreadsheets.

    Actions:
        create: Create a new spreadsheet
        list: List spreadsheets in a folder
        share: Share a spreadsheet with users
        folders: List folders in Drive
        info: Get spreadsheet metadata
        summary: Get summary of multiple spreadsheets (headers, preview rows)

    Examples:
        # Create new spreadsheet
        drive("create", title="Sales Report 2024", folder_id="folder_id_here")

        # List spreadsheets in folder
        drive("list", folder_id="folder_id")

        # Share spreadsheet
        drive("share", spreadsheet_id="xxx", recipients=[{"email": "user@example.com", "role": "writer"}])

        # List folders
        drive("folders", folder_id="parent_folder_id")

        # Get spreadsheet info
        drive("info", spreadsheet_id="xxx")

        # Get summary of multiple spreadsheets
        drive("summary", spreadsheet_ids=["id1", "id2"], preview_rows=3)
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    drive_service = ctx.request_context.lifespan_context.drive_service
    default_folder = ctx.request_context.lifespan_context.folder_id
    action = action.lower()

    # === CREATE ===
    if action == "create":
        if not title:
            return {"error": "title is required for create action"}

        target_folder = folder_id or default_folder
        body = {'name': title, 'mimeType': 'application/vnd.google-apps.spreadsheet'}
        if target_folder:
            body['parents'] = [target_folder]

        result = drive_service.files().create(supportsAllDrives=True, body=body, fields='id, name, parents').execute()
        return {'spreadsheet_id': result['id'], 'title': result['name'], 'folder': result.get('parents', ['root'])[0]}

    # === LIST ===
    elif action == "list":
        target_folder = folder_id or default_folder
        query = "mimeType='application/vnd.google-apps.spreadsheet'"
        if target_folder:
            query += f" and '{target_folder}' in parents"

        result = drive_service.files().list(
            q=query, spaces='drive', includeItemsFromAllDrives=True, supportsAllDrives=True,
            fields='files(id, name, modifiedTime)', orderBy='modifiedTime desc'
        ).execute()

        return {'spreadsheets': [{'id': f['id'], 'title': f['name'], 'modified': f.get('modifiedTime')} for f in result.get('files', [])]}

    # === SHARE ===
    elif action == "share":
        if not spreadsheet_id:
            return {"error": "spreadsheet_id is required for share action"}
        if not recipients:
            return {"error": "recipients are required for share action"}

        successes, failures = [], []
        for r in recipients:
            email, role = r.get('email'), r.get('role', 'writer')
            if not email:
                failures.append({'email': None, 'error': 'Missing email'})
                continue
            if role not in ['reader', 'commenter', 'writer']:
                failures.append({'email': email, 'error': f"Invalid role: {role}"})
                continue
            try:
                result = drive_service.permissions().create(
                    fileId=spreadsheet_id, body={'type': 'user', 'role': role, 'emailAddress': email},
                    sendNotificationEmail=True, fields='id'
                ).execute()
                successes.append({'email': email, 'role': role, 'permission_id': result['id']})
            except Exception as e:
                failures.append({'email': email, 'error': str(e)})

        return {"shared": successes, "failed": failures}

    # === FOLDERS ===
    elif action == "folders":
        parent = folder_id or 'root'
        query = f"mimeType='application/vnd.google-apps.folder' and '{parent}' in parents"

        result = drive_service.files().list(
            q=query, spaces='drive', includeItemsFromAllDrives=True, supportsAllDrives=True,
            fields='files(id, name)', orderBy='name'
        ).execute()

        return {'folders': [{'id': f['id'], 'name': f['name']} for f in result.get('files', [])]}

    # === INFO ===
    elif action == "info":
        if not spreadsheet_id:
            return {"error": "spreadsheet_id is required for info action"}

        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        props = spreadsheet.get('properties', {})
        sheets_list = [{'title': s['properties']['title'], 'sheet_id': s['properties']['sheetId']}
                       for s in spreadsheet.get('sheets', [])]

        return {
            'spreadsheet_id': spreadsheet_id,
            'title': props.get('title'),
            'locale': props.get('locale'),
            'time_zone': props.get('timeZone'),
            'sheets': sheets_list
        }

    # === SUMMARY ===
    elif action == "summary":
        if not spreadsheet_ids:
            return {"error": "spreadsheet_ids are required for summary action"}

        summaries = []
        for sid in spreadsheet_ids:
            summary = {'spreadsheet_id': sid, 'title': None, 'sheets': [], 'error': None}
            try:
                ss = sheets_service.spreadsheets().get(spreadsheetId=sid, fields='properties.title,sheets(properties(title,sheetId))').execute()
                summary['title'] = ss.get('properties', {}).get('title', 'Unknown')

                for s in ss.get('sheets', []):
                    sheet_title = s.get('properties', {}).get('title')
                    sheet_summary = {'title': sheet_title, 'headers': [], 'preview': [], 'error': None}
                    try:
                        result = sheets_service.spreadsheets().values().get(
                            spreadsheetId=sid, range=f"{sheet_title}!1:{preview_rows + 1}"
                        ).execute()
                        values = result.get('values', [])
                        if values:
                            sheet_summary['headers'] = values[0]
                            sheet_summary['preview'] = values[1:] if len(values) > 1 else []
                    except Exception as e:
                        sheet_summary['error'] = str(e)
                    summary['sheets'].append(sheet_summary)
            except Exception as e:
                summary['error'] = str(e)
            summaries.append(summary)

        return {'summaries': summaries}

    return {"error": f"Unknown action: {action}"}


# =============================================================================
# UTILITY TOOLS
# =============================================================================

@mcp.tool()
def get_presets(ctx: Context = None) -> Dict[str, Any]:
    """
    Get all available presets for styles, table formats, number formats, and colors.

    Returns dictionaries of:
    - styles: Text/cell style presets (h1, h2, header, bold, success, error, etc.)
    - table_styles: "table" = native Google Sheets Table, "basic*" = styled ranges (no auto-expand/filters)
    - number_formats: Number format patterns (currency, percent, date, etc.)
    - colors: Named colors available for use
    - chart_styles: Chart style presets
    """
    # "table" = native Google Sheets Table (Format > Convert to Table)
    # "basic*" = styled ranges with manual formatting (not real tables)
    native_tables = [k for k, v in TABLE_STYLES.items() if v.get('native', False)]
    styled_ranges = [k for k, v in TABLE_STYLES.items() if not v.get('native', False)]

    return {
        'styles': list(STYLES.keys()),
        'table_styles': {'native_table': native_tables, 'styled_ranges': styled_ranges},
        'number_formats': NUMBER_FORMATS,
        'colors': list(COLORS.keys()),
        'chart_styles': list(CHART_STYLES.keys())
    }


@mcp.tool()
def ascii_diagram(
    action: str,
    text: Optional[str] = None,
    lines: Optional[List[str]] = None,
    width: int = 77,
    height: int = 10,
    elements: Optional[List[Dict[str, Any]]] = None,
    data: Optional[List[Any]] = None,
    headers: Optional[List[str]] = None,
    rows: Optional[List[List[str]]] = None,
    palette: str = "blocks",
    direction: str = "radial",
    contrast: float = 0.7,
    box_style: str = "light",
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Build ASCII diagrams programmatically with proper alignment.

    Actions:
        box: Create a box with centered text
        title: Create a title bar (single-line box)
        comment: Create a comment annotation (◄── text)
        arrow: Create an arrow (direction: right, left, down, up)
        diagram: Build full diagram from elements list
        bar_chart: Create horizontal bar chart from data
        sparkline: Create inline sparkline from numeric data
        progress: Create progress bar
        shaded_box: Create a box with gradient shading fill
        table: Create a bordered data table

    Args:
        text: Text content for box/title/comment/shaded_box title
        lines: Multiple lines for box content
        width: Total width (default 77, good for sheets)
        height: Height for shaded_box (default 10)
        elements: List of element dicts for "diagram" action
        data: Data for charts (bar_chart: [("label", value)...], sparkline: [1,2,3...])
        headers: Column headers for table action
        rows: Data rows for table action
        palette: Shading palette (ascii, blocks, dots, density, braille)
        direction: Gradient direction (horizontal, vertical, radial, diagonal)
        contrast: Contrast level 0.0-1.0 for shaded_box
        box_style: Border style (light, heavy, double, rounded)

    Element types for "diagram" action:
        {"type": "title", "text": "TITLE TEXT"}
        {"type": "box", "text": "Content"} or {"type": "box", "lines": ["Line1", "Line2"]}
        {"type": "box", "text": "Content", "x": 4, "comment": "◄── Note"}
        {"type": "text", "text": "Raw line", "x": 10}
        {"type": "arrow", "direction": "down", "x": 20}
        {"type": "spacer"}
        {"type": "bar_chart", "data": [("Sales", 100), ("Costs", 50)], "bar_width": 20}
        {"type": "sparkline", "data": [1,5,3,8,4], "label": "Trend:"}
        {"type": "progress", "value": 75, "max": 100, "width": 20}
        {"type": "shaded_box", "width": 40, "height": 10, "palette": "blocks", "direction": "radial"}
        {"type": "table", "headers": ["Col1", "Col2"], "rows": [["A", "B"]], "box_style": "light"}

    Shading palettes:
        ascii: " .:-=+*#%@" (classic ASCII art)
        blocks: " ░▒▓█" (smooth gradients)
        dots: " ·∘○●◉" (geometric)
        density: " .,;!lI$@" (high detail)
        braille: "⠀⠁⠃⠇⠏⠟⠿⣿" (ultra-fine)

    Box styles:
        light: ┌─┐│└┘ (minimal)
        heavy: ┏━┓┃┗┛ (bold)
        double: ╔═╗║╚╝ (formal)
        rounded: ╭─╮│╰╯ (friendly)

    Examples:
        # Create a shaded box with radial gradient
        ascii_diagram("shaded_box", text="Status", width=50, height=12, palette="blocks", direction="radial")

        # Create a shaded box with horizontal gradient using double border
        ascii_diagram("shaded_box", width=40, height=8, palette="dots", direction="horizontal", box_style="double")

        # Create a data table
        ascii_diagram("table", headers=["Task", "Status"], rows=[["Deploy", "✓"], ["Test", "⏳"]], box_style="heavy")

        # Build diagram with shaded box
        ascii_diagram("diagram", elements=[
            {"type": "title", "text": "DASHBOARD"},
            {"type": "shaded_box", "width": 50, "height": 8, "palette": "blocks", "direction": "radial", "title": "Metrics"},
            {"type": "table", "headers": ["Name", "Value"], "rows": [["CPU", "45%"], ["RAM", "2.1GB"]]}
        ])
    """
    action = action.lower()

    if action == "box":
        content = lines if lines else [text or ""]
        box_lines = _ascii_box(content, width)
        return {"lines": box_lines, "text": "\n".join(box_lines)}

    elif action == "title":
        title_lines = _ascii_title_box(text or "", width)
        return {"lines": title_lines, "text": "\n".join(title_lines)}

    elif action == "comment":
        comment = _ascii_comment(text or "")
        return {"text": comment}

    elif action == "arrow":
        direction = text or "right"
        arrow = _ascii_arrow(direction, width)
        return {"text": arrow}

    elif action == "diagram":
        if not elements:
            return {"error": "elements list is required for diagram action"}
        diagram_text = _ascii_diagram(elements, width)
        return {"lines": diagram_text.split("\n"), "text": diagram_text}

    elif action == "bar_chart":
        if not data:
            return {"error": "data is required for bar_chart action (list of (label, value) tuples)"}
        # Convert list of lists to list of tuples if needed
        chart_data = [(d[0], d[1]) for d in data] if data else []
        chart_lines = _ascii_bar_chart(chart_data, bar_width=width)
        return {"lines": chart_lines, "text": "\n".join(chart_lines)}

    elif action == "sparkline":
        if not data:
            return {"error": "data is required for sparkline action (list of numbers)"}
        spark = _ascii_sparkline(data)
        label = text or ""
        result = f"{label} {spark}" if label else spark
        return {"text": result}

    elif action == "progress":
        if not data or len(data) < 1:
            return {"error": "data is required for progress action [value] or [value, max]"}
        value = data[0]
        max_val = data[1] if len(data) > 1 else 100
        bar = _ascii_progress_bar(value, max_val, width)
        return {"text": bar}

    elif action == "shaded_box":
        shaded_lines = _ascii_shaded_box(width, height, text, palette, direction, contrast, box_style)
        return {"lines": shaded_lines, "text": "\n".join(shaded_lines)}

    elif action == "table":
        if not headers:
            return {"error": "headers is required for table action"}
        table_lines = _ascii_table(headers, rows or [], box_style)
        return {"lines": table_lines, "text": "\n".join(table_lines)}

    elif action == "chars":
        # Return available ASCII characters for reference
        return {
            "box_chars": ASCII,
            "bar_blocks": CHART_BLOCKS,
            "sparkline_chars": SPARKLINE_CHARS,
            "shading_palettes": SHADING_PALETTES,
            "box_styles": list(BOX_STYLES.keys()),
        }

    return {"error": f"Unknown action: {action}"}


@mcp.tool()
def batch_update(spreadsheet_id: str, requests: List[Dict[str, Any]], ctx: Context = None) -> Dict[str, Any]:
    """
    Low-level batch update for advanced operations not covered by other tools.
    Use this as an escape hatch for complex operations.

    Pass raw Google Sheets API request objects.
    See: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    if not requests:
        return {"error": "requests cannot be empty"}
    return sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()


# =============================================================================
# RESOURCE
# =============================================================================

@mcp.resource("spreadsheet://{spreadsheet_id}/info")
def get_spreadsheet_info(spreadsheet_id: str) -> str:
    """Get spreadsheet info as JSON."""
    context = mcp.get_lifespan_context()
    ss = context.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return json.dumps({
        "title": ss.get('properties', {}).get('title', 'Unknown'),
        "sheets": [{"title": s['properties']['title'], "sheetId": s['properties']['sheetId']} for s in ss.get('sheets', [])]
    }, indent=2)


# =============================================================================
# MAIN
# =============================================================================

def main():
    transport = "stdio"
    for i, arg in enumerate(sys.argv):
        if arg == "--transport" and i + 1 < len(sys.argv):
            transport = sys.argv[i + 1]
            break
    mcp.run(transport=transport)
