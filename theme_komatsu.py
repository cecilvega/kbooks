import polars as pl
import plotnine as p9
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

# ============================================================================
# KOMATSU THEME TEMPLATE - Save this section for reuse
# ============================================================================

# Define Komatsu color palette
KOMATSU_COLORS = {
    "primary_blue": "#003F87",  # Deep blue from the image
    "primary_yellow": "#FFCC00",  # Yellow from the image
    "light_blue": "#0066CC",  # Lighter blue for contrast
    "gray": "#E5E5E5",  # Light gray for background
    "text_primary": "#333333",  # Main text color
    "text_secondary": "#666666",  # Secondary text color
    "grid_color": "#E0E0E0",  # Grid line color
}

# Extended color palette for multiple components
COMPONENT_COLORS = [
    "#003F87",  # Deep blue
    "#FFCC00",  # Yellow
    "#333333",  # Dark gray
    "#0066CC",  # Light blue
    "#9370DB",  # Medium purple (similar to the image)
    "#FFA500",  # Orange
    "#808080",  # Gray
    "#4169E1",  # Royal blue
]


def theme_komatsu(figure_size=(14, 7)):
    """
    Komatsu corporate theme for plotnine plots.
    Clean, professional appearance with company colors.

    Args:
        figure_size: tuple of (width, height) in inches

    Returns:
        plotnine theme object
    """
    return p9.theme_bw() + p9.theme(  # Start with clean base
        figure_size=figure_size,
        # Text elements
        text=p9.element_text(family="Arial", color=KOMATSU_COLORS["text_primary"]),
        axis_text_x=p9.element_text(rotation=0, ha="center", size=10),  # Changed rotation to 0 for months
        axis_text_y=p9.element_text(size=10),
        axis_title=p9.element_text(size=12, weight="bold"),
        # Title and subtitle
        plot_title=p9.element_text(size=18, weight="bold", color=KOMATSU_COLORS["primary_blue"]),
        plot_subtitle=p9.element_text(size=12, style="italic", color=KOMATSU_COLORS["text_secondary"]),
        # Legend
        legend_position="right",  # Changed to right like in the image
        legend_title=p9.element_text(weight="bold", size=11),
        legend_text=p9.element_text(size=10),
        legend_background=p9.element_rect(fill="white", color="none"),
        # Grid and background
        panel_grid_major_x=p9.element_blank(),  # Remove vertical grid
        panel_grid_minor=p9.element_blank(),  # Remove minor grid
        panel_grid_major_y=p9.element_line(color=KOMATSU_COLORS["grid_color"], size=0.5),
        panel_border=p9.element_rect(color=KOMATSU_COLORS["text_primary"], size=1),
        # Background colors
        plot_background=p9.element_rect(fill="white"),
        panel_background=p9.element_rect(fill="white"),
    )


# ============================================================================
# DATA PROCESSING
# ============================================================================
