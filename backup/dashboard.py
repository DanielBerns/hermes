# scripts/dashboard.py
from pathlib import Path
import logging

import asyncio  # Import the asyncio library
from nicegui import ui
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# from hermes.message_board.agent import initialize_agent, SendPublicMessage
from hermes.core.helpers import get_directory, get_resource
from hermes.core.services import Services
from hermes.domain.sample import Sample
from hermes.domain.session import get_session

from hermes.reporting.reports import (
    get_report_by_tag,
    get_report_by_brand,
    get_report_brand_competition,
)

filename = Path(__file__)
script, project_identifier = filename.stem, filename.parents[1].stem
services = Services(script, project_identifier)

logger = logging.getLogger(__name__)
logger.info(f"Starting {script}")

mecon_container = services.storage.container(Sample.MECON)

db_container = services.storage.container(Sample.DATABASE, base=mecon_container)
db_uri = str(get_resource(db_container, services.database_name, ".db"))

# --- Report Configuration ---
REPORT_TYPES = {
    "View by Product Tag": get_report_by_tag,
    "View by Brand": get_report_by_brand,
    "Brand Competition Analysis": get_report_brand_competition,
}

# --- UI Application ---
@ui.page("/")
def dashboard():
    # --- Help Dialog ---
    with ui.dialog() as help_dialog, ui.card():
        ui.label("Help/About").classes("text-h6")
        ui.markdown(
            """
            This dashboard provides several reports to analyze the product data from the Hermes database.

            **Report Types:**

            - **View by Product Tag:** This report shows a hierarchical view of all product tags. For each tag, it lists the brands associated with it and the specific products that link them.
            - **View by Brand:** This report provides a brand-centric view, showing all product tags associated with each brand and the products that create the link.
            - **Brand Competition Analysis:** This report allows you to analyze brand competition. Enter a brand name to see which other brands are associated with the same product tags.

            **How to Use:**

            1.  Select a report type from the dropdown menu in the left panel.
            2.  If you choose "Brand Competition Analysis," enter a brand name in the text field that appears.
            3.  Click the "Generate Report" button to view the report in the main content area.
            """
        )
        ui.button("Close", on_click=help_dialog.close)

    # --- Header ---
    with ui.header(elevated=True).style("background-color: #3874c8").classes(
        "items-center justify-between"
    ):
        ui.label("Hermes Reporting Dashboard")
        ui.button("?", on_click=help_dialog.open).props("icon=question_mark")

    # --- Left Drawer (Controls) ---
    with ui.left_drawer().style("background-color: #f2f2f2"):
        with ui.column().classes("p-4"):
            ui.label("Controls").classes("text-h6")
            report_select = ui.select(
                list(REPORT_TYPES.keys()), value="View by Product Tag", label="Report Type"
            )
            brand_input = ui.input("Enter Brand Name").bind_visibility_from(
                report_select, "value", value="Brand Competition Analysis"
            )
            ui.button("Generate Report", on_click=lambda: generate_report())

    # --- Main Content Area ---
    report_area = ui.column().classes("w-full p-4")
    with report_area:
        ui.label("Please select a report from the panel and click 'Generate.'")

    # --- Report Generation Logic (Now Asynchronous) ---
    async def generate_report():
        # Capture UI values before starting the async task
        report_name = report_select.value
        brand_name_value = brand_input.value

        # Clear previous report and show a loading spinner for better UX
        report_area.clear()
        with report_area:
            ui.spinner(size="lg").classes("self-center")

        # Define the function that runs the blocking database query
        def get_data_from_db():
            with get_session(db_uri) as session:
                report_function = REPORT_TYPES[report_name]
                if report_name == "Brand Competition Analysis":
                    # Pass the captured brand name
                    return report_function(session, brand_name_value)
                else:
                    return report_function(session)

        # Run the blocking function in a separate thread to avoid UI timeout
        data = await asyncio.to_thread(get_data_from_db)

        # Clear the spinner and display the report
        report_area.clear()

        with report_area:
            if not data:
                ui.label(f"No data found for '{report_name}'.").classes("text-h6")
                if report_name == "Brand Competition Analysis" and brand_name_value:
                    ui.label(f"Check if the brand '{brand_name_value}' exists.")
                return

            ui.label(report_name).classes("text-h6")
            if report_name in ["View by Product Tag", "View by Brand"]:
                ui.tree(
                    [
                        {
                            "id": key1,
                            "label": key1,
                            "children": [
                                {
                                    "id": f"{key1}-{key2}",
                                    "label": key2,
                                    "children": [
                                        {"id": f"{key1}-{key2}-{item}", "label": item}
                                        for item in items
                                    ],
                                }
                                for key2, items in value1.items()
                            ],
                        }
                        for key1, value1 in data.items()
                    ],
                    label_key="label",
                    children_key="children",
                )
            else:  # Brand Competition Analysis
                ui.tree(
                    [
                        {
                            "id": tag,
                            "label": tag,
                            "children": [
                                {"id": f"{tag}-{brand}", "label": brand} for brand in brands
                            ],
                        }
                        for tag, brands in data.items()
                    ],
                    label_key="label",
                    children_key="children",
                )


# --- Run the application ---
ui.run()
