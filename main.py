#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TNEA Cutoff Data Analyzer CLI Tool

This script fetches TNEA cutoff data from the official API,
processes it, and allows users to filter, sort, and export
the data to Excel, PDF, or CSV formats.
"""

import argparse
import json
import sys
import pandas as pd
import requests
from io import BytesIO # Required for xhtml2pdf

# Attempt to import xhtml2pdf for PDF generation
try:
    from xhtml2pdf import pisa
except ImportError:
    pisa = None
    print("Warning: xhtml2pdf library not found. PDF export will not be available. "
          "Install it with 'pip install xhtml2pdf'.", file=sys.stderr)

# --- Configuration ---
BASE_URL = "https://cutoff.tneaonline.org/api/auth/glist/"
YEAR_TO_API_CODE = {
    2024: '1C',
    2023: '2C',
    2022: '3C',
    2021: '4C',
    2020: '5C'
}
API_CODE_TO_YEAR = {v: k for k, v in YEAR_TO_API_CODE.items()}

# Define the columns and their new names for the DataFrame
COLUMN_MAPPING = {
    'coc': 'College Code',
    'con': 'College Name',
    'brc': 'Branch Code',
    'brn': 'Branch Name',
    'OC': 'OC Cutoff',
    'BC': 'BC Cutoff',
    'BCM': 'BCM Cutoff',
    'MBC': 'MBC Cutoff',
    'SC': 'SC Cutoff',
    'SCA': 'SCA Cutoff',
    'ST': 'ST Cutoff',
    'octl': 'OC Total Seats',
    'ocal': 'OC Allotted Seats',
    'bctl': 'BC Total Seats',
    'bcal': 'BC Allotted Seats',
    'bcmtl': 'BCM Total Seats',
    'bcmal': 'BCM Allotted Seats',
    'mbctl': 'MBC Total Seats',
    'mbcal': 'MBC Allotted Seats',
    'sctl': 'SC Total Seats',
    'scal': 'SC Allotted Seats',
    'scatl': 'SCA Total Seats',
    'scaal': 'SCA Allotted Seats',
    'sttl': 'ST Total Seats',
    'stal': 'ST Allotted Seats'
}

CUTOFF_COLUMNS = ['OC Cutoff', 'BC Cutoff', 'BCM Cutoff', 'MBC Cutoff', 'SC Cutoff', 'SCA Cutoff', 'ST Cutoff']

# --- Helper Functions ---

def fetch_tnea_data(year_api_code: str) -> list:
    """
    Fetches TNEA cutoff data for a given year API code.

    Args:
        year_api_code (str): The API code for the year (e.g., '1C', '2C').

    Returns:
        list: A list of dictionaries containing the cutoff data, or None if an error occurs.
    """
    url = f"{BASE_URL}{year_api_code}"
    try:
        print(f"Fetching data from: {url}")
        response = requests.get(url, timeout=30) # Added timeout
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - URL: {url}", file=sys.stderr)
        if response.status_code == 401:
             print("This might be due to an authorization issue or an invalid API endpoint.", file=sys.stderr)
        elif response.status_code == 404:
             print("The requested resource was not found. Check the year/API code.", file=sys.stderr)
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err} - URL: {url}", file=sys.stderr)
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err} - URL: {url}", file=sys.stderr)
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request: {req_err} - URL: {url}", file=sys.stderr)
    except json.JSONDecodeError as json_err:
        print(f"Failed to decode JSON response: {json_err}. Response text: {response.text[:200]}...", file=sys.stderr)
    return None

def process_data(raw_data: list) -> pd.DataFrame:
    """
    Processes the raw JSON data into a pandas DataFrame.
    - Removes the '_id' column.
    - Renames columns to be more user-friendly.
    - Converts cutoff columns to numeric types, coercing errors.

    Args:
        raw_data (list): The raw data fetched from the API.

    Returns:
        pd.DataFrame: A processed DataFrame.
    """
    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)

    # Remove the '_id' column if it exists
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])

    # Rename columns
    df = df.rename(columns=COLUMN_MAPPING)

    # Ensure all defined columns exist, add if missing (with NaN)
    for new_col_name in COLUMN_MAPPING.values():
        if new_col_name not in df.columns:
            df[new_col_name] = pd.NA # Use pandas NA for missing values

    # Convert cutoff columns to numeric, handling potential errors (e.g., empty strings for ST)
    for col in CUTOFF_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert seat count columns to numeric (integer), handling potential errors
    seat_cols = [col for col in COLUMN_MAPPING.values() if 'Seats' in col and col not in CUTOFF_COLUMNS]
    for col in seat_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64') # Use Int64 to support NaN

    # Reorder columns to match COLUMN_MAPPING order + any extra ones at the end
    ordered_columns = [col for col in COLUMN_MAPPING.values() if col in df.columns]
    extra_columns = [col for col in df.columns if col not in ordered_columns]
    df = df[ordered_columns + extra_columns]

    return df

def apply_filters(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    """
    Applies filters to the DataFrame based on CLI arguments.

    Args:
        df (pd.DataFrame): The DataFrame to filter.
        args (argparse.Namespace): Parsed command-line arguments.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    if df.empty:
        return df

    # College Code filter
    if args.filter_college_code:
        df = df[df['College Code'].astype(str) == str(args.filter_college_code)]

    # College Name filter (case-insensitive, contains)
    if args.filter_college_name:
        df = df[df['College Name'].str.contains(args.filter_college_name, case=False, na=False)]

    # Branch Code filter
    if args.filter_branch_code:
        df = df[df['Branch Code'].str.upper() == args.filter_branch_code.upper()]

    # Branch Name filter (case-insensitive, contains)
    if args.filter_branch_name:
        df = df[df['Branch Name'].str.contains(args.filter_branch_name, case=False, na=False)]

    # Min/Max Cutoff filters for each community
    for community_key, cutoff_col_name in [
        ('oc', 'OC Cutoff'), ('bc', 'BC Cutoff'), ('bcm', 'BCM Cutoff'),
        ('mbc', 'MBC Cutoff'), ('sc', 'SC Cutoff'), ('sca', 'SCA Cutoff'), ('st', 'ST Cutoff')
    ]:
        min_cutoff_arg = getattr(args, f"min_{community_key}_cutoff", None)
        max_cutoff_arg = getattr(args, f"max_{community_key}_cutoff", None)

        if min_cutoff_arg is not None:
            df = df[df[cutoff_col_name] >= min_cutoff_arg]
        if max_cutoff_arg is not None:
            df = df[df[cutoff_col_name] <= max_cutoff_arg]
            
    return df

def apply_sorting(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    """
    Applies sorting to the DataFrame based on CLI arguments.

    Args:
        df (pd.DataFrame): The DataFrame to sort.
        args (argparse.Namespace): Parsed command-line arguments.

    Returns:
        pd.DataFrame: The sorted DataFrame.
    """
    if df.empty or not args.sort_by:
        return df

    sort_column_actual_name = None
    # Find the actual column name (case-insensitive partial match for user-friendliness)
    # First, try exact match with mapped names
    if args.sort_by in COLUMN_MAPPING.values():
        sort_column_actual_name = args.sort_by
    else: # Try partial case-insensitive match
        for k, v in COLUMN_MAPPING.items():
            if args.sort_by.lower() in v.lower() or args.sort_by.lower() in k.lower() :
                sort_column_actual_name = v
                break
        if not sort_column_actual_name: # Fallback to check original keys if user used one
             if args.sort_by in COLUMN_MAPPING.keys():
                 sort_column_actual_name = COLUMN_MAPPING[args.sort_by]


    if not sort_column_actual_name or sort_column_actual_name not in df.columns:
        print(f"Warning: Sort column '{args.sort_by}' not found. Available columns: {', '.join(df.columns)}", file=sys.stderr)
        return df

    ascending_order = args.sort_order == 'asc'
    try:
        # When sorting by cutoff, NaNs should ideally be last
        na_position = 'last' if ascending_order else 'first' # For descending, NaNs first might be better
        if sort_column_actual_name in CUTOFF_COLUMNS:
             df = df.sort_values(by=sort_column_actual_name, ascending=ascending_order, na_position=na_position)
        else: # For text or other numeric columns
             df = df.sort_values(by=sort_column_actual_name, ascending=ascending_order, na_position='last')
    except Exception as e:
        print(f"Error sorting by column '{sort_column_actual_name}': {e}", file=sys.stderr)
    return df

def save_to_excel(df: pd.DataFrame, filename: str):
    """Saves the DataFrame to an Excel file."""
    try:
        # Fill NaN values with 'N/A' for better readability in Excel
        df_display = df.fillna('N/A')
        df_display.to_excel(filename, index=False, engine='openpyxl')
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving to Excel file {filename}: {e}", file=sys.stderr)

def save_to_csv(df: pd.DataFrame, filename: str):
    """Saves the DataFrame to a CSV file."""
    try:
        # Fill NaN values with empty string for CSV or 'N/A'
        df_display = df.fillna('') # Or 'N/A'
        df_display.to_csv(filename, index=False)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving to CSV file {filename}: {e}", file=sys.stderr)

def save_to_pdf(df: pd.DataFrame, filename: str):
    """Saves the DataFrame to a PDF file."""
    if pisa is None:
        print("PDF export is unavailable because xhtml2pdf is not installed.", file=sys.stderr)
        return

    try:
        # Fill NaN values for display
        df_display = df.fillna('N/A')
        
        # Basic HTML styling for the PDF table
        html_string = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                @page {{ 
                    size: A4 landscape; 
                    @frame header_frame {{
                        -pdf-frame-content: header_content;
                        left: 50pt; width: 512pt; top: 20pt; height: 50pt;
                    }}
                     @frame content_frame {{
                        left: 20pt; width: 780pt; top: 90pt; height: 480pt; /* Adjusted for landscape */
                    }}
                    @frame footer_frame {{
                        -pdf-frame-content: footer_content;
                        left: 50pt; width: 512pt; top: 772pt; height: 20pt;
                    }}
                }}
                body {{ font-family: "Helvetica", "Arial", sans-serif; font-size: 8pt; }}
                table {{ width: 100%; border-collapse: collapse; }}
                th, td {{ border: 1px solid #dddddd; text-align: left; padding: 4px; }}
                th {{ background-color: #f2f2f2; }}
                .header {{ text-align: center; font-size: 12pt; }}
                .footer {{ text-align: center; font-size: 7pt; }}
            </style>
        </head>
        <body>
            <div id="header_content" class="header">TNEA Cutoff Data</div>
            {df_display.to_html(index=False, border=0)}
            <div id="footer_content" class="footer">Page <pdf:pagenumber /> of <pdf:pagecount /></div>
        </body>
        </html>
        """
        
        with open(filename, "wb") as pdf_file:
            pisa_status = pisa.CreatePDF(BytesIO(html_string.encode("UTF-8")), dest=pdf_file, encoding='UTF-8')

        if not pisa_status.err:
            print(f"Data successfully saved to {filename}")
        else:
            print(f"Error creating PDF: {pisa_status.err}", file=sys.stderr)

    except Exception as e:
        print(f"Error saving to PDF file {filename}: {e}", file=sys.stderr)


def list_unique_values(df: pd.DataFrame, column_name: str, display_name: str):
    """Lists unique values from a specified column."""
    if df.empty or column_name not in df.columns:
        print(f"{display_name} data is not available or column '{column_name}' is missing.")
        return
    
    unique_items = df[[COLUMN_MAPPING.get('coc', 'College Code'), column_name]].drop_duplicates().sort_values(by=column_name)
    print(f"\n--- Unique {display_name} (with College Codes if applicable) ---")
    if column_name == COLUMN_MAPPING.get('con', 'College Name'): # College Name
        for _, row in unique_items.iterrows():
             print(f"  Code: {row[COLUMN_MAPPING.get('coc', 'College Code')]} - Name: {row[column_name]}")
    elif column_name == COLUMN_MAPPING.get('brn', 'Branch Name'): # Branch Name
        unique_branches = df[[COLUMN_MAPPING.get('brc','Branch Code'), column_name]].drop_duplicates().sort_values(by=column_name)
        for _, row in unique_branches.iterrows():
            print(f"  Code: {row[COLUMN_MAPPING.get('brc','Branch Code')]} - Name: {row[column_name]}")
    else: # Generic case
        for item in df[column_name].unique():
            print(f"  {item}")
    print("--------------------------------------------------")

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="TNEA Cutoff Data Analyzer CLI Tool.")
    
    # --- Required Arguments ---
    parser.add_argument(
        "--year",
        type=int,
        choices=YEAR_TO_API_CODE.keys(),
        required=True,
        help="Year for which to fetch data (e.g., 2023)."
    )

    # --- Output Arguments (conditionally required) ---
    parser.add_argument(
        "--output-file",
        type=str,
        help="Path to the output file (e.g., data.xlsx, data.pdf, data.csv). Required if not using --list-* actions."
    )
    parser.add_argument(
        "--format",
        choices=['excel', 'pdf', 'csv'],
        default='excel',
        help="Format of the output file (default: excel)."
    )

    # --- Action Arguments (alternative to output) ---
    parser.add_argument(
        "--list-colleges",
        action="store_true",
        help="List all unique college names and codes for the specified year and exit."
    )
    parser.add_argument(
        "--list-branches",
        action="store_true",
        help="List all unique branch names and codes for the specified year and exit."
    )
    parser.add_argument(
        "--list-sortable-columns",
        action="store_true",
        help="List all column names that can be used for sorting and exit."
    )


    # --- Filtering Arguments ---
    filter_group = parser.add_argument_group('Filtering Options')
    filter_group.add_argument("--filter-college-code", type=str, help="Filter by exact College Code (e.g., 1).")
    filter_group.add_argument("--filter-college-name", type=str, help="Filter by College Name (case-insensitive, contains).")
    filter_group.add_argument("--filter-branch-code", type=str, help="Filter by exact Branch Code (e.g., CS).")
    filter_group.add_argument("--filter-branch-name", type=str, help="Filter by Branch Name (case-insensitive, contains).")
    
    for comm_code, comm_name in [('oc', 'OC'), ('bc', 'BC'), ('bcm', 'BCM'), ('mbc', 'MBC'), ('sc', 'SC'), ('sca', 'SCA'), ('st', 'ST')]:
        filter_group.add_argument(f"--min-{comm_code}-cutoff", type=float, help=f"Minimum {comm_name} cutoff mark.")
        filter_group.add_argument(f"--max-{comm_code}-cutoff", type=float, help=f"Maximum {comm_name} cutoff mark.")

    # --- Sorting Arguments ---
    sort_group = parser.add_argument_group('Sorting Options')
    sort_group.add_argument(
        "--sort-by",
        type=str,
        help=f"Column name to sort by (e.g., 'Branch Name', 'OC Cutoff'). Use --list-sortable-columns to see options."
    )
    sort_group.add_argument(
        "--sort-order",
        choices=['asc', 'desc'],
        default='asc',
        help="Sort order: 'asc' for ascending, 'desc' for descending (default: asc)."
    )
    
    args = parser.parse_args()

    if args.list_sortable_columns:
        print("--- Available columns for sorting (use these names with --sort-by): ---")
        for original, mapped in COLUMN_MAPPING.items():
            print(f"  - '{mapped}' (or try '{original}')")
        print("--------------------------------------------------------------------")
        sys.exit(0)

    # Validate that output_file is provided if not a list action
    is_list_action = args.list_colleges or args.list_branches
    if not is_list_action and not args.output_file:
        parser.error("--output-file is required unless using --list-colleges or --list-branches.")


    year_api_code = YEAR_TO_API_CODE.get(args.year)
    if not year_api_code:
        print(f"Error: Invalid year {args.year}. Supported years: {', '.join(map(str, YEAR_TO_API_CODE.keys()))}", file=sys.stderr)
        sys.exit(1)

    raw_data = fetch_tnea_data(year_api_code)
    if raw_data is None:
        print(f"Failed to fetch data for year {args.year}. Exiting.", file=sys.stderr)
        sys.exit(1)
    if not raw_data:
        print(f"No data found for year {args.year}. This might be an API issue or data not yet available.", file=sys.stderr)
        sys.exit(0)


    df = process_data(raw_data)
    if df.empty:
        print("No data to process after initial fetch and processing. Exiting.", file=sys.stderr)
        sys.exit(0)

    # Handle list actions
    if args.list_colleges:
        list_unique_values(df, COLUMN_MAPPING.get('con', 'College Name'), "Colleges")
        sys.exit(0)
    
    if args.list_branches:
        list_unique_values(df, COLUMN_MAPPING.get('brn', 'Branch Name'), "Branches")
        sys.exit(0)

    # Apply filters and sorting for data export
    df_filtered = apply_filters(df.copy(), args) # Use copy to avoid modifying original df for multiple operations
    if df_filtered.empty:
        print("No data matches the applied filters.", file=sys.stderr)
        sys.exit(0)

    df_sorted = apply_sorting(df_filtered.copy(), args)


    # Save the data
    if args.output_file:
        if args.format == 'excel':
            save_to_excel(df_sorted, args.output_file)
        elif args.format == 'csv':
            save_to_csv(df_sorted, args.output_file)
        elif args.format == 'pdf':
            if pisa:
                save_to_pdf(df_sorted, args.output_file)
            else:
                print("PDF generation skipped as xhtml2pdf is not available.", file=sys.stderr)
                print("Consider saving to Excel or CSV instead, or install xhtml2pdf.", file=sys.stderr)

if __name__ == "__main__":
    main()
