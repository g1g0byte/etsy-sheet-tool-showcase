import os
import re
import sys
import csv
import yaml
import datetime
import traceback
from itertools import chain
from tabulate import tabulate
from collections import Counter
from yaml.loader import SafeLoader
from google.oauth2 import service_account
from googleapiclient.discovery import build


def setup_google_apis():
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    SERVICE_ACCOUNT_FILE = "keys.json"
    creds = None
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)


def get_transactions():
    """Reads CSV files from "FILES HERE" folder into a list of dicts."""
    csv_files_directory = "FILES HERE/"
    transactions_data = []
    for csv_file in os.listdir(csv_files_directory):
        with open(csv_files_directory + csv_file, "r") as file:
            reader = csv.reader(file)
            next(reader, None)
            for row in reader:
                transactions_data.append(
                    {
                        "sale_date": datetime.datetime.strptime(row[0], "%m/%d/%y").strftime('%d/%m/%y'),
                        "item_title": str(row[1]),
                        "item_quantity": int(row[3]),
                        "item_price": float(row[4]),
                        "transaction_id": str(row[13]),
                    }
                )
    print("Transaction data from reading csv files:\n")
    print(tabulate(transactions_data, headers="keys", showindex="always", tablefmt="github"))
    return transactions_data


def format_data_for_sheet1(transactions_data: list):
    """Takes a list of data and creates nested lists containing data for each row in the sheet."""
    # Checks if string contains substring which tells if item is avaliable in both 3oz or 5oz
    regex = rf"\b{config.get('ounce_differentiator')}\b"
    formatted_data = []
    for transaction in transactions_data:
        temp_list = []
        if re.findall(regex, transaction["item_title"]):
            if transaction["item_price"] in config.get("five_ounce_prices"):
                temp_list.append(transaction["item_title"][:-7] + "(5oz)")
            else:
                temp_list.append(transaction["item_title"][:-7] + "(3oz)")
        else:
            temp_list.append(transaction["item_title"])
        temp_list.append(transaction["sale_date"])
        temp_list.append(transaction["transaction_id"])

        if transaction["item_quantity"] > 1:
            for i in range(transaction["item_quantity"]):
                temp_list_copy = temp_list.copy()
                temp_list_copy[2] = temp_list_copy[2] + f" ({i+1})"
                formatted_data.append(temp_list_copy)
        else:
            formatted_data.append(temp_list)
    return formatted_data


def read_sheet_data(range: str):
    """Returns google sheets API call response for reading a sheets values."""
    return (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=range)
        .execute()
    )


def check_for_duplicates(range: str, data_to_add: list):
    """Compares list of data to add vs. data already in the sheet and returns the difference between them."""
    sheet_data = read_sheet_data(range)

    # Check if sheet contains any values at all
    if "values" not in sheet_data:
        return data_to_add
    
    unique_values = [x for x in data_to_add if x not in sheet_data["values"]]

    if not any(unique_values):
        return None
    else:
        return unique_values


def append_to_sheet(body: dict, range: str):
    """Returns google sheets API call response for appending values to a sheet."""
    return  (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=SPREADSHEET_ID,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            range=range,
            body=body,
        )
        .execute()
    )


def write_to_sheet1(data: list):
    """Appends data to sheet1."""
    range = config.get("sheet1_range")
    print("\n\nDo not worry if program appears frozen")
    values = check_for_duplicates(range, data)

    if values == None:
        print("\n\nNo new rows to add to sheet1")
        return
    
    print("\n\nRows being written to sheet 1:\n")
    print(tabulate(values, headers=["sale data", "item title", "transaction id"], showindex="always", tablefmt="github"))

    body = {"values": values}
    result = append_to_sheet(body, range)
    print("\n\nWriting to sheet1 results:", result)


def get_unique_items():
    """Returns a set of every different type of item from sheet1."""
    sheet_data = read_sheet_data(config.get("sheet1_title_range"))
    return set(chain.from_iterable(sheet_data["values"]))


def count_item_sales():
    """Returns a Counter object of how many times each unique item appears in sheet1."""
    sheet_data = read_sheet_data(config.get("sheet1_title_range"))
    return Counter(list(chain.from_iterable(sheet_data["values"])))


def get_sheet2_item_titles():
    """Returns a list of each title in sheet2 in order they appear top to bottom."""
    sheet_data = read_sheet_data(config.get("sheet2_title_range"))
    if "values" not in sheet_data:
        return None
    return list(chain.from_iterable(sheet_data["values"]))


def append_new_item_titles(unique_items: set, sheet2_item_titles: list):
    """Checks for missing titles in sheet2 and appends new item title(s) to sheet2 if found."""
    new_items = []
    for item in unique_items:
        if item not in sheet2_item_titles:
            sheet2_item_titles.append(item)
            new_items.append([item])

    if any(new_items):
        range = config.get("sheet2_title_range")
        body = {"values": new_items}
        result = append_to_sheet(body, range)
        print("\n\nWriting new items to sheet2 result:\n", result)    


def format_data_for_sheet2(item_sales_count: Counter, sheet2_item_titles: list):
    """Takes a list of data and creates nested lists containing data for each row in the sheet."""
    item_data = config.get("item_data")
    formatted_data = []

    for title in sheet2_item_titles:
        row_data = []
        row_data.append(next((item["make_cost"] for item in item_data if item["title"] == title), "ERR: NOT FOUND"))
        row_data.append(next((item["selling_price"] for item in item_data if item["title"] == title), "ERR: NOT FOUND"))
        row_data.append(item_sales_count[title])
        formatted_data.append(row_data)

    return formatted_data


def update_sheet(body: dict, range: str):
    """Returns google sheets API call response for updating values in a sheet."""
    return  (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=SPREADSHEET_ID,
            valueInputOption="USER_ENTERED",
            range=range,
            body=body,
        )
        .execute()
    )

def write_to_sheet2(data: list):
    """Updates data in sheet2."""
    range = config.get("sheet2_range")
    body = {"values": data}
    result = update_sheet(body, range)
    print("\n\nWriting item sales to sheet2 result:\n", result)


def main():
    transactions_data = get_transactions()

    # -------- SHEET1 -------- #
    sheet1_values = format_data_for_sheet1(transactions_data)
    write_to_sheet1(sheet1_values)

    # -------- SHEET2 -------- #
    # Get amount of item sales for each item
    item_sales_count = count_item_sales()

    # Get title of each different type of item
    unique_items = get_unique_items()
    print("\n\nAmount of unique items:", len(unique_items))

    # Get item title from sheet in order they appear
    sheet2_item_titles = get_sheet2_item_titles()

    # Handle if there are no items in sheet 2
    if sheet2_item_titles == None:
        range = config.get("sheet2_title_range")
        body = {"values": [[item] for item in unique_items]}
        result = update_sheet(body, range)

        sheet2_item_titles = unique_items
        print("\n\nNo titles found in sheet 2 so added them result:\n", result)
    else:
        # If there is an item not already on the sheet then add it
        append_new_item_titles(unique_items, sheet2_item_titles)

    sheet2_values = format_data_for_sheet2(item_sales_count, sheet2_item_titles)

    print("\n\nSheet2_item_titles:\n")
    print(tabulate(([x] for x in sheet2_item_titles), headers=["idx", "title"], showindex="always", tablefmt="github"))

    print("\n\nItem_sales_count:\n")
    print(tabulate(([key, value] for key, value in item_sales_count.items()), headers=["idx", "item title", "quant. sold"], showindex="always", tablefmt="github"))

    print("\n\nSheet2 formatted data:\n")
    print(tabulate(sheet2_values, headers=["idx", "make cost", "selling price", "quant. sold"], showindex="always", tablefmt="github"))

    write_to_sheet2(sheet2_values)


if __name__ == "__main__":
    try:
        with open('config.yaml') as file:
            config = yaml.load(file, Loader=SafeLoader)

        SPREADSHEET_ID = config.get("spreadsheet_id")
        service = setup_google_apis()

        main()
    except BaseException:
        print(sys.exc_info()[0])
        print(traceback.format_exc())
    finally:
        input("\nPress Enter to exit ...")
