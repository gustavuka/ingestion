import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
from api_helpers import (
    get_credentials,
    get_merchant_id,
    send_products_info,
    update_merchant,
    delete_store,
)


def prepare_prices_stock(path_to_file: str, branches: tuple) -> DataFrame:
    df = pd.read_csv(path_to_file, sep="|")

    # remove zero and negative stock
    df = df[df["STOCK"] > 0]
    # select only MM and RHSM branches
    df = df[df["BRANCH"].isin(branches)]
    # drop duplicated info
    df = df.drop_duplicates(subset=["SKU", "BRANCH"], keep=False)

    return df


def prepare_products(path_to_file: str) -> DataFrame:
    df = pd.read_csv(path_to_file, sep="|")

    # merge category columns
    df["ALL_CATEGORIES"] = (
        df["CATEGORY"].str.lower()
        + "|"
        + df["SUB_CATEGORY"].str.lower()
        + "|"
        + df["SUB_SUB_CATEGORY"].str.lower()
    )

    # remove unused columns
    drop_columns = [
        "DESCRIPTION_STATUS",
        "ORGANIC_ITEM",
        "KIRLAND_ITEM",
        "CATEGORY",
        "SUB_CATEGORY",
        "SUB_SUB_CATEGORY",
    ]
    df.drop(drop_columns, inplace=True, axis=1)

    # remove html tags
    df["ITEM_DESCRIPTION"] = (
        df["ITEM_DESCRIPTION"].str.replace(r"<[^<>]*>", "", regex=True).str.strip()
    )

    # remove weird ascii characters
    df["ITEM_NAME"] = df["ITEM_NAME"].str.replace("   ", "")

    # get package info from item description when possible
    df["PACKAGE"] = df["ITEM_DESCRIPTION"].str.extract(
        r"(\d[0-9]{1,} .{2,3}|[0-9]{1,}UN|[0-9]{1,}KG|[0-9]{1,}M|\d+GR)", expand=True
    )
    df["PACKAGE"].fillna("", inplace=True)

    return df


def handle_requests(merchant_name: str, dataframes: tuple[DataFrame]) -> dict:
    token = get_credentials(path_to_env="local.env")
    merchant_id = get_merchant_id(merchant_name, token)
    update_merchant(merchant_id, token, "Richards", is_active=True)
    delete_store("Beauty", token)

    errors = {}
    for df in dataframes:
        most_expensive_products_df = df.nlargest(100, "PRICE")
        for row in zip(*most_expensive_products_df.to_dict("list").values()):
            payload = {
                "merchant_id": merchant_id,
                "sku": str(row[0]),
                "barcodes": [str(row[5])],
                "brand": row[10],
                "name": row[7],
                "description": row[8],
                "package": row[12],
                "image_url": row[9],
                "category": row[11],
                "url": "",
                "branch_products": [
                    {
                        "branch": row[1],
                        "stock": row[3],
                        "price": row[2],
                    },
                ],
            }
            response = send_products_info(payload, token)
            if response.status_code != 200:
                error_id = len(errors) + 1
                errors[error_id] = {payload: payload, response: response.json()}

    return errors


def process_csv_files(
    merchant_name: str, path_to_prices_stock_csv: str, path_to_products_csv: str
) -> dict:
    df_prices = prepare_prices_stock(path_to_prices_stock_csv, ["MM", "RHSM"])
    df_products = prepare_products(path_to_products_csv)

    df = pd.merge(df_prices, df_products, on="SKU", how="left")

    df_RHSM, df_MM = [x for _, x in df.groupby(df["BRANCH"] == "MM")]

    return handle_requests(merchant_name, (df_RHSM, df_MM))


if __name__ == "__main__":
    # files need to be downloaded and placed within the richart folder
    # otherwise just update the file paths
    process_csv_files("Richard's", "PRICES-STOCK.csv", "PRODUCTS.csv")
