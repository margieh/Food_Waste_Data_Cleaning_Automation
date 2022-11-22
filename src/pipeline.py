import argparse
import hashlib
import logging
import os
import sys
from pathlib import Path


import pandas as pd
import emoji

from helpers.calculations import (
    calc_tons_never_harvested,
    calc_tons_never_harvested_by_cause,
)
from helpers.processors import create_schema, save_df_to_path

logging.basicConfig(
    format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S", level=logging.INFO
)


FARM_DATA_SCHEMA = {
    "year": "int64",
    "state": "object",
    "commodity_name": "object",
    "refed_food_department": "object",
    "refed_food_category": "object",
    "acres_planted": "float64",
    "acres_harvested": "float64",
    "us_dollars_harvested": "float64",
    "tons_harvested": "float64",
    "percent_maturity": "float64",
}

CAUSES_DATA_SCHEMA = {"cause": "object", "rate": "float64"}
CAUSES_DATA_PATH = "causes/farm_not_harvested_causes.csv"

FARM_DATA_REQ_COLS = [
    "year",
    "state",
    "commodity_name",
    "acres_planted",
    "acres_harvested",
    "tons_harvested",
    "us_dollars_harvested",
    "percent_maturity",
]

# these will be concatenated and hashed to create a unique id for each row of data
HASH_COL_LS = ["year", "state", "commodity_name"]


class CleaningPipeline:
    def __init__(self, path, schema):
        self.path = path
        self.filename = os.path.split(path)[1]
        self.schema = schema

    def open_csv(self):
        try:
            df = pd.read_csv(self.path, index_col=0)
            logging.info("Reading data from %s.", self.filename)
            return df

        except FileNotFoundError as e:
            raise e

    def validate_farm_data_schema(self, df):
        """Compares schemas (dict) with col_name: dtype."""
        df_schema = create_schema(df)
        if not self.schema == df_schema:
            logging.info(
                "%s failed validation with schema %s\
                    . File schema %s",
                self.filename,
                self.schema,
                df_schema,
            )
            return None
        logging.info("%s passed validation with schema %s", self.filename, self.schema)
        return df

    def lowercase_str_cols(self, df):
        logging.info("Converting strings to lower.")
        return df.applymap(lambda s: s.lower() if type(s) == str else s)

    def drop_nan_rows_in_required_cols(self, df):
        logging.info("Dropping rows with nan/null in required cols.")
        return df.dropna(axis=0, subset=FARM_DATA_REQ_COLS)

    def make_hash_id(self, df, hash_col_ls):
        """Creates a hashlib.sha512 from columns which should define a row as unique."""
        req_cols_df = df.filter(hash_col_ls, axis=1)
        uid = req_cols_df.apply(lambda row: "_".join(row.values.astype(str)), axis=1)
        uid_hashed = uid.str.encode("utf-8").apply(
            lambda x: (hashlib.sha512(x).hexdigest().upper())
        )
        df["uid"] = uid_hashed
        logging.info("uid hashes created for rows in dataset.")

        return df

    def remove_dups(self, df):
        logging.info("Checking for and removing duplicates rows.")
        return df.drop_duplicates(subset="uid", keep="first")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-subdir", "--subdirectory", help="enter subdirectory name")
    parser.add_argument(
        "-merge",
        "--merge_all_dir_files",
        default=False,
        help="merge all files at directory",
    )

    params = parser.parse_args()
    subdir = params.subdirectory
    merge = params.merge_all_dir_files

    for req_dir in ["raw", "cleaned", "production"]:
        if not os.path.exists(req_dir):
            os.makedirs(req_dir)

    directory_name = subdir

    open_files = Path(directory_name).glob("*")
    for file in open_files:
        print("Starting validation process for %s. To exit press CTRL+C", file)
        fname = os.path.split(file)[1]
        pipeline = CleaningPipeline(
            file,
            FARM_DATA_SCHEMA,
        )

        cleaned_data = (
            pipeline.open_csv()
            .pipe(pipeline.validate_farm_data_schema)
            .pipe(pipeline.lowercase_str_cols)
            .pipe(pipeline.drop_nan_rows_in_required_cols)
            .pipe(pipeline.make_hash_id, HASH_COL_LS)
            .pipe(pipeline.remove_dups)
        )

        save_df_to_path(cleaned_data, fname, "cleaned", "v1")
        logging.info(
            "Data cleaning for %s complete! %s",
            fname,
            emoji.emojize(":grinning_face_with_big_eyes:"),
        )

        logging.info('Calculating "tons never harvested" for %s', fname)
        df_tnh = calc_tons_never_harvested(cleaned_data)

        causes_data = pd.read_csv(CAUSES_DATA_PATH)
        if not create_schema(causes_data) == CAUSES_DATA_SCHEMA:
            logging.critical(
                "Please update causes file data schema to match expected input: %s",
                CAUSES_DATA_SCHEMA,
            )
            sys.exit(0)

        logging.info('Calculating "tons never harvested" by cause for %s', fname)
        prod_df = calc_tons_never_harvested_by_cause(df_tnh, causes_data)

        save_df_to_path(prod_df, fname, "production", "v1")
        logging.info(
            "Data processing for %s complete! %s", fname, emoji.emojize(":rocket:")
        )

    if merge:
        os.chdir("production/")
        file_list = os.listdir(os.getcwd())
        combined_csv = pd.concat([pd.read_csv(f) for f in file_list])
        save_df_to_path(combined_csv, "combined.csv", os.getcwd(), "v2")
        logging.info(
            "All files processed. Concatenations complete! %s",
            emoji.emojize(":rocket:") * 10,
        )


if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


# python3 pipeline.py -subdir raw
# python3 pipeline.py -subdir raw -merge True
