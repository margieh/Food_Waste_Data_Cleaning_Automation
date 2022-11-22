import os
import pandas as pd
import pytest
from pipeline import CleaningPipeline, HASH_COL_LS, FARM_DATA_SCHEMA


df = pd.DataFrame(
    columns=[
        "",
        "year",
        "state",
        "commodity_name",
        "refed_food_department",
        "refed_food_category",
        "acres_planted",
        "acres_harvested",
        "us_dollars_harvested",
        "tons_harvested",
        "percent_maturity",
    ]
)
df.loc[0] = [
    2202,
    2016,
    "Alabama",
    "PECANS",
    "Dry goods",
    "Nuts and seeds",
    8900.0,
    8900.0,
    4467000.0,
    1100.0,
    0.5,
]
df.loc[1] = [
    2203,
    2016,
    "Alabama",
    "PEANUTS",
    "Dry goods",
    "Nuts and seeds",
    175000.0,
    172000.0,
    121982000.0,
    309600.0,
    0.5,
]
df.loc[2] = [
    2203,
    None,
    "Alabama",
    "PEANUTS",
    "Dry goods",
    "Nuts and seeds",
    175000.0,
    172000.0,
    121982000.0,
    309600.0,
    0.5,
]


df2 = pd.DataFrame(
    columns=[
        "",
        "year",
        "state",
        "commodity_name",
        "refed_food_department",
        "refed_food_category",
        "acres_planted",
        "acres_harvested",
        "us_dollars_harvested",
        "tons_harvested",
        "percent_maturity",
    ]
)
df2.loc[0] = [
    "2202",
    "2016",
    "Alabama",
    "PECANS",
    "Dry goods",
    "Nuts and seeds",
    "8900.0",
    "8900.0",
    "4467000.0",
    "1100.0",
    "0.5",
]
df2.loc[1] = [
    "2203",
    "2016",
    "Alabama",
    "PEANUTS",
    "Dry goods",
    "Nuts and seeds",
    "175000.0",
    "172000.0",
    "121982000.0",
    "309600.0",
    "0.5",
]

PATH = "tests/sample_data.csv"


@pytest.fixture
def cleaningpipeline():
    """Returns a Wallet instance with a balance of 20"""
    return CleaningPipeline(path=PATH, schema=FARM_DATA_SCHEMA)


def test_default_cleaning_pipeline(cleaningpipeline):
    assert cleaningpipeline.schema == FARM_DATA_SCHEMA
    assert cleaningpipeline.path == PATH
    assert cleaningpipeline.filename == os.path.split(PATH)[1]


def test_open_csv_bad_file_ext():
    cp = CleaningPipeline(path="some/place/file.txt", schema=FARM_DATA_SCHEMA)
    with pytest.raises(FileNotFoundError) as e:
        cp.open_csv()


def test_open_csv_good_file_ext(cleaningpipeline):
    res = cleaningpipeline.open_csv()
    assert type(res).__name__ == "DataFrame"


def test_validate_farm_data_returns_none_if_bad_schema(cleaningpipeline):
    df2 = df.copy()
    res = cleaningpipeline.validate_farm_data_schema(df2)
    assert res == None


def test_validate_farm_data_returns_df_if_good_schema(cleaningpipeline):
    df = cleaningpipeline.open_csv()
    res = cleaningpipeline.validate_farm_data_schema(df)
    assert type(res).__name__ == "DataFrame"


def test_lowercase_str_cols_returns_all_lower(cleaningpipeline):
    df3 = df.copy()
    df_lowered = cleaningpipeline.lowercase_str_cols(df3)
    assert df_lowered.iloc[0]["commodity_name"] == "pecans"


def test_drop_nan_rows_in_required_cols_removes_row(cleaningpipeline):
    df4 = df.copy()
    df_2_rows = cleaningpipeline.drop_nan_rows_in_required_cols(df4)
    assert len(df4) != len(df_2_rows)
    assert len(df_2_rows) == 2


def test_make_hash_id_returns_expected_hash(cleaningpipeline):
    df5 = df.copy()
    df_2_rows = cleaningpipeline.drop_nan_rows_in_required_cols(df5)
    df_hashed = cleaningpipeline.make_hash_id(df_2_rows, HASH_COL_LS)
    actual_col_ls = list(df_hashed.columns)
    assert "uid" in actual_col_ls
    assert (
        df_hashed.iloc[0, -1]
        == "874760C4D93DA78A7701471F943FCF9B95E744D42B0660DD2D2A3F2599B5D9F6E46B11926F035A470C9F7FA5B0059E15A67D9C7B2AEC5029D0C37DEDC215594A"
    )


def test_remove_dups(cleaningpipeline):
    dups_df = pd.concat([df] * 3, ignore_index=True)
    df_2_rows = cleaningpipeline.drop_nan_rows_in_required_cols(dups_df)
    df_hashed = cleaningpipeline.make_hash_id(df_2_rows, HASH_COL_LS)
    min_df = cleaningpipeline.remove_dups(df_hashed)
    assert len(dups_df) != len(min_df)
