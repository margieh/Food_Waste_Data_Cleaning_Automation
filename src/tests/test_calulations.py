import pytest
import pandas as pd
from helpers.calculations import *


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


causes_df = pd.DataFrame(columns=["cause", "rate"])
causes_df.loc[0] = ["not_marketable", 0.28]
causes_df.loc[1] = ["inedible", 0.28]
causes_df.loc[2] = ["bad_weather", 0.042]
causes_df.loc[3] = ["pests_disease", 0.00063]
causes_df.loc[4] = ["market_dynamics", 0.00013]
causes_df.loc[5] = ["other", 0.39724]


def test_calc_tons_never_harvested_creates_expected_fields_values():
    df1 = df.copy()
    df_calc = calc_tons_never_harvested(df1)
    actual_cols = list(df_calc.columns)

    assert "acres_unharvested" in actual_cols
    assert "yield_tons_per_acre" in actual_cols
    assert "price_per_ton" in actual_cols
    assert "tons_never_harvested" in actual_cols

    assert df_calc.iloc[0]["tons_never_harvested"] == pytest.approx(0.0, 0.1)
    assert df_calc.iloc[1]["price_per_ton"] == pytest.approx(393.998708, 0.1)


def test_calc_tons_never_harvested_by_cause_creates_expected_fields_values():
    cdf = causes_df.copy()
    df_calc2 = calc_tons_never_harvested(df.copy())
    df_calc_causes = calc_tons_never_harvested_by_cause(df_calc2, cdf)

    actual_cols = list(df_calc_causes.columns)

    assert "not_marketable" in actual_cols
    assert "inedible" in actual_cols
    assert "bad_weather" in actual_cols
    assert "pests_disease" in actual_cols
    assert "market_dynamics" in actual_cols
    assert "other" in actual_cols

    assert df_calc_causes.iloc[0]["not_marketable"] == pytest.approx(0.0, 0.1)
    assert df_calc_causes.iloc[1]["other"] == pytest.approx(1072.548, 0.1)
