import pandas._testing as tm
from helpers.processors import *


def test_create_schema_():
    df = tm.makeMixedDataFrame()
    expected = {"A": "float64", "B": "float64", "C": "object", "D": "datetime64[ns]"}
    actual = create_schema(df)
    assert expected == actual
