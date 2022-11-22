def calc_tons_never_harvested(df):
    df["acres_unharvested"] = df["acres_planted"] - df["acres_harvested"]
    df["yield_tons_per_acre"] = df["tons_harvested"].div(df["acres_harvested"].values)
    df["price_per_ton"] = df["us_dollars_harvested"].div(df["tons_harvested"].values)
    df["tons_never_harvested"] = (
        df["acres_unharvested"] * df["yield_tons_per_acre"] * df["percent_maturity"]
    )
    return df


def calc_tons_never_harvested_by_cause(farm_df, causes_df):
    i = 0
    for i in range(len(causes_df)):
        cause, rate = causes_df.iloc[i]
        farm_df[cause] = farm_df["tons_never_harvested"].apply(lambda x: x * rate)
        i += 1

    return farm_df
