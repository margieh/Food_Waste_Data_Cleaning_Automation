from datetime import datetime


def create_schema(df):
    """Creates a schema: dict with col_name: dtype."""

    schema = df.dtypes.map(lambda x: x.name).to_dict()
    return schema


def save_df_to_path(df, in_file, out_dir, ftype):
    timestamp = datetime.now()
    out_file_name = in_file.split(".")[0]
    df.to_csv(
        f'{out_dir}/{str(timestamp.strftime("%Y_%m_%d_%H:%M:%S"))}_{out_file_name}_{ftype}.csv',
        index=False,
    )
