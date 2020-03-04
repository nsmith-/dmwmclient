import pandas


def format_dates(df, columns):
    """Convert UNIX timestamp columns to datetime"""
    if df.size > 0:
        df[columns] = df[columns].apply(lambda v: pandas.to_datetime(v, unit="s"))
    return df
