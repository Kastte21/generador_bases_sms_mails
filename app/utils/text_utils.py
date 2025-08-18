# app/utils/text_utils.py
import polars as pl

def to_proper_case(series: pl.Series) -> pl.Series:
    return series.str.to_titlecase()