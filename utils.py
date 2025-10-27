from pprint import pprint as pp
from typing import Dict

import polars as pl


def to_dicts(dfs: Dict):
    all = pl.DataFrame(
        [dict(df.rows()) for df in dfs.values()],
        orient='row',
        schema=dfs.keys(),
    )
    pp(all.to_dicts())


def display(dfs: Dict):
    pl.Config.set_tbl_hide_dataframe_shape(True)
    for name, df in dfs.items():
        print(f'({name})')
        print(df)


def join(returns):
    # drop nulls to account for shorter hists
    return pl.concat(returns, how='align_left').drop_nulls()
    # ┌───────────┬───────────┬───────────┬───────────┐
    # │ SPY       ┆ QQQ       ┆ IWM       ┆ jpm       │
    # │ ---       ┆ ---       ┆ ---       ┆ ---       │
    # │ f32       ┆ f32       ┆ f32       ┆ f32       │
    # ╞═══════════╪═══════════╪═══════════╪═══════════╡
    # │ 0.003091  ┆ 0.000161  ┆ 0.01631   ┆ 0.014349  │
    # │ 0.001618  ┆ 0.009608  ┆ -0.003237 ┆ -0.01153  │
    # │ 0.004221  ┆ 0.007399  ┆ 0.005603  ┆ 0.004596  │
    # └───────────┴───────────┴───────────┴───────────┘
