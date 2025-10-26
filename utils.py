from pprint import pprint as pp
from typing import Dict

import polars as pl

DAILY_ANN = 252**0.5 * 100


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


def calc_risk(target, basket, all_returns):
    basket_weights = dict(
        zip(basket[target].to_list(), basket['weight'].to_list())
    )
    wtd_basket_returns = (
        all_returns.select(list(basket_weights.keys()))
        .with_columns(
            [
                pl.col(symbol) * weight
                for symbol, weight in basket_weights.items()
            ]
        )
        .sum_horizontal()
        .alias('basket')
    )
    print(wtd_basket_returns)
    print(all_returns.select(target))
    # vols = (
    #     pl.DataFrame(
    #         [all_returns.select(target), wtd_basket_returns],
    #     )
    #     .with_columns(
    #         (pl.col(target) - pl.col('basket')).alias('hedged')
    #     )
    #     .select(pl.all())
    #     .std()
    #     * DAILY_ANN
    # )
    # print(vols)
