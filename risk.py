import polars as pl

DAILY_ANN = 252**0.5 * 100


def calc_vols(target, basket, all_returns):
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
    vols = (
        pl.DataFrame(
            [all_returns.get_column(target), wtd_basket_returns],
        )
        .with_columns(
            (pl.col(target) - pl.col('basket')).alias('hedged')
        )
        .select(pl.all())
        .std()
        * DAILY_ANN
    ).with_columns(
        (1 - pl.col('hedged') / pl.col(target)).alias('reduction')
    )
    return vols
