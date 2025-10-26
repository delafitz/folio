import polars as pl
import typer

from etfs import FACTORS, INDICES, get_etf_groups
from hist import get_returns
from model import run_opt

pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_hide_column_data_types(True)
app = typer.Typer()


def get_scenarios(target):
    returns = {
        name: get_returns(name, symbols)
        for name, symbols in (
            get_etf_groups() | {target: [target]}
        ).items()
    }
    # ┌────────────┬───────────┬───────────┬───────────┐
    # │ date       ┆ SPY       ┆ QQQ       ┆ IWM       │
    # │ ---        ┆ ---       ┆ ---       ┆ ---       │
    # │ str        ┆ f32       ┆ f32       ┆ f32       │
    # ╞════════════╪═══════════╪═══════════╪═══════════╡
    # │ 2024-10-28 ┆ 0.003091  ┆ 0.000161  ┆ 0.01631   │
    # │ 2024-10-29 ┆ 0.001618  ┆ 0.009608  ┆ -0.003237 │
    # │ 2024-10-30 ┆ -0.003025 ┆ -0.007558 ┆ -0.001353 │
    # └────────────┴───────────┴───────────┴───────────┘
    print(returns)
    if not returns[target].is_empty():
        return {
            'index_only': [returns[key] for key in [INDICES, target]],
            'all': [
                returns[key] for key in [INDICES, FACTORS, target]
            ],
        }


def join(returns):
    return pl.concat(returns, how='align_left').select(
        pl.all().exclude('date')
    )
    # ┌───────────┬───────────┬───────────┬───────────┐
    # │ SPY       ┆ QQQ       ┆ IWM       ┆ jpm       │
    # │ ---       ┆ ---       ┆ ---       ┆ ---       │
    # │ f32       ┆ f32       ┆ f32       ┆ f32       │
    # ╞═══════════╪═══════════╪═══════════╪═══════════╡
    # │ 0.003091  ┆ 0.000161  ┆ 0.01631   ┆ 0.014349  │
    # │ 0.001618  ┆ 0.009608  ┆ -0.003237 ┆ -0.01153  │
    # │ 0.004221  ┆ 0.007399  ┆ 0.005603  ┆ 0.004596  │
    # └───────────┴───────────┴───────────┴───────────┘


def get_opts(scenarios, target):
    return {
        name: run_opt(join(returns), target)
        for name, returns in scenarios.items()
    }


@app.command()
def prompt():
    while True:
        symbol = typer.prompt('symbol')
        scenarios = get_scenarios(symbol)
        if scenarios:
            baskets = get_opts(scenarios, symbol)
            for name, basket in baskets.items():
                print(f'({name})')
                print(basket)


# uv run -m folio SYMBOL
if __name__ == '__main__':
    app()
