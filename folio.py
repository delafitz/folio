import polars as pl
import typer

from etfs import FACTORS, INDICES, get_etf_groups
from hist import get_returns
from model import run_opt
from utils import calc_risk, display, join

pl.Config.set_tbl_hide_column_data_types(True)
app = typer.Typer()


def get_scenarios(target):
    # get hist returns for index/factor etf baskets and symbol
    returns = {
        name: get_returns(name, symbols)
        for name, symbols in (
            get_etf_groups() | {target: [target]}
        ).items()
    }

    # recombine hists into index and all scenarios w/symbol
    if not returns[target].is_empty():
        return {
            'index_only': join(
                [returns[key] for key in [INDICES, target]]
            ),
            'all_etfs': join(
                [returns[key] for key in [INDICES, FACTORS, target]]
            ),
        }


@app.command()
def prompt():
    while True:
        symbol = typer.prompt('symbol')

        scenarios = get_scenarios(symbol)
        if scenarios:
            baskets = {
                name: run_opt(returns.drop('date'), symbol)
                for name, returns in scenarios.items()
            }
            display(baskets)
            for name, basket in baskets.items():
                calc_risk(symbol, basket, scenarios['all_etfs'])


# uv run -m folio SYMBOL
if __name__ == '__main__':
    app()
