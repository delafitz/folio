from pprint import pprint as pp

import polars as pl

from etfs import FACTORS, INDICES, get_etf_groups
from hist import get_returns
from model import run_opt


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
    # │ 2024-10-31 ┆ -0.019603 ┆ -0.025243 ┆ -0.016619 │
    # │ 2024-11-01 ┆ 0.004221  ┆ 0.007399  ┆ 0.005603  │
    # └────────────┴───────────┴───────────┴───────────┘
    return {
        'index_only': [returns[key] for key in [INDICES, target]],
        'all': [returns[key] for key in [INDICES, FACTORS, target]],
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
    baskets = {
        name: run_opt(join(returns), target)
        for name, returns in scenarios.items()
    }
    print(baskets)
    hedges = pl.DataFrame(
        [dict(basket.rows()) for basket in baskets.values()],
        orient='row',
        schema=baskets.keys(),
    )
    pp(hedges.to_dicts())


if __name__ == '__main__':
    target = 'jpm'
    scenarios = get_scenarios(target)
    get_opts(scenarios, target)
