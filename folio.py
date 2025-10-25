from pprint import pprint as pp

import polars as pl
from skfolio.measures import RiskMeasure
from skfolio.optimization import MeanRisk, ObjectiveFunction
from skfolio.prior import EmpiricalPrior

from hist import get_etf_baskets, get_returns
from timing import timeit


@timeit
def run_opt(X, cols, target):
    model = MeanRisk(
        prior_estimator=EmpiricalPrior(),
        objective_function=ObjectiveFunction.MINIMIZE_RISK,
        risk_measure=RiskMeasure.VARIANCE,
        budget=None,
        min_weights=-1,
        max_short=1.0,
        max_weights={target: -1},
    )
    model.fit(X)
    weights = (
        pl.DataFrame(data=[model.weights_], schema=cols, orient='row')
        .transpose(
            include_header=True,
            header_name=target,
            column_names=['weight'],
        )
        .remove(pl.col(target) == target)
        .sort(by='weight', descending=True)
    )
    return weights


def get_baskets(symbol):
    indices = get_returns(get_etf_baskets(indices=True), 'indices')
    factors = get_returns(get_etf_baskets(sectors=True), 'factors')
    target = get_returns([symbol], symbol)

    baskets = {}
    for name, basket in {
        'indices': [indices, target],
        'factors': [indices, factors, target],
    }.items():
        joint = pl.concat(basket, how='align_left').select(
            pl.all().exclude('date')
        )
        weights = run_opt(joint.to_pandas(), joint.columns, symbol)
        baskets[name] = weights

    data = [dict(basket.rows()) for _, basket in baskets.items()]
    print(data)
    hedges = pl.DataFrame(
        [data],
        orient='row',
        schema=baskets.keys(),
    )
    pp(hedges.to_dicts())


if __name__ == '__main__':
    get_baskets('jpm')
