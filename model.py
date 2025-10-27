import polars as pl
from skfolio.measures import RiskMeasure
from skfolio.optimization import MeanRisk, ObjectiveFunction
from skfolio.prior import EmpiricalPrior

from risk import calc_vols
from timing import timeit

MAX_BUDGET = 0.5
THRESHOLD_LONG = 0.10
CARDINALITY = 5
L1_COEF = 1e-5


@timeit
def run_opt(
    target,
    X,
    max_budget=MAX_BUDGET,
    threshold_long=THRESHOLD_LONG,
    cardinality=CARDINALITY,
    l1_coef=L1_COEF,
):
    model = MeanRisk(
        # need a MIP solver
        # SCIP is 3x slower than clarabel
        # set the target symbol weight to -1
        # get a sparse port of positive weights to min risk
        # thresholds min low weights
        # cardinality limits non-zero
        # non-zero L1 is very sensitive (~0.00005)
        solver='SCIP',
        prior_estimator=EmpiricalPrior(),
        objective_function=ObjectiveFunction.MINIMIZE_RISK,
        risk_measure=RiskMeasure.VARIANCE,
        max_weights={target: -1},
        min_weights=-1,
        max_short=1.0,
        threshold_short=-1.0,
        budget=None,
        max_budget=max_budget,
        threshold_long=threshold_long,
        l1_coef=l1_coef,
        cardinality=cardinality,
    )
    model.fit(X)
    weights = (
        pl.DataFrame(
            data=[model.weights_],
            schema=list(X.columns),
            orient='row',
        )
        .transpose(
            include_header=True,
            header_name=target,
            column_names=['weight'],
        )
        .remove(pl.col(target) == target)
        .filter(pl.col('weight') > 1e-5)
        .sort(by='weight', descending=True)
    )
    # ┌─────┬──────────┐
    # │ jpm ┆ weight   │
    # │ --- ┆ ---      │
    # │ str ┆ f64      │
    # ╞═════╪══════════╡
    # │ XLF ┆ 0.928508 │
    # │ KRE ┆ 0.1      │
    # │ XME ┆ 0.1      │
    # └─────┴──────────┘
    return weights


def run_opts(symbol, scenarios):
    baskets = {
        name: run_opt(symbol, returns.drop('date'))
        for name, returns in scenarios.items()
    }
    for _, basket in baskets.items():
        vols = (
            calc_vols(symbol, basket, scenarios['all_etfs'])
            if not basket.is_empty()
            else None
        )
        pl.Config.set_tbl_hide_dataframe_shape(True)
        pl.Config.set_float_precision(3)
        print(basket)
        print(vols)
