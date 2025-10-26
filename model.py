import polars as pl
from skfolio.measures import RiskMeasure
from skfolio.optimization import MeanRisk, ObjectiveFunction
from skfolio.prior import EmpiricalPrior

from timing import timeit

CARDINALITY = 5
L1_COEF = 1e-5


@timeit
def run_opt(X, target, l1_coef=L1_COEF, cardinality=CARDINALITY):
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
        budget=None,
        min_weights=-1,
        max_short=1.0,
        max_weights={target: -1},
        threshold_long=0.1,
        threshold_short=-1.0,
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
