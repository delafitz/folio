import polars as pl
from skfolio.optimization import MeanRisk, ObjectiveFunction
from skfolio.prior import EmpiricalPrior

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
