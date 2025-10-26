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
