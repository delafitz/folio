import os

import polars as pl
from polygon import RESTClient

from dates import get_dt_span, ts_to_date

API_KEY = 'T9J9ukxYu5PlFfbbphx4KLkdoSVJmnF7'
HIST_STORE = './cache/hist_data'

INDICES = 'SPY,QQQ,IWM'
SPDRS = 'XLF,XLK,XLV,XLC,XLE,XLI,XLY,XRT,XLU,XLP,KRE,XBI,XME,XOP,XHB'
ISHARES = 'MTUM,SOXX,IGV,IYR,ITB,IBB'
OTHER = 'GLD,USO,IBIT'

HIST_WINDOW = 365

BASKET_SCHEMA = {
    'date': pl.String,
    'close': pl.Float32,
}


def get_etf_baskets(index_only=False, factor_only=False):
    if index_only:
        etfs = [INDICES]
    elif factor_only:
        etfs = [SPDRS, ISHARES, OTHER]
    else:
        etfs = [INDICES, SPDRS, ISHARES, OTHER]

    return [sym for syms in etfs for sym in syms.split(',')]


def fetch_hist(symbols):
    client = RESTClient(API_KEY)
    from_dt, to_dt = get_dt_span(HIST_WINDOW)
    hists = []
    for symbol in symbols:
        hist = [
            [ts_to_date(agg.timestamp), agg.close]
            for agg in client.list_aggs(
                symbol.upper(), 1, 'day', from_dt, to_dt
            )
        ]
        df = pl.DataFrame(hist, schema=BASKET_SCHEMA, orient='row')
        df = df.rename({'close': symbol})
        hists.append(df)
    df_all = pl.concat(hists, how='align_left')
    return df_all


def get_cache(name):
    file = f'{HIST_STORE}-{name}.parquet'
    return pl.read_parquet(file) if os.path.exists(file) else None


def write_cache(hist, name):
    hist.write_parquet(
        f'{HIST_STORE}-{name}.parquet', compression='zstd'
    )


def get_returns(symbols, name, force_fetch=False):
    hist = get_cache(name)
    if hist is None or force_fetch:
        hist = fetch_hist(symbols)
    if not hist.is_empty():
        write_cache(hist, name)
        return hist.with_columns(
            pl.all().exclude('date').pct_change()
        ).drop_nulls()  # [-HIST_WINDOW:]
