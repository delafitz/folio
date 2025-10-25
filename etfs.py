INDEX = 'SPY,QQQ,IWM'
SPDRS = 'XLF,XLK,XLV,XLC,XLE,XLI,XLY,XRT,XLU,XLP,KRE,XBI,XME,XOP,XHB'
ISHARES = 'MTUM,SOXX,IGV,IYR,ITB,IBB'
OTHER = 'GLD,USO,IBIT'

INDICES = 'indices'
FACTORS = 'factors'


def build_group(etfs):
    return [sym for syms in etfs for sym in syms.split(',')]


def get_etf_groups():
    return {
        INDICES: build_group([INDEX]),
        FACTORS: build_group([SPDRS, ISHARES, OTHER]),
    }
