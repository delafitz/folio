from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

START_TIME = time(9, 30, 0)
END_TIME = time(16, 0, 0)
EST = ZoneInfo('EST')
UTC = ZoneInfo('UTC')
DT_FMT = '%Y-%m-%d'


def ts_to_date(ts):
    return datetime.fromtimestamp(ts * 1e-3).strftime(DT_FMT)


def get_dt_span(days):
    end = date.today()
    start = end - timedelta(days=days)

    return datetime.combine(
        start, START_TIME, tzinfo=EST
    ), datetime.combine(end, END_TIME, tzinfo=EST)
