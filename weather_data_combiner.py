import logging
import re
from ConfigParser import ConfigParser
from StringIO import StringIO
from datetime import datetime, timedelta
from glob import glob
from os.path import join, abspath, dirname, basename

import pandas as pd
from pandas.tseries.index import date_range
from pandas.tslib import NaTType

from utils import *

ANNUAL = 'annual'
MONTHLY = 'monthly'
FILE_NAME_PROTOTYPE = '%s_final.%s.filled'


def combine_data():
    logging.info('Start parsing weather data')
    config = ConfigParser()
    config.read(join(dirname(abspath(__file__)), 'paths.cfg'))

    streamflow_data_path = config.get('paths', 'streamflow_data')
    section_data_daily_path = config.get('paths', 'section_data_daily')
    section_data_monthly_path = config.get('paths', 'section_data_monthly')

    output_dir_path = config.get('paths', 'output_dir')

    log_file_name = join(dirname(abspath(__file__)), 'process.log')
    configure_logging(log_file_name)
    open(log_file_name, 'w').close()

    monthly_data_for_combining = get_data_for_combining(streamflow_data_path, section_data_monthly_path)
    daily_data_for_combining = get_data_for_combining(streamflow_data_path, section_data_daily_path)

    monthly_result_df = combine_data_to_monthly_result_df(monthly_data_for_combining)
    annual_result_df = combine_data_to_annual_result_df(daily_data_for_combining)

    dump_results(output_dir_path, MONTHLY, monthly_result_df)
    dump_results(output_dir_path, ANNUAL, annual_result_df)


def get_data_for_combining(streamflow_data_path, section_data_path):
    section_no = get_section_number(streamflow_data_path)
    prcp_df = get_section_file(section_no, section_data_path, 'prcp')
    tmax_df = get_section_file(section_no, section_data_path, 'tmax')
    tmin_df = get_section_file(section_no, section_data_path, 'tmin')

    sf_df_collection = []
    for streamflow_file_path in glob(join(streamflow_data_path, '*')):
        streamflow_buffer = StringIO()
        with open(streamflow_file_path) as streamflow_file:
            pass
            for line in streamflow_file.read().splitlines(True):
                if not line.startswith('#') and not line.startswith('"'):
                    streamflow_buffer.write(line)
        streamflow_buffer.seek(0)

        sf_df = pd.read_csv(streamflow_buffer, usecols=[0, 1, 2, 3], parse_dates=True, index_col=2,
                            comment='#', names=['agency_cd', 'site_no', 'datetime', 'cfs'])
        site_no = get_site_no(streamflow_file_path)
        sf_df_collection.append((site_no, sf_df))

    return prcp_df, tmax_df, tmin_df, sf_df_collection


def get_site_no(streamflow_file_path):
    site_no_pattern = re.compile('_(\d+)_')
    result = site_no_pattern.search(streamflow_file_path)
    if result:
        return result.group(1)
    else:
        raise Exception('Could not extract sete_no from %s file path' % streamflow_file_path)


def get_section_number(streamflow_data_path):
    site_no_pattern = re.compile('_(\d+)')
    result = site_no_pattern.search(basename(streamflow_data_path))
    if result:
        return int(result.group(1))
    else:
        raise Exception('Could not extract sete_no from %s file path' % streamflow_data_path)


def get_section_file(section_no, section_files_path, section_data_type):
    for section_file_path in glob(join(section_files_path, '*_%02d_%s_*' % (section_no, section_data_type))):
        return pd.read_csv(section_file_path, usecols=[0, 1], parse_dates=True, index_col=0,
                           na_values=["NAN"])


def combine_data_to_monthly_result_df(data_for_combining):
    result_df_collection = []
    prcp_df, tmax_df, tmin_df, sf_df_collection = data_for_combining
    freq = 'M'
    prcp_monthly_sum = prcp_df.groupby(pd.TimeGrouper(freq=freq)).sum()
    tmax_monthly_sum = tmax_df.groupby(pd.TimeGrouper(freq=freq)).sum()
    tmin_monthly_sum = tmin_df.groupby(pd.TimeGrouper(freq=freq)).sum()

    for site_no, sf_df in sf_df_collection:
        sf_df_monthly_mean = sf_df.groupby(pd.TimeGrouper(freq=freq)).mean()
        df = pd.DataFrame(index=sf_df_monthly_mean.index)
        df['MONTH'] = sf_df_monthly_mean.index.month
        df['YEAR'] = sf_df_monthly_mean.index.year
        df['MONTHLYFLOWVOL'] = sf_df_monthly_mean['cfs'] * (0.00000245 * 30)
        df['MEANMONTHLYQ'] = df['MONTHLYFLOWVOL'] / 30
        df['ACCMAXSTORAGE'] = None
        df['FLOWSTORAGERATIO'] = None
        df['PRECIP'] = prcp_monthly_sum['area-weighted'] / 10.0
        df['PRECIPMEAN'] = df['PRECIP'] / 30
        df['PRECIPVOL'] = None
        df['TMAX'] = tmax_monthly_sum['area-weighted'] / 10.0
        df['TMIN'] = tmin_monthly_sum['area-weighted'] / 10.0

        result_df_collection.append((site_no, df))

    return result_df_collection


def combine_data_to_annual_result_df(data_for_combining):
    result_df_collection = []
    prcp_df, _, _, sf_df_collection = data_for_combining
    prcp_daily_sum = prcp_df.groupby(pd.TimeGrouper(freq='D')).sum()
    prcp_monthly_sum = prcp_df.groupby(pd.TimeGrouper(freq='A')).sum()

    for site_no, sf_df in sf_df_collection:
        annual_max_idx = sf_df['cfs'].groupby(pd.TimeGrouper(freq='A')).idxmax()
        annual_max_values = sf_df['cfs'].groupby(pd.TimeGrouper(freq='A')).max()
        annual_min_values = sf_df['cfs'].groupby(pd.TimeGrouper(freq='A')).min()

        prcp_df.index = pd.to_datetime(prcp_df.index.map(lambda t: str(t.date())), format='%Y-%m-%d')
        datetime_range = pd.to_datetime(annual_max_idx.values)

        sf_df_monthly_mean = sf_df.groupby(pd.TimeGrouper(freq='A')).mean()
        df = pd.DataFrame(index=sf_df_monthly_mean.index)
        df['MONTH'] = sf_df_monthly_mean.index.month
        df['YEAR'] = sf_df_monthly_mean.index.year
        df['ANNUALFLOWVOL'] = sf_df_monthly_mean['cfs'] * (0.000893 * 30)
        df['MEANANNUALQ'] = df['ANNUALFLOWVOL'] / 365
        df['ACCMAXSTORAGE'] = None
        df['FLOWSTORAGERATIO'] = None
        df['PEAKDATE'] = annual_max_idx
        df['PEAKQCFS'] = annual_max_values
        df['PRECIPTOT'] = prcp_monthly_sum['area-weighted'] / 10.0
        df['PRECIPVOL'] = None

        df['PRECIP1'] = prcp_df.loc[datetime_range].get('area-weighted').values

        df['5TO1SUM'] = \
        pd.DataFrame(map(get_time_range(5, 1, prcp_df), datetime_range), index=sf_df_monthly_mean.index)[0] / 10
        df['30SUM'] = pd.DataFrame(map(get_time_range(30, 0, prcp_df), datetime_range), index=sf_df_monthly_mean.index)[
                          0] / 10
        df['60SUM'] = pd.DataFrame(map(get_time_range(60, 0, prcp_df), datetime_range), index=sf_df_monthly_mean.index)[
                          0] / 10
        df['90SUM'] = pd.DataFrame(map(get_time_range(90, 0, prcp_df), datetime_range), index=sf_df_monthly_mean.index)[
                          0] / 10
        df['180SUM'] = \
        pd.DataFrame(map(get_time_range(180, 0, prcp_df), datetime_range), index=sf_df_monthly_mean.index)[0] / 10
        df['365SUM'] = \
        pd.DataFrame(map(get_time_range(365, 0, prcp_df), datetime_range), index=sf_df_monthly_mean.index)[0] / 10
        df['5TO1RANGE'] = \
        pd.DataFrame(map(get_time_diff(5, 1, prcp_df), datetime_range), index=sf_df_monthly_mean.index)[0] / 10

        df['MINQCFS'] = annual_min_values
        result_df_collection.append((site_no, df))

    return result_df_collection


def dump_results(out_dir, data_type, data):
    mkpath(out_dir)
    for site_no, df in data:
        result_file_path = join(out_dir, FILE_NAME_PROTOTYPE % (site_no, data_type))

        df.to_csv(result_file_path)


def get_time_range(days_before, days_after, data_df):
    def get_date_value(date):
        if isinstance(date, NaTType):
            return 0
        start = datetime(date.year, date.month, date.day) - timedelta(days=days_before)
        end = datetime(date.year, date.month, date.day) + timedelta(days=days_after)

        dates = pd.to_datetime(map(lambda t: t.date(), date_range(start, end)), format='%Y-%m-%d')
        if dates[0] in data_df.index:
            return data_df.loc[dates].sum(skipna=True, numeric_only=True).values[0]
        else:
            return 0

    return get_date_value


def get_time_diff(days_before, days_after, data_df):
    def get_date_value(date):
        if isinstance(date, NaTType):
            return 0
        start = datetime(date.year, date.month, date.day) - timedelta(days=days_before)
        end = datetime(date.year, date.month, date.day) + timedelta(days=days_after)

        dates = pd.to_datetime(map(lambda t: t.date(), date_range(start, end)), format='%Y-%m-%d')
        if dates[0] in data_df.index:
            dates_values = data_df.loc[dates]
            diff = dates_values.max() - dates_values.min()
            return diff.values[0]
        else:
            return 0

    return get_date_value


if __name__ == '__main__':
    combine_data()
