import logging
import re
from ConfigParser import ConfigParser
from glob import glob
from os.path import join, abspath, dirname, basename
from StringIO import StringIO

import pandas as pd

from utils import *

ANNUAL = 'annual'
MONTHLY = 'monthly'
FILE_NAME_PROTOTYPE = '%s_final.%s.filled'


def combine_data():
    logging.info('Start parsing weather data')
    config = ConfigParser()
    config.read(join(dirname(abspath(__file__)), 'paths.cfg'))

    streamflow_data_path = config.get('paths', 'streamflow_data')
    section_data_path = config.get('paths', 'section_data')

    output_dir_path = config.get('paths', 'output_dir')

    log_file_name = join(dirname(abspath(__file__)), 'process.log')
    configure_logging(log_file_name)
    open(log_file_name, 'w').close()

    data_for_combining = get_data_for_combining(streamflow_data_path, section_data_path)

    # monthly_result_df = combine_data_to_monthly_result_df(data_for_combining)
    annual_result_df = combine_data_to_annual_result_df(data_for_combining)

    # dump_results(output_dir_path, MONTHLY, monthly_result_df)
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
        return pd.read_csv(section_file_path, parse_dates=True, index_col=0, na_values=["NAN"])


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
    freq_d = 'D'
    freq_y = 'A'
    prcp_daily_sum = prcp_df.groupby(pd.TimeGrouper(freq=freq_d)).sum()
    prcp_monthly_sum = prcp_df.groupby(pd.TimeGrouper(freq=freq_y)).sum()


    for site_no, sf_df in sf_df_collection:
        annual_max_idx = sf_df['cfs'].groupby(pd.TimeGrouper(freq=freq_y)).idxmax()
        annual_max_values = sf_df['cfs'].groupby(pd.TimeGrouper(freq=freq_y)).max()
        annual_min_values = sf_df['cfs'].groupby(pd.TimeGrouper(freq=freq_y)).min()

        prcp_df.index = pd.to_datetime(prcp_df.index.map(lambda t: str(t.date())), format='%Y-%m-%d')

        print(pd.to_datetime(annual_max_idx.values))
        print(prcp_df.index)
        print(prcp_df.loc[pd.to_datetime(annual_max_idx.values)])
        break
        sf_df_monthly_mean = sf_df.groupby(pd.TimeGrouper(freq=freq_y)).mean()
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

        df['PRECIP1'] = None

        df['MINQCFS'] = annual_min_values
        print(df)
        result_df_collection.append((site_no, df))

    return result_df_collection


def dump_results(out_dir, data_type, data):
    mkpath(out_dir)
    for site_no, df in data:
        result_file_path = join(out_dir, FILE_NAME_PROTOTYPE % (site_no, data_type))

        df.to_csv(result_file_path)


if __name__ == '__main__':
    combine_data()
