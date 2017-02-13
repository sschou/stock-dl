# -*- coding: utf-8 -*-

# Python Build-in
import os.path
import datetime as dt

# 3rd Party Libs
import pandas as pd
import pandas_datareader.data as web

# Local
import constant as const


class Stock(object):
    """ Stock Infomation
        By default, all downloaded data is stored in a csv file
        at STOCK_FOLDER
    """
    def __init__(self, stock_id, file_ext='.csv',
                 working_path=os.getcwd(),
                 source='yahoo'):
        assert isinstance(stock_id, str)
        assert isinstance(file_ext, str)
        folder_path = os.path.join(working_path, const.STOCK_FOLDER)
        if not os.path.isdir(folder_path):
            os.mkdir(folder_path)
        self.stock_id = stock_id
        self.file_ext = file_ext
        self.file_name = ''.join([stock_id, file_ext])
        self.data_file_path = os.path.join(folder_path, self.file_name)

        if source == 'yahoo':
            self.data_src = source
        else:
            raise NotImplementedError(
                'data source=%r is not implemented' % source)

    def __local_data_reader(self):
        if not os.path.isfile(self.data_file_path):
            return pd.DataFrame()

        if self.file_ext == '.csv':
            df = pd.read_csv(self.data_file_path,
                             parse_dates=['Date'], index_col=['Date'])
        elif self.file_ext == '.xls':
            df = pd.read_excel(self.data_file_path,
                               parse_dates=['Date'], index_col=['Date'])
        else:
            raise NotImplementedError(
                'local data file type %r is not implemented' % self.file_ext)
        return df

    def __local_data_writer(self, df):

        if self.file_ext == '.csv':
            df.to_csv(self.data_file_path, encoding='utf-8')
        elif self.file_ext == '.xls':
            df.to_excel(self.data_file_path, encoding='utf-8')
        else:
            raise NotImplementedError(
                'local data file type %r is not implemented' % self.file_ext)

    def __cleanup_date(self, start, end, df):
        """
            Parameters
            ----------
            start : {datetime, None}
                left boundary for date range (defaults to 1/1/2016)
            end : {datetime, None}
                right boundary for date range (defaults to today)
            df : {pandas dataframe}
                use to get last valid data date
        """
        if start is None and not df.empty:
            start = df.index[-1].to_pydatetime()
        elif start is None and df.empty:
            start = dt.datetime(const.START_YEAR,
                                const.START_MONTH,
                                const.START_DAY)

        if end is None:
            end = dt.datetime.today()

        return start, end

    def fetch_data(self, start=None, end=None):
        """ use Pandas datareader to fetch data from Yahoo Finance

        Parameters
        ----------
        start : {datetime, None}
            left boundary for date range (defaults to 1/1/2016)
        end : {datetime, None}
            right boundary for date range (defaults to today)

        """

        local_df = self.__local_data_reader()
        start, end = self.__cleanup_date(start, end, local_df)

        web_df = web.DataReader(self.stock_id, self.data_src, start, end)
        # do clean up
        if self.data_src == 'yahoo':  # yahoo includes market closed day
            web_df = web_df[web_df['Volume'] != 0]

        if local_df.empty:
            self.__local_data_writer(web_df)
        else:
            local_df = local_df.combine_first(web_df)
            self.__local_data_writer(local_df)


if __name__ == '__main__':
    pass
