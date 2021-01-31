"""
Using pandas library, move data from John Hopkin's github repo to AWS S3

Data source: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series
"""
import pandas as pd
import numpy as np
import re
from random import randint
import s3fs


class CovidDF():
    def __init__(self, df, title, gb, metric):
        self.df = df
        self.title = title
        self.gb = gb
        self.metric = metric


class CovidData:
    CONFIRMED = 'Confirmed'
    DEATHS = 'Deaths'
    RECOVERED = 'Recovered'
    GB_US = ['country', 'state', 'county']
    GB_GLOBAL = ['country', 'state']

    cols_to_rename = {'Country_Region': 'country', 'Country/Region': 'country', 'Province_State': 'state', 'Province/State': 'state', 'Admin2': 'county', 'UID': 'location_id', 'Long_': 'lon', 'Long': 'lon'}

    def __init__(self):
        """
        Initalize each of the dataframes from the 5 John Hopkins time series files
        """
        BASE_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series'
        us_confirmed_url = f'{BASE_URL}/time_series_covid19_confirmed_US.csv'
        global_confirmed_url = f'{BASE_URL}/time_series_covid19_confirmed_global.csv'
        us_deaths_url = f'{BASE_URL}/time_series_covid19_deaths_US.csv'
        global_deaths_url = f'{BASE_URL}/time_series_covid19_deaths_global.csv'
        global_recovered_url = f'{BASE_URL}/time_series_covid19_recovered_global.csv'
        location_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv'

        self.location = CovidDF(pd.read_csv(location_url, error_bad_lines=False), 'location', 'None', 'None')
        self.us_confirmed = CovidDF(pd.read_csv(us_confirmed_url, error_bad_lines=False), 'us_confirmed', self.GB_US, self.CONFIRMED)
        self.global_confirmed = CovidDF(pd.read_csv(global_confirmed_url, error_bad_lines=False), 'global_confirmed', self.GB_GLOBAL, self.CONFIRMED)

        self.us_deaths = CovidDF(pd.read_csv(us_deaths_url, error_bad_lines=False), 'us_deaths', self.GB_US, self.DEATHS)
        self.global_deaths = CovidDF(pd.read_csv(global_deaths_url, error_bad_lines=False), 'global_deaths', self.GB_GLOBAL, self.DEATHS)

        self.global_recovered = CovidDF(pd.read_csv(global_recovered_url, error_bad_lines=False), 'global_recovered', self.GB_GLOBAL, self.RECOVERED)

        self.DFs = [self.us_confirmed, self.global_confirmed, self.us_deaths, self.global_deaths, self.global_recovered]

    def clean_time_series(self):
        """
        Rename the cols of each dataframe, and rename all values of US -> United States
        """
        for DF in self.DFs:
            df = DF.df
            df.rename(columns=self.cols_to_rename, inplace=True)
            df.loc[df['country'] == 'US', 'country'] = 'United States'

    def merge_missing_locations(self):
        """
        The location file provided has a few missing locations (Yakutat, Alaska and Repatriated Travellers, Canada) as of 1/18/2021
        Compare the US and Global confirmed files against the location file and add any missing locations to the location file
        """
        #filter for the columns needed for combined key
        df_gl = self.global_confirmed.df[['state', 'country', 'Lat', 'lon']]
        df_us = self.us_confirmed.df[['state', 'country', 'Lat', 'lon', 'iso2', 'county']]

        #create a combined key field in both us and global confirmed
        df_gl['combined_key'] = (df_gl['state'] + ', ').fillna('') + df_gl['country']
        df_us['combined_key'] = (df_us['county'] + ', ').fillna('') + (df_us['state'] + ', ').fillna('') + df_us['country']

        #concat row-wise us and global confirmed
        df_concat = pd.concat([df_gl, df_us], axis=0)
        #get the missing locations
        df_missing = df_concat.loc[~df_concat['combined_key'].isin(self.location.df['combined_key'])]
        #create a 6 digit unique id, that's the smallest UID not used in the original location table
        df_missing['location_id'] = df_missing.groupby('combined_key')['combined_key'].transform(lambda x: randint(100000, 999999))
        df_missing['Population'] = np.nan
        df_missing = df_missing[['location_id', 'country', 'state', 'iso2', 'county', 'Population', 'Lat', 'lon', 'combined_key']]
        self.location.df = pd.concat([self.location.df, df_missing], axis=0)

    def clean_location(self):
        """ Clean/update the location dataframe """
        df = self.location.df
        df = df.rename(columns=self.cols_to_rename)
        df = df[['location_id', 'country', 'state', 'iso2', 'county', 'Population', 'Lat', 'lon']]
        df.loc[df['country'] == 'US', 'country'] = 'United States'
        #have to manually recreate combined_key field since original field isnt consistently formatted
        df['combined_key'] = (df['county'] + ', ').fillna('') + (df['state'] + ', ').fillna('') + df['country']
        self.location.df = df
        self.merge_missing_locations()

    def __get_date_cols(self, df):
        """ Find the columns that match date regex """
        pattern = re.compile(r'\d{1,2}/\d{1,2}/\d{2}')
        date_cols = list(filter(pattern.match, df.columns))
        return date_cols

    def __convert_headers_to_datetime(self, df, date_cols):
        """
        Convert the date columns from string -> datetime
        """
        date_converted_cols = pd.to_datetime(date_cols, format='%m/%d/%y')
        d = dict(zip(date_cols, date_converted_cols))
        df = df.rename(columns=d)
        return df

    def __melt_cols(self, df, id_vars, metric):
        """ Melt date columns to rows  """
        date_cols = self.__get_date_cols(df)
        cols_to_keep = id_vars + date_cols
        df = df[cols_to_keep]
        df = self.__convert_headers_to_datetime(df, date_cols)
        df = df.melt(id_vars=id_vars, var_name='dt', value_name=metric)
        return df

    def melt_dfs(self):
        """ For each df, melt datetime columns """
        for DF in self.DFs:
            DF.df = self.__melt_cols(DF.df, id_vars=DF.gb, metric=DF.metric).sort_values(DF.gb)

    def __get_daily_totals(self, df, gb, metric):
        """
        Converts metric cumsum value to daily values
        """
        df[metric] = df.groupby(gb, dropna=False)[metric].diff().fillna(0)
        return df

    def get_daily_totals_dfs(self):
        """ Converts cumsum totals to daily for all df's """
        for DF in self.DFs:
            DF.df = self.__get_daily_totals(DF.df, DF.gb, DF.metric)

    def add_levels(self):
        """
        Update inconsistent territory levels.

        For example, China is only by country/state,
        while France is by both country/state and country only.
        This means that if summing up all the states to get each country's total,
        would work for some countries (China),
        but would double count for other countries (France)

        To fix, this creates a seperate line for each missing level, i.e. create a country line for China
        """
        for DF in self.DFs:
            df = DF.df
            if 'global' in DF.title:
                null_countries = df.loc[df['state'].isnull()]['country'].unique()
                # roll_countries don't have distinct country line, all their rows include states, i.e China, Aus
                roll_countries = df.loc[~df['country'].isin(null_countries)]
                #create distinct country line
                rolled = roll_countries.groupby(['country', 'dt'])[roll_countries.columns[-1]].sum().reset_index()
                rolled.insert(1, 'state', np.nan)
                if not rolled.empty:
                    DF.df = pd.concat([df, rolled], axis=0)
            else:
                rolled = df.loc[df['county'].notnull()].groupby(['country', 'state', 'dt'])[df.columns[-1]].sum().reset_index()
                rolled.insert(3, 'county', np.nan)
                if not rolled.empty:
                    DF.df = pd.concat([df, rolled], axis=0)

    def __save_csv(self, df, path, title, aws_key=None, aws_secret=None):
        """ Save csv to either local or S3 """
        f = f'{path}{title}_diff.csv'
        if aws_key:
            s3 = s3fs.S3FileSystem(key=aws_key, secret=aws_secret)
            print(f'saving to s3: {f}')
            f = s3.open(f, 'w')
        print('finished s3 open')
        df.to_csv(f, sep='\t', index=False)

    def save_csv_local(self):
        """ save as local file """
        for DF in self.DFs:
            self.__save_csv(DF.df, '/Users/jwong/Documents/', DF.title)

    def save_csv_s3(self, aws_key, aws_secret, bucket):
        """ save to s3 """
        for DF in self.DFs:
            self.__save_csv(DF.df, bucket, DF.title, aws_key, aws_secret)
        #save location
        self.__save_csv(self.location.df, bucket, self.location.title, aws_key, aws_secret)


def main(aws_key, aws_secret, bucket):
    covid = CovidData()
    covid.clean_time_series()
    print(covid.us_confirmed.df.head())
    covid.clean_location()
    covid.melt_dfs()
    print('made it to daily total')
    covid.get_daily_totals_dfs()
    print('made it add_levels')
    covid.add_levels()

    covid.save_csv_s3(aws_key, aws_secret, bucket)


if __name__ == '__main__':
    main()
