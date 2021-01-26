import pandas as pd
import numpy as np
import re
from random import randint


class CovidDF():
    def __init__(self, df, title, gb, val_col):
        self.df = df
        self.title = title
        self.gb = gb
        self.val_col = val_col


class CovidData:
    CONFIRMED = 'Confirmed'
    DEATHS = 'Deaths'
    RECOVERED = 'Recovered'
    GB_US = ['country', 'state', 'county']
    GB_GLOBAL = ['country', 'state']

    cols_to_rename = {'Country_Region': 'country', 'Country/Region': 'country', 'Province_State': 'state', 'Province/State': 'state', 'Admin2': 'county', 'UID': 'location_id', 'Long_': 'lon', 'Long': 'lon'}

    def __init__(self):
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

    def add_levels(self):
        '''
        Update inconsistent territory levels. For example, China is only by country/state, while France is by both country/state and country only. This means that summing up all the states to get each country's total, i.e. China would also mean inadvertently double counting France.

        To fix, this creates a seperate line for each missing level, i.e. create a country line for China
        '''
        for DF in self.DFs:
            df = DF.df
            if 'global' in DF.title:
                null_countries = df.loc[df['state'].isnull()]['country'].unique()
                #China, Aus, France
                roll_countries = df.loc[~df['country'].isin(null_countries)]
                rolled = roll_countries.groupby(['country', 'Dt'])[roll_countries.columns[-1]].sum().reset_index()
                rolled.insert(1, 'state', np.nan)
                if not rolled.empty:
                    DF.df = pd.concat([df, rolled], axis=0)
            else:
                rolled = df.loc[df['county'].notnull()].groupby(['country', 'state', 'Dt'])[df.columns[-1]].sum().reset_index()
                rolled.insert(3, 'county', np.nan)
                if not rolled.empty:
                    DF.df = pd.concat([df, rolled], axis=0)

    def clean_time_series(self):
        for DF in self.DFs:
            df = DF.df
            df.rename(columns=self.cols_to_rename, inplace=True)
            df.loc[df['country'] == 'US', 'country'] = 'United States'

    def merge_missing_locations(self):
        '''
        The location file provided has a few missing locations (Yakutat, Alaska and Repatriated Travellers, Canada) as of 1/18/2021
        '''
        #filter for the columns needed for combined key
        df_gl = self.global_confirmed.df[['state', 'country', 'Lat', 'lon']]
        df_us = self.us_confirmed.df[['state', 'country', 'Lat', 'lon', 'iso2', 'county']]

        #create a combined key field in both us and global confirmed
        df_gl['combined_key'] = (df_gl['state'] + ', ').fillna('') + df_gl['country']
        df_us['combined_key'] = (df_us['county'] + ', ').fillna('') + (df_us['state'] + ', ').fillna('') + df_us['country']

        #concat row-wise us and global confirmed
        df_concat = pd.concat([df_gl, df_us], axis=0)
        df_missing = df_concat.loc[~df_concat['combined_key'].isin(self.location.df['combined_key'])]
        #create a 6 digit unique id, that's the smallest UID not used in the original location table
        df_missing['location_id'] = df_missing.groupby('combined_key')['combined_key'].transform(lambda x: randint(100000, 999999))
        #default 100k
        df_missing['Population'] = np.nan
        df_missing = df_missing[['location_id', 'country', 'state', 'iso2', 'county', 'Population', 'Lat', 'lon', 'combined_key']]
        self.location.df = pd.concat([self.location.df, df_missing], axis=0)

    def clean_location(self):
        df = self.location.df
        df = df.rename(columns=self.cols_to_rename)
        df = df[['location_id', 'country', 'state', 'iso2', 'county', 'Population', 'Lat', 'lon']]
        df.loc[df['country'] == 'US', 'country'] = 'United States'
        #have to manually recreate combined_key field since original field isnt consistently formatted
        df['combined_key'] = (df['county'] + ', ').fillna('') + (df['state'] + ', ').fillna('') + df['country']
        self.location.df = df
        self.merge_missing_locations()

    def save_csv(self, df, path, title, aws_key=None, aws_secret=None):
        f = f'{path}{title}_diff.csv'
        if aws_key:
            import s3fs
            s3 = s3fs.S3FileSystem(key=aws_key, secret=aws_secret)
            print(f'saving to s3: {f}')
            f = s3.open(f, 'w')
        print('finished s3 open')
        df.to_csv(f, sep='\t', index=False)

    def save_csv_local(self):
        for DF in self.DFs:
            self.save_csv(DF.df, '/Users/jwong/Documents/', DF.title)

    def save_csv_s3(self, aws_key, aws_secret, bucket):
        #bucket_title = 's3://covid-data-jh-normalized/'
        for DF in self.DFs:
            self.save_csv(DF.df, bucket, DF.title, aws_key, aws_secret)
        #save location
        self.save_csv(self.location.df, bucket, self.location.title, aws_key, aws_secret)

    def get_date_cols(self, df):
        pattern = re.compile(r'\d{1,2}/\d{1,2}/\d{2}')
        date_cols = list(filter(pattern.match, df.columns))
        return date_cols

    def convert_headers_to_datetime(self, df, date_cols):
        date_converted_cols = pd.to_datetime(date_cols, format='%m/%d/%y')
        d = dict(zip(date_cols, date_converted_cols))
        df = df.rename(columns=d)
        return df

    def melt_cols(self, df, id_vars, value_name):
        date_cols = self.get_date_cols(df)
        #print(f'date_cols: {date_cols}')
        cols_to_keep = id_vars + date_cols
        df = df[cols_to_keep]
        df = self.convert_headers_to_datetime(df, date_cols)
        df = df.melt(id_vars=id_vars, var_name='Dt', value_name=value_name)
        return df

    def melt_dfs(self):
        for DF in self.DFs:
            DF.df = self.melt_cols(DF.df, id_vars=DF.gb, value_name=DF.val_col).sort_values(DF.gb)

    def get_daily_totals(self, df, gb, value_name):
        df[value_name] = df.groupby(gb, dropna=False)[value_name].diff().fillna(0)
        return df

    def get_daily_totals_dfs(self):
        for DF in self.DFs:
            DF.df = self.get_daily_totals(DF.df, DF.gb, DF.val_col)


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
