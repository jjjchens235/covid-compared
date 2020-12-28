import pandas as pd
import re


class CovidDF():
    def __init__(self, df, title, gb, val_col):
        self.df = df
        self.title = title
        self.gb = gb
        if 'confirmed' in title:
            print(title)
            self.gb = self.gb + ['Population']
        self.val_col = val_col


class CovidData:
    CONFIRMED = 'Confirmed'
    DEATHS = 'Deaths'
    RECOVERED = 'Recovered'
    GB_US = ['Country', 'iso2', 'State', 'County']
    GB_GLOBAL = ['Country', 'State']

    cols_to_rename = {'Country_Region': 'Country', 'Country/Region': 'Country', 'Province_State': 'State', 'Province/State': 'State', 'Admin2': 'County'}

    def __init__(self):
        BASE_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series'
        us_confirmed_url = f'{BASE_URL}/time_series_covid19_confirmed_US.csv'
        global_confirmed_url = f'{BASE_URL}/time_series_covid19_confirmed_global.csv'
        us_deaths_url = f'{BASE_URL}/time_series_covid19_deaths_US.csv'
        global_deaths_url = f'{BASE_URL}/time_series_covid19_deaths_global.csv'
        global_recovered_url = f'{BASE_URL}/time_series_covid19_recovered_global.csv'
        population_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv'
        self.population = CovidDF(pd.read_csv(population_url, error_bad_lines=False)[['Combined_Key', 'Population']], 'population', 'None', 'None')

        self.us_confirmed = CovidDF(pd.read_csv(us_confirmed_url, error_bad_lines=False), 'us_confirmed', self.GB_US, self.CONFIRMED)
        self.global_confirmed = CovidDF(pd.read_csv(global_confirmed_url, error_bad_lines=False), 'global_confirmed', self.GB_GLOBAL, self.CONFIRMED)

        self.us_deaths = CovidDF(pd.read_csv(us_deaths_url, error_bad_lines=False), 'us_deaths', self.GB_US, self.DEATHS)
        self.global_deaths = CovidDF(pd.read_csv(global_deaths_url, error_bad_lines=False), 'global_deaths', self.GB_GLOBAL, self.DEATHS)

        self.global_recovered = CovidDF(pd.read_csv(global_recovered_url, error_bad_lines=False), 'global_recovered', self.GB_GLOBAL, self.RECOVERED)

        self.DFs = [self.us_confirmed, self.global_confirmed, self.us_deaths, self.global_deaths, self.global_recovered]

    def join_population(self):
        #self.global_confirmed['Combined_Key'] = self.global_confirmed['State'].fillna('') + self.global_confirmed['Country']
        self.global_confirmed.df['Combined_Key'] = (self.global_confirmed.df['State'] + ', ').fillna('') + self.global_confirmed.df['Country']
        self.global_confirmed.df = self.global_confirmed.df.merge(self.population.df, on='Combined_Key')

        self.us_confirmed.df = self.us_confirmed.df.merge(self.population.df, on='Combined_Key')

    def clean(self, df):
        # rename
        df.rename(columns=self.cols_to_rename, inplace=True)

    def clean_all(self):
        for DF in self.DFs:
            self.clean(DF.df)

    def save_csv(self, df, name):
        df.to_csv(f'/Users/jwong/Documents/{name}_diff.csv', sep='\t', index=False)

    def save_all_csv(self):
        for DF in self.DFs:
            self.save_csv(DF.df, DF.title)

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
        print(f'id_vars: {id_vars}')
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


if __name__ == '__main__':
    covid = CovidData()
    covid.clean_all()
    covid.join_population()
    print(covid.us_confirmed.df.head())
    covid.melt_dfs()
    covid.get_daily_totals_dfs()

    covid.save_all_csv()
