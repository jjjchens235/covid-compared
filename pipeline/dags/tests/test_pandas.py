from datetime import datetime
import unittest
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sister_dir = os.path.join(parent_dir, 'scripts/')
os.sys.path.insert(0, sister_dir)
from covid_pandas import CovidData


class TestPandas(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.data = CovidData()
        self.data.clean_time_series()
        self.data.raw_totals = {}

    def get_raw_totals(self):
        for DF in self.data.DFs:
            title = DF.title
            df = DF.df
            date_cols = self.data.get_date_cols(df)[-1]
            raw_total = df[date_cols].sum().sum()
            self.data.raw_totals[title] = raw_total

    def test_totals(self):
        """
        Check that the raw github data totals for each metric match the daily totals that are derived using melt() and diff()
        """
        self.get_raw_totals()
        self.data.melt_dfs()
        self.data.get_daily_totals_dfs()

        for DF in self.data.DFs:
            title = DF.title
            metric = DF.metric
            print(f'\ndataframe: {title}')
            df = DF.df
            daily_total = df[metric].sum()
            raw_total = self.data.raw_totals[title]
            self.assertEqual(raw_total, daily_total)
