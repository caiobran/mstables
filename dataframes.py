import pandas as pd
import numpy as np
import sqlite3
import json
import sys
import re
import os


class DataFrames():

    dbfile = 'db/mstables.sqlite' # Standard db file name

    def __init__(self, file = dbfile):

        print('Creating intial DataFrames ...')

        # SQLite connection
        self.conn = sqlite3.connect(file)
        self.cur = self.conn.cursor()

        # Row Headers
        ColHeaders = table(self.cur, 'ColHeaders', True)
        self.ColHeaders = ColHeaders.set_index('id')

        # Dates and time references
        timerefs = table(self.cur, 'TimeRefs', True)
        self.timerefs = timerefs.set_index('id').replace(['', '—'], None)

        # Reference tables
        self.urls = table(self.cur, 'URLs', True)
        self.types = table(self.cur, 'Types', True)
        self.tickers = table(self.cur, 'Tickers', True)
        self.sectors = table(self.cur, 'Sectors', True)
        self.industries = table(self.cur, 'Industries', True)
        self.styles = table(self.cur, 'StockStyles', True)
        self.exchanges = table(self.cur, 'Exchanges', True)
        self.countries = table(self.cur, 'Countries', True)
        self.companies = table(self.cur, 'Companies', True)
        self.currencies = table(self.cur, 'Currencies', True)
        self.companytypes = table(self.cur, 'CompanyTypes', True)
        #self.fetchedurls = table(self.cur, 'Fetched_urls', True)

        # Master table
        master = (table(self.cur, 'Master', True)
            .drop(['companytype_id', 'style_id'] , axis=1)
            .rename(columns={'fyend_id':'fy_end', 'update_date_id':'updated'})
            .merge(self.tickers, left_on='ticker_id', right_on='id')
            .drop(['id'] , axis=1)
            .merge(self.exchanges, left_on='exchange_id', right_on='id')
            .drop(['id'] , axis=1)
            .rename(columns={'exchange_sym':'exchange_symbol'})
            .merge(self.countries, left_on='country_id', right_on='id')
            .drop(['id', 'country_id', 'a2_iso'] , axis=1)
            .merge(self.companies, left_on='company_id', right_on='id')
            .drop(['id', 'company_id'] , axis=1)
            .merge(self.industries, left_on='industry_id', right_on='id')
            .drop(['id', 'industry_id'] , axis=1)
            .merge(self.sectors, left_on='sector_id', right_on='id')
            .drop(['id', 'sector_id'] , axis=1)
            .merge(self.types, left_on='type_id', right_on='id')
            .drop(['id', 'type_id'] , axis=1)
            .rename(columns={'country':'country_name', 'a3_un':'country'})
            .replace(['', '—'], None)
            )
        master = master[[
            'ticker_id', 'exchange_id', 'country', 'country_name',
            'exchange_symbol', 'exchange', 'ticker', 'company', 'type_code',
            'type', 'sector', 'industry', 'fy_end', 'updated']]
        master['updated'] = master['updated'].astype('int')
        master['fy_end'] = master['fy_end'].astype('int')
        master[['fy_end', 'updated']] = (
            master[['fy_end', 'updated']].replace(self.timerefs['dates' ]))
        master['fy_end'] = pd.to_datetime(master['fy_end'])
        master['updated'] = pd.to_datetime(master['updated'])

        self.master = master.sort_values(by='fy_end', ascending=False)
        print('Initial DataFrames created.')


    def QuoteHeader(self):
        return table(self.cur, 'MSheader')


    def Valuation(self):
        valuation = table(self.cur, 'MSvaluation')
        valuation.iloc[:,2:13] = (
            valuation.iloc[:,2:13].replace(self.timerefs['dates']))
        return valuation


    def KeyRatios(self):
        keyratios = table(self.cur, 'MSfinancials')
        keyratios.iloc[:,2:13] = (
            keyratios.iloc[:,2:13].replace(self.timerefs['dates']))
        return keyratios


    def FinancialHealth(self):
        finanhealth = table(self.cur, 'MSratio_financial')
        finanhealth.iloc[:, 2:13] = (finanhealth
            .iloc[:, 2:13].replace(self.timerefs['dates']))
        return finanhealth


    def Profitability(self):
        profitab = table(self.cur, 'MSratio_profitability')
        profitab.iloc[:, 2:13] = (profitab
            .iloc[:, 2:13].replace(self.timerefs['dates']))
        return profitab


    def Growth(self):
        growth = table(self.cur, 'MSratio_growth')
        growth.iloc[:, 2:13] = (growth
            .iloc[:, 2:13].replace(self.timerefs['dates']))
        return growth


    def CashflowHealth(self):
        cfhealth = table(self.cur, 'MSratio_cashflow')
        cfhealth.iloc[:, 2:13] = (cfhealth
            .iloc[:, 2:13].replace(self.timerefs['dates']))
        return cfhealth


    def Efficiency(self):
        efficiency = table(self.cur, 'MSratio_efficiency')
        efficiency.iloc[:, 2:13] = (efficiency
            .iloc[:, 2:13].replace(self.timerefs['dates']))
        return efficiency

    # Income Statement - Annual
    def AnnualIS(self):
        rep_is_yr = table(self.cur, 'MSreport_is_yr')
        rep_is_yr.iloc[:,2:8] = (
            rep_is_yr.iloc[:,2:8].replace(self.timerefs['dates']))
        cols = [col for col in rep_is_yr.columns if 'label' in col]
        rep_is_yr[cols] = (
            rep_is_yr[cols].replace(self.ColHeaders['header']))
        return rep_is_yr

    # Income Statement - Quarterly
    def QuarterlyIS(self):
        rep_is_qt = table(self.cur, 'MSreport_is_qt')
        rep_is_qt.iloc[:,2:8] = (
            rep_is_qt.iloc[:,2:8].replace(self.timerefs['dates']))
        cols = [col for col in rep_is_qt.columns if 'label' in col]
        rep_is_qt[cols] = (
            rep_is_qt[cols].replace(self.ColHeaders['header']))
        return rep_is_qt

    # Balance Sheet - Annual
    def AnnualBS(self):
        rep_bs_yr = table(self.cur, 'MSreport_bs_yr')
        rep_bs_yr.iloc[:,2:7] = (
            rep_bs_yr.iloc[:,2:7].replace(self.timerefs['dates']))
        cols = [col for col in rep_bs_yr.columns if 'label' in col]
        rep_bs_yr[cols] = (
            rep_bs_yr[cols].replace(self.ColHeaders['header']))
        return rep_bs_yr

    # Balance Sheet - Quarterly
    def QuarterlyBS(self):
        rep_bs_qt = table(self.cur, 'MSreport_bs_qt')
        rep_bs_qt.iloc[:,2:7] = (
            rep_bs_qt.iloc[:,2:7].replace(self.timerefs['dates']))
        cols = [col for col in rep_bs_qt.columns if 'label' in col]
        rep_bs_qt[cols] = (
            rep_bs_qt[cols].replace(self.ColHeaders['header']))
        return rep_bs_qt

    # Cashflow Statement - Annual
    def AnnualCF(self):
        rep_cf_yr = table(self.cur, 'MSreport_cf_yr')
        rep_cf_yr.iloc[:,2:8] = (
            rep_cf_yr.iloc[:,2:8].replace(self.timerefs['dates']))
        cols = [col for col in rep_cf_yr.columns if 'label' in col]
        rep_cf_yr[cols] = (
            rep_cf_yr[cols].replace(self.ColHeaders['header']))
        return rep_cf_yr

    # Cashflow Statement - Quarterly
    def QuarterlyCF(self):
        rep_cf_qt = table(self.cur, 'MSreport_cf_qt')
        rep_cf_qt.iloc[:,2:8] = (
            rep_cf_qt.iloc[:,2:8].replace(self.timerefs['dates']))
        cols = [col for col in rep_cf_qt.columns if 'label' in col]
        rep_cf_qt[cols] = (
            rep_cf_qt[cols].replace(self.ColHeaders['header']))
        return rep_cf_qt

    # 10yr Price History
    def PriceHistory(self):
        return table(self.cur, 'MSpricehistory')

    def __del__(self):
        self.cur.close()
        self.conn.close()


def table(cur, tbl, prnt = False):
    cur.execute('select * from {}'.format(tbl))
    cols = list(tbl_js[tbl].keys())
    if 'PRIMARY KEY' in cols: cols = cols[:-1]

    try:
        if prnt == True:
            msg = 'Creating DataFrame \'{}\' ...'
            print(msg.format(tbl.lower()))
        return pd.DataFrame(cur.fetchall(), columns=cols)
    except:
        raise


with open('input/tables.json') as file:
    tbl_js = json.load(file)
    tbl_names = list(tbl_js.keys())
