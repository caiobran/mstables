msTables
========

msTables is a [MorningStar.com](https://www.morningstar.com) scraper written in python that fetches, parses and stores financial and market data for over 70k securities into a relational SQLite database. The scraper provides a Command Line Interface (CLI) that allows the user greater flexibility for creating and managing multiple *.sqlite* files. Once data has been downloaded into the database files, [dataframes.py](dataframes.py) module can be used to easily create DataFrame objects from the database tables for further analysis.

The scraper should work as long as the structure of the responses does not change for the URL's used. See [input/api.json](input/api.json) for the complete list of URL's.

## Motivation:
As a fan of [Benjamin Graham](https://en.wikipedia.org/wiki/Benjamin_Graham)'s [value investing](https://en.wikipedia.org/wiki/Value_investing), I have always searched for sources of consolidated financial data that would allow me to identify 'undervalued' companies from a large pool of global public stocks. However, most *(if not all)* financial services that provide such data consolidation are not free and, as a small retail investor, I was not willing to pay for their fees. In fact, most of the data I needed was already available for free on various financial website, just not in a consolidated format. Therefore, I decided to create a web scraper for [MorningStar.com](https://www.morningstar.com), which is the website that I found to have the most available data in a more standardized and structured format. MS was also one of the only website services that published free financial performance data for the past 10 yrs, while most sites only provided free data for last 5 yrs.

## Next steps:
- Incorporate new code into fetch.py, under the create_tables method, to download and use the xml files listed on [MorningStar's robot.txt](https://www.morningstar.com/robots.txt) as references for when creating new database tables (current version only uses the [ms_sal-quote-stock-sitemap.xml](input/ms_sal-quote-stock-sitemap.xml) file which was downloaded and stored locally for reference)
- Finalize instructions for the scraper CLI
- Finalize Jupyter [notebook][1] with examples of how to use the DataFrames class from [dataframes.py](dataframes.py)


Instructions
------------

### Program Requirements:
The scraper should run on any Linux distribution that has Python3 and the following modules installed:

- [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/)
- [requests](http://docs.python-requests.org/en/master/)
- [sqlite3](https://docs.python.org/3/library/sqlite3.html)
- [pandas](https://pandas.pydata.org/)
- [numpy](http://www.numpy.org/)
- [git](https://pypi.org/project/GitPython/)

To view the [notebook with data visualization examples][1] mentioned in the instructions below, you must also have [Jupyter](https://jupyter.org/) and [matplotlib](https://matplotlib.org/) installed.

### Installation
Open a Linux terminal in the desired installation directory and execute `git clone https://github.com/caiobran/msTables.git` to download the project files.

### Using the scraper Command Line Interface (CLI).

Execute `python main.py` from the project root directory to start the scraper CLI. If the program has started correctly, you should see the following interface:

![Imgur](https://i.imgur.com/D1Y25LN.png)

1. If you are running the scraper for the first time, enter option `1` to create the initial SQLite database tables.
2. Once that action has been completed, and on subsequent runs, enter option `2` to download the latest data from the MorningStar [URL's](input/api.json).
    - You will be prompted to enter the number of records you would like to update. You can enter a large number such as `1000000` if you would like the scraper to update all records. You may also enter smaller quantities if you do not want the scraper to run for a long period of time.
    - On average, it has taken about three days to update all records with the current program parameters and an Internet speed > 100mbps. The program can be interrupted at any time using <kbd>Ctrl</kbd>+<kbd>C</kbd>.
    - One may want to increase the size of the multiprocessing pool in [main.py](main.py) that is used for URL requests to speed up the scraper. *However, I do not recommend doing that as the MorningStar servers will not be too happy about receiving many simultaneous GET requests from the same IP address.*

*(documentation in progress, to be updated with instructions on remaining actions)*

### How to access the SQLite database tables using module _dataframes.py_:
The scraper will automatically create a directory *db/* in the root folder to store the *.sqlite* database files generated. The current file name in use will be displayed on the scraper CLI under action `0` (see CLI figure above). Database files will contain a relational database with the following main tables:

**Database Tables**

- _**Master**_: Main bridge table with complete list of security and exchange symbol pairs, security name, sector, industry, security type, and FY end dates
- _**MSheader**_: Quote Summary data with day hi, day lo, 52wk hi, 52wk lo, forward P/E, div. yield, volumes, and current P/B, P/S, and P/CF ratios
- _**MSvaluation**_: 10yr stock valuation indicators (P/E, P/S, P/B, P/C)
- _**MSfinancials**_: Key performance ratios for past 10 yrs
- _**MSratio_cashflow**_, _**MSratio_financial**_, _**MSratio_growth**_, _**MSratio_profitability**_, _**MSratio_efficiency**_: Financial performance ratios for past 10 yrs
- _**MSreport_is_yr**_, _**MSreport_is_qt**_: Income Statements for past 5 yrs and 5 qtrs, respectively
- _**MSreport_bs_yr**_, _**MSreport_bs_qt**_: Balance Sheets for past 5 yrs and 5 qtrs, respectively
- _**MSreport_cf_yr**_, _**MSreport_cf_qt**_: Cash Flow Statements for past 5 yrs and 5 qtrs, respectively
- _**MSpricehistory**_: Table with current 50, 100 and 200 day price averages and 10 year price history (compressed)

**How to slice and dice the data using dataframes.py**

Module _dataframes_ contains a class that can be used to generate pandas DataFrames for the data in the SQLite database file that is generated by the web crawler.

See the Jupyter notebook [data_overview.ipynb][1] for examples on how to create DataFrame objects to manipulate and visualize the data. Below is a list of all content found in the notebook:

**Content**

1. [Required modules and matplotlib backend](#modules)
1. [Creating a master (bridge table) DataFrame instance using the DataFrames class](#master)
1. [Methods for creating DataFrame instances](#methods)
    1. `quoteheader` - [MorningStar (MS) Quote Header](#quote)
    1. `valuation` - [MS Valuation table with Price Ratios (P/E, P/S, P/B, P/C) for the past 10 yrs](#val)
    1. `keyratios` - [MS Ratio - Key Financial Ratios & Values](#keyratios)
    1. `finhealth` - [MS Ratio - Financial Health](#finhealth)
    1. `profitability` - [MS Ratio - Profitability](#prof)
    1. `growth` - [MS Ratio - Growth](#growth)
    1. `cfhealth` - [MS Ratio - Cash Flow Health](#cfh)
    1. `efficiency` - [MS Ratio - Efficiency](#eff)
    1. `annualIS` - [MS Annual Income Statements](#isa)
    1. `quarterlyIS` - [MS Quarterly Income Statements](#isq)
    1. `annualBS` - [MS Annual Balance Sheets](#bsa)
    1. `quarterlyBS` - [MS Quarterly Balance Sheets](#bsq)
    1. `annualCF` - [MS Annual Cash Flow Statements](#cfa)
    1. `quarterlyCF` - [MS Quarterly Cash Flow Statements](#cfq)
1. [Performing statistical analysis](#stats)
    1. [Count of database records](#stats)
    1. [Last updated dates](#lastupdate)
    1. [Number of records by security type](#type)
    1. [Number of records by country, based on of exchanges](#country)
    1. [Number of records per exchange](#exchange)
    1. [Number of stocks by sector](#sector)
    1. [Number of stocks by industry](#industry)
    1. [Mean price ratios (P/E, P/S, P/B, P/CF) of stocks by sectors](#meanpr)
1. [Applying various criteria to filter common stocks](#value) *(in progress)*
    1. Rule 1: No earnings deficit (loss) for past 5 or 7 years
    1. Rule 2: Uniterrupted and increasing Dividends for past 5 yrs
    1. Rule 3: P/E Ratio of 25 or less for the past 7 yrs and less then 20 for TTM
    1. Rule 4: P/B Ratio of 1.2 or less for TTM
    1. Rule 5: Filter for "bargain issues"
    1. Rule 6: Operating Cash Flow growth for the past 7 yr
    1. Rule 7: Owner earnings growth rate > 6% over past 7 years
    1. Rule 8: Long-term debt < 50% of total capital
    1. Rule 9: NAV per share > Stock Price
    1. Rule 10: Growth Stocks as described by Benjamin Graham in The Intelligent Investor
    1. Rule 11: Positive ratio of earnings to fixed charge
    1. Rule 12: CAN SLIM

MIT License
-----------

Copyright (c) 2019 Caio Brandao

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

[1]:https://github.com/caiobran/msTables/blob/master/data_overview.ipynb
