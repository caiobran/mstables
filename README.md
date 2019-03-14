msTables
========

Project Overview
----------------

### Objective:
Create a MorningStar.com scraper that stores data into a relational SQLite database for further analysis. *(to be updated with tables.py info)*

### Progress:
Parser with CLI has been published. Current version includes automatic parsing and storing of data. 

### Next steps:
- Implement pandas function for data processing and analysis (currently under test/)
- Finalize documentation / instructions on how to use the tool including Jupyter examples

Instructions
------------

### Program Requirements:
The scraper should run on any Linux distribution that has Python3 and the following modules installed:

- Beautiful Soup
- requests
- sqlite3
- pandas
- numpy
- git

To view the [data visualization examples][1] mentioned in the instructions, you must also have [jupyter](https://jupyter.org/) and [matplotlib](https://matplotlib.org/) installed.

### Installation
Open a Linux terminal in the desired installation directory and execute `git clone https://github.com/caiobran/msTables.git` to download the project files.

#### Command Line Interface (CLI).

Execute `python main.py` from the project root directory to start the scraper CLI. If the program started correctly, you should see the following:

![Imgur](https://i.imgur.com/aisCne1.png)

If you are running the scraper for the first time, you must first enter `1` to create the initial database tables. Once that action has been completed and on subsequent runs, enter `5` to fetch data from the MorningStar API's.
*(documentation in progress, to be updated with instructions on remaining actions)*

This program should work as long as the structure of the responses does not change for the API's listed in [input/api.json](input/api.json).

### Database tables:
The scraper will automatically create a directory *db/* in the root folder to store the *.sqlite* files generated. Each file will contains a relational database with the following main tables:

- `Master`: Main bridge table with complete list of security and exchange symbol pairs, security name, sector, industry, type, and FY end dates
- `MSheader`: Quote Summary data with day hi, day lo, 52wk hi, 52wk lo, forward P/E, div. yield, volumes, and current P/B, P/S, and P/CF ratios
- `MSValuation`: 10yr stock valuation indicators (P/E, P/S, P/B, P/C)
- `MSfinancials`: Key performance ratios for past 10 yrs
- `MSratio_cashflow`, `MSratio_financial`, `MSratio_growth`, `MSratio_profitability`, `MSratio_efficiency`: Financial performance ratios for past 10 yrs
- `MSreport_is_yr`, `MSreport_is_qt`: Income Statements for past 5 yrs and 5 qtrs, respectively
- `MSreport_bs_yr`, `MSreport_bs_qt`: Balance Sheets for past 5 yrs and 5 qtrs, respectively
- `MSreport_cf_yr`, `MSreport_cf_qt`: Cash Flow Statements for past 5 yrs and 5 qtrs, respectively

See Jupyter notebook [data_overview.ipynb][1] for examples on how to create DataFrame objects to manipulate and visualize the data.


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

[1]:data_overview.ipyn
