msTables
========

### Objective:
Create a MorningStar.com scraper which stores the data into a relational SQLite database so one can perform fundamental analysis across publicly listed equities from around the world. *(to be updated with tables.py info)*

### Progress:
Command line interface for the MorningStar.com scraper has been published including the automated parsing and storing of the data into an .slqite file.

### Next steps:
- Implement pandas function for data processing and analysis (currently under test/)
- Finalize documentation / instructions on how to use the tool including Jupyter examples


Instructions
------------

From within the equiTable folder, execute file main.py with a Python interpreter to start the application. All .sqlite files created will be stored under db/. *(to be updated with step-by-step instructions)*

This program should work as long as the structure of the responses does not change for the API's listed in [input/api.json](input/api.json).

Open file [data_overview.ipynb](data_overview.ipynb) from Jupyter to see examples on how to work with the data using the pandas and matplotlib modules.

### Package required to run scraper:
- Python3
- Beautiful Soup
- requests
- sqlite3
- pandas
- numpy

### Main database tables created:
- `Master`:     Main bridge table with complete list of security and exchange symbol pairs, security name, sector, industry, type, and FY end dates
- `MSheader`: Quote Summary data with day hi, day lo, 52wk hi, 52wk lo, forward P/E, div. yield, volumes, and current P/B, P/S, and P/CF ratios
- `MSValuation`: 10yr stock valuation indicators (P/E, P/S, P/B, P/C)
- `MSfinancials`: Key performance ratios for past 10 yrs
- `MSratio_cashflow`, `MSratio_financial`, `MSratio_growth`, `MSratio_profitability`, `MSratio_efficiency`: Financial performance ratios for past 10 yrs
- `MSreport_is_yr`, `MSreport_is_qt`: Income Statements for past 5 yrs and 5 qtrs, respectively
- `MSreport_bs_yr`, `MSreport_bs_qt`: Balance Sheets for past 5 yrs and 5 qtrs, respectively
- `MSreport_cf_yr`, `MSreport_cf_qt`: Cash Flow Statements for past 5 yrs and 5 qtrs, respectively


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
