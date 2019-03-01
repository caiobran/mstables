equiTable
=========

### Objective:
Create a MorningStar.com scraper which stores the data into a relational SQLite database so one can perform fundamental analysis across all publicly listed equities from around the world.

### Progress:
Command line interface for the MorningStar.com scraper has been published including the automated parsing and storing of the data into an .slqite file.


Instructions
------------

From within the equiTable folder, execute file main.py with a Python interpreter to start the application. All .sqlite files created will be stored under db/.

### Current Package Dependencies:
- Python 3
- Beautiful Soup
- requests
- sqlite3
- pandas
- numpy

### MorningStar data parsed into database:
- Stock quote summary page (day hi, day lo, 52wk hi, 52wk lo, forward P/E, etc.)
- Company profile with industry and sector data
- 10yr stock valuation indicators (P/E, P/S, P/B, P/C)
- Key performance ratios for past 10 yrs
- Annual financial results for past 10 yrs
- Income statement for past 5 yrs and 5 qtrs
- Balance Sheet for past 5 yrs and 5 qtrs
- Cashflow Statement for past 5 yrs and 5 qtrs

### Next steps:
- Implement pandas function for data processing and analysis (currently under test/)
- Create web-based application for visualization of data (longer term)


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
