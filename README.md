# equiTable


### Objective:
Create a MorningStar.com scraper which stores the data into a relational SQLite database so one can perform fundamental analysis across all publicly listed equities from around the world.


### Current Package Dependencies:
- Python 3
- Beautiful Soup
- pandas
- numpy
- requests
- sqlite3


### Progress:
Command line interface for the MorningStar.com scraper has been published including the automated parsing and storing of the data into an .slqite file.


### Instructions:
Execute _ _ init _ _ .py with a Python interpreter to start the application. All .sqlite files created will be stored under db/.


#### Data parsed:
- Stock quote summary page (day hi, day lo, 52wk hi, 52wk lo, forward P/E, etc.)
- Company profile with industry and sector data
- 10yr stock valuation indicators (P/E, P/S, P/B, P/C)
- Key performance ratios for past 10 yrs
- Annual financial results for past 10 yrs
- Income statement for past 5 yrs and 5 qtrs
- Balance Sheet for past 5 yrs and 5 qtrs
- Cashflow Statement for past 5 yrs and 5 qtrs


#### Next steps:
- Implement pandas function for data processing and analysis (currently under test/)
- Create web-based application for visualization of data (longer term)
