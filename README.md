# equiTable


### Objective: 
Create a simple screener for global equities with a concise display of financial data and performance indicators for all publicly listed equities from around the world.


### Current Package Dependencies:
- Python 3
- requests
- sqlite3


### Progress: 
Development is still in early stages. Current application only includes a command line scraper for MorningStar.com which uses SQLite to store data.

The current features include parsing of the following stock data from the API's listed in file api.json. This data is parsed with Python3 and stored in a relational SQLite database for later processing and analysis.


#### Data parsed:
- ~24k stock symbols across ~35 exchanges
- API's: 
	- Stock quote summary page (day hi, day lo, 52wk hi, 52wk lo, forward P/E, etc.)
	- Company profile with industry and sector data
	- 10yr stock valuation indicators (P/E, P/S, P/B, P/C)
	- Key performance ratios for past 10 yrs
	- Annual financial results for past 10 yrs
	- Income statement for past 5 yrs and 5 qtrs (pending ...)
	- Balance Sheet for past 5 yrs and 5 qtrs (pending ...)
	- Cashflow Statement for past 5 yrs and 5 qtrs (pending ...)


#### Next steps:
- Finish parsing code for Key Ratios and Historical Prices
- Create database views
- Implement Pandas fucntions for data processing and analysis
- Create web-based application for visualization of data (longer term)
