def fetch_ticker(company_tickers, company):
    try:
        ticker = company_tickers.loc[company]["Ticker Symbol"]
    except KeyError:
        ticker = "NA"
    return ticker
