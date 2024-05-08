import yfinance

hist = yfinance.Ticker("AAPL").history(
    period = "1d",
    interval = "1h",
    start = "2023-12-03",
    end = "2024-01-10",
    prepost = True
)
print(hist.head())