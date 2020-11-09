# My_test
Script should create 8 csv files: 1 file - list of pairs, 1 file - merged candles, 6 files - market trades
for each pair on each market.

All numeric columns in the OHLC DataFrames are float64
In market trades DataFrames вata processed with pandas.to_numeric
# Short description of functions
* get_symbols_table(): Returns a DataFrame with pairs available for both exchanges and their names for each exchange
* get_random_3(DataFrame): returns [list,list] with the names of 3 random pairs for each exchange1

* before_after_chart_for_okex(func): Decorator function for visual debugging of data smoothing.
* anomaly_analysis_and_smoothing(DataFrame): z test for each value of variance and smoothing
* get_ohlc_data_from_okex(ticker: str): returns a DataFrame with data on all daily candlesticks of a trading pair on Okex
* get_ohlc_data_from_binance(ticker: str): returns a DataFrame with data on all daily candlesticks of a trading pair on Binance
* merge_candles_from_random_tickers(tickers: [list,list]): returns DataFrame with merged candlestick data for each pair
* get_market_trades_okex(ticker): returns a DataFrame with information about the last 100 trades on Okex
* get_market_trades_binance(ticker): returns a DataFrame with information about the last 1000 trades on Binance
# Sample output
Below is an example of the result of the function anomaly_analysis_and_smoothing(DataFrame) for ZEC-BTC.

![alt tag](http://dl3.joxi.net/drive/2020/11/09/0005/0439/369079/79/9e25a2e6ce.png "ZEC-BTC")​
![alt tag](http://dl4.joxi.net/drive/2020/11/09/0005/0439/369079/79/820df7777a.png "ZEC-BTC")​
# Known Issues
* The data for some of the trading instruments may contain flat candlesticks.
 This is due to the fact that the trade was conducted with zero volume or volatility.
* The first candles usually contain zeros or low-quality data, so the first three are truncated in the output
* You can only use the graphic decorator for one-time use of the function anomaly_analysis_and_smoothing(DataFrame)