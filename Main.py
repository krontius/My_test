# !/user/bin/python3
import pandas as pd
import requests
import json
import random
import datetime
import ccxt
import pytz
import plotly.graph_objects as go

BINANCE = ccxt.binance()
datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
BINANCE.load_markets()


def get_symbols_table():
    # returns a DataFrame with pairs available for both exchanges and their names for each exchange
    url_okex = 'https://www.okex.com/api/spot/v3/instruments'
    api_okex = requests.get(url_okex)
    data_okex = json.loads(api_okex.text)
    df_okex_tickers = pd.DataFrame(pd.json_normalize(data_okex)['instrument_id'])
    df_okex_tickers['okex'] = df_okex_tickers.replace('\-', '', regex=True).astype(str)
    df_binance_tickers_ccxt = pd.DataFrame(BINANCE.markets.keys())
    df_binance_tickers_ccxt['symbol'] = df_binance_tickers_ccxt.replace('/', '', regex=True).astype(str)
    df_symbols = pd.merge(left=df_binance_tickers_ccxt, right=df_okex_tickers, left_on='symbol',
                          right_on='okex')
    df_symbols.rename(
        columns={0: 'binance_name', 'symbol': 'binance_key', 'instrument_id': 'okex_name', 'okex': 'okex_key'},
        inplace=True)
    return df_symbols


def get_random_3(df):
    # returns [list,list] with the names of 3 random pairs for each exchange
    tickers_okex = []
    tickers_binance = []
    df_symbols = get_symbols_table()
    for i in range(3):
        a = random.randrange(0, len(df) - 1)
        tickers_binance.append(df_symbols['binance_name'].iloc[a])
        tickers_okex.append(df_symbols['okex_name'].iloc[a])
    return [tickers_okex, tickers_binance]


def plot_ohlc_binance(df, title=''):
    fig1 = go.Figure(data=go.Ohlc(x=df.index,
                                  open=df['Open_binance'],
                                  high=df['Hight_binance'],
                                  low=df['Low_binance'],
                                  close=df['Close_binance']))
    fig1.update_layout(title=title)
    fig1.show()
    return


def before_after_chart_for_okex(func):
    def wrapper(df):
        fig1 = go.Figure(data=go.Ohlc(x=df.index,
                                      open=df['Open_okex'],
                                      high=df['Hight_okex'],
                                      low=df['Low_okex'],
                                      close=df['Close_okex']))
        fig1.update_layout(title='before')
        fig1.show()
        func(df)
        df = func(df)
        fig1 = go.Figure(data=go.Ohlc(x=df.index,
                                      open=df['Open_okex'],
                                      high=df['Hight_okex'],
                                      low=df['Low_okex'],
                                      close=df['Close_okex']))
        fig1.update_layout(title='after')
        fig1.show()

    return wrapper


@before_after_chart_for_okex
def anomaly_analysis_z(df_res):
    ohlc_cols = [['Open_binance', 'Hight_binance', 'Low_binance', 'Close_binance'],
                 ['Open_okex', 'Hight_okex', 'Low_okex', 'Close_okex']]
    # plot_ohlc_okex(df_res, 'Okex_' + str(n))
    for i in range(4):
        df_res[ohlc_cols[0][i] + '_dev'] = 2 * abs(df_res[ohlc_cols[0][i]] - df_res[ohlc_cols[1][i]]) / (
                df_res[ohlc_cols[0][i]] + df_res[ohlc_cols[1][i]])
        df_res[ohlc_cols[0][i] + '_Z_dev'] = (df_res[ohlc_cols[0][i] + '_dev'] - df_res[
            ohlc_cols[0][i] + '_dev'].mean()) / df_res[ohlc_cols[0][i] + '_dev'].std()
    for i in range(4):
        df_res[ohlc_cols[1][i] + '_dev'] = 2 * abs(df_res[ohlc_cols[1][i]] - df_res[ohlc_cols[0][i]]) / (
                df_res[ohlc_cols[1][i]] + df_res[ohlc_cols[0][i]])
        df_res[ohlc_cols[1][i] + '_Z_dev'] = (df_res[ohlc_cols[1][i] + '_dev'] - df_res[
            ohlc_cols[1][i] + '_dev'].mean()) / df_res[ohlc_cols[1][i] + '_dev'].std()
    df_res.loc[abs(df_res['Open_okex_Z_dev']) > 7.5, ['Open_okex']] = df_res['Close_okex'].shift(1)
    df_res.loc[abs(df_res['Close_okex_Z_dev']) > 7.5, ['Close_okex']] = df_res['Open_okex'].shift(-1)
    df_res.loc[abs(df_res['Hight_okex_Z_dev']) > 7.5, ['Hight_okex']] = \
        (df_res['Hight_binance'] + df_res['Hight_okex']) / 2
    df_res.loc[abs(df_res['Low_okex_Z_dev']) > 7.5, ['Low_okex']] = \
        (df_res['Low_binance'] + df_res['Low_okex']) / 2
    pd.options.display.float_format = '{:.8f}'.format
    df_res = df_res.round(8)
    print(df_res)
    plot_ohlc_binance(df_res, 'Binance')
    return df_res


def get_ohlc_data_from_okex(ticker):
    timeframe = 86400
    end = datetime.datetime.combine(datetime.datetime.today(), datetime.datetime.min.time())
    start = end - datetime.timedelta(days=200)
    df_ret = pd.DataFrame()
    while True:
        url = 'https://www.okex.com/api/spot/v3/instruments/' + str(ticker) + '/candles?granularity=' + str(
            timeframe) + '&start=' + str(start.isoformat()) + '.000Z&end=' + str(end.isoformat()) + '.000Z'
        api = requests.get(url)
        data = json.loads(api.text)
        df = pd.DataFrame(data)
        df_ret = pd.concat([df_ret, df])
        end = start
        start = end - datetime.timedelta(days=200)
        if len(df) < 200:
            break

    df_ret.rename(columns={0: 'DateTime', 1: 'Open', 2: 'Hight', 3: 'Low', 4: 'Close', 5: 'Volume'},
                  inplace=True)
    df_ret.sort_values(by='DateTime', ascending=True, inplace=True)
    df_ret['DateTime'] = df_ret.replace('T16', 'T00', regex=True)
    df_ret['DateTime'] = df_ret['DateTime'].replace('T', ' ', regex=True)
    df_ret['DateTime'] = df_ret['DateTime'].replace('.000Z', '', regex=True).astype('datetime64[ns]')
    df_ret.set_index('DateTime', inplace=True)
    df_ret.name = ticker
    return df_ret


def get_ohlc_data_from_binance(ticker):
    # returns a DataFrame with data on all daily candlesticks of a trading pair on Binance
    trading_pair = ticker
    df_ret = pd.DataFrame()
    since = 0
    while True:
        candles = BINANCE.fetch_ohlcv(trading_pair, '4h', since)
        since = candles[-1][0] + 3600
        candles = [[BINANCE.iso8601(candle[0])] + candle[1:] for candle in candles]
        df = pd.DataFrame(candles)
        df_ret = pd.concat([df_ret, df])
        if len(df) < 500:
            break
    df_ret.rename(columns={0: 'DateTime', 1: 'Open', 2: 'Hight', 3: 'Low', 4: 'Close', 5: 'Volume'},
                  inplace=True)
    df_ret = df_ret.apply(pd.to_numeric, errors='ignore')
    df_ret['DateTime'] = df_ret['DateTime'].replace('T', ' ', regex=True)
    df_ret['DateTime'] = df_ret['DateTime'].replace('.000Z', '', regex=True).astype('datetime64[ns]')
    df_ret['DateTime'] = df_ret['DateTime'].map(lambda x: x - datetime.timedelta(hours=16))
    df_ret.set_index('DateTime', inplace=True)
    df_ret = df_ret.resample('D').agg({'Open': 'first',
                                       'Hight': 'max',
                                       'Low': 'min',
                                       'Close': 'last',
                                       'Volume': 'sum', })
    df_ret.name = ticker
    return df_ret


def merge_candles_from_random_tickers(tickers, count):
    tickers_okex = tickers[0]
    tickers_binance = tickers[1]
    for i in range(count):
        print(tickers_binance[i])
        df_binance = get_ohlc_data_from_binance(tickers_binance[i])
        df_okex = get_ohlc_data_from_okex(tickers_okex[i])
        df_result = pd.merge(df_binance, df_okex, on=['DateTime'], how='inner')
        df_result.rename(columns={'Open_x': 'Open_binance', 'Hight_x': 'Hight_binance', 'Low_x': 'Low_binance',
                                  'Close_x': 'Close_binance', 'Volume_x': 'Volume_binance', 'Open_y': 'Open_okex',
                                  'Hight_y': 'Hight_okex', 'Low_y': 'Low_okex', 'Close_y': 'Close_okex',
                                  'Volume_y': 'Volume_okex'},
                         inplace=True)
        df_result = df_result.apply(pd.to_numeric, errors='ignore')
        pd.options.display.float_format = '{:.8f}'.format
        df_result = df_result.round(8)
        # delete the first 3 rows because they always contains the worst data for model
        df_result = df_result.iloc[3:]
        df_result.rename = tickers_okex[i] + '_ohlc'
    return df_result


def merge_candles_from_3_random_tickers_old(tickers):
    # creates 3 csv files in the project folder with merged candlestick data for each pair
    tickers_okex = tickers[0]
    tickers_binance = tickers[1]
    for i in range(3):
        print(tickers_binance[i])
        df_binance = get_ohlc_data_from_binance(tickers_binance[i])
        df_okex = get_ohlc_data_from_okex(tickers_okex[i])
        df_result = pd.merge(df_binance, df_okex, on=['DateTime'], how='outer')
        df_result.sort_values(by='DateTime', ascending=True, inplace=True)
        df_result.rename(columns={'Open_x': 'Open_binance', 'Hight_x': 'Hight_binance', 'Low_x': 'Low_binance',
                                  'Close_x': 'Close_binance', 'Volume_x': 'Volume_binance', 'Open_y': 'Open_okex',
                                  'Hight_y': 'Hight_okex', 'Low_y': 'Low_okex', 'Close_y': 'Close_okex',
                                  'Volume_y': 'Volume_okex'},
                         inplace=True)
        df_result = df_result.apply(pd.to_numeric, errors='ignore')
        df_result['DateTime'] = df_result['DateTime'].replace('T', ' ', regex=True)
        df_result['DateTime'] = df_result['DateTime'].replace('.000Z', '', regex=True).astype('datetime64[ns]')
        df_result.set_index('DateTime', inplace=True)
        df_result = df_result.resample('D').agg({'Open_binance': 'first',
                                                 'Hight_binance': 'max',
                                                 'Low_binance': 'min',
                                                 'Close_binance': 'last',
                                                 'Volume_binance': 'sum',
                                                 'Open_okex': 'first',
                                                 'Hight_okex': 'max',
                                                 'Low_okex': 'min',
                                                 'Close_okex': 'last',
                                                 'Volume_okex': 'sum'})
        pd.options.display.float_format = '{:.8f}'.format
        df_result = df_result.round(8)
        df_result = df_result[:-1]
        df_result = df_result[:-1]
        print(df_result)
        df_result.to_csv(tickers_okex[i] + '.csv', sep=';')
    return


def get_market_trades_okex(ticker):
    # returns a DataFrame with information about the last 100 trades on Okex
    url = 'https://www.okex.com/api/spot/v3/instruments/' + str(ticker) + '/trades'
    api = requests.get(url)
    data = json.loads(api.text)
    df_ret = (pd.json_normalize(data))
    df_ret.rename(columns={0: 'DateTime', 1: 'trade_id', 2: 'price', 3: 'size', 4: 'side'},
                  inplace=True)
    pd.options.display.float_format = '{:.8f}'.format
    df_ret = df_ret.round(8)

    return df_ret


def get_market_trades_binance(ticker):
    # returns a DataFrame with information about the last 500 trades on Okex
    ticker = str(ticker).replace('/', '')
    url = 'https://api.binance.com/api/v3/trades?symbol=' + str(ticker)
    api = requests.get(url)
    data = json.loads(api.text)
    df_ret = (pd.json_normalize(data))
    df_ret.rename(
        columns={0: 'id', 1: 'price', 2: 'qty', 3: 'quoteQty', 4: 'time', 5: 'isBuyerMaker', 6: 'isBestMatch'},
        inplace=True)
    df_ret = df_ret.apply(pd.to_numeric, errors='ignore')
    pd.options.display.float_format = '{:.8f}'.format
    df_ret = df_ret.round(8)
    return df_ret


if __name__ == '__main__':
    # configuration should create 10 csv files: 1 file - list of pairs, 3 files - merged candles,6 files - market trades
    # df_symbols = get_symbols_table()
    # df_symbols.to_csv('pairs.csv', sep=';')
    # tickers = get_random_3(df_symbols)
    # print(tickers)
    # merge_candles_from_3_random_tickers(tickers)
    # for elem in tickers[0]:
    #     get_market_trades_okex(elem).to_csv(elem + '_okex_market_trades.csv', sep=';')
    # for elem in tickers[1]:
    #     get_market_trades_binance(elem).to_csv(str(elem).replace('/', '') + '_binance_market_trades.csv', sep=';')
    anomaly_analysis_z(
        merge_candles_from_random_tickers(
            get_random_3(
                get_symbols_table()), 1))
    # .to_csv('test.csv', sep=';')
