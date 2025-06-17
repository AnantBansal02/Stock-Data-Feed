import upstox_client
import json

apiInstance = upstox_client.HistoryV3Api()

# candles data response structure:
# response ->response.status, response.data
# response.data -> list[list[object]]
def get_historical_candle_data(symbol: str, toDate: str,fromDate: str, timePeriod: str, multiplier: str = "1"):
    try:
        response = apiInstance.get_historical_candle_data1(symbol, timePeriod, multiplier, toDate, fromDate)
        return response
    except Exception as e:
        print("Exception when calling HistoryV3Api->get_historical_candle_data1: %s\n" % e)

def get_intraday_candle_data(symbol: str, timePeriod: str, multiplier: str = "1"):
    try:
        response = apiInstance.get_intra_day_candle_data(symbol, timePeriod, multiplier)
        return response
    except Exception as e:
        print("Exception when calling HistoryV3Api->get_intra_day_candle_data: %s\n" % e)

# get_historical_candle_data("NSE_EQ|INE848E01016", "2025-06-06", "2025-06-06", "minutes")
# get_intraday_candle_data("NSE_EQ|INE466L01038", "minutes")