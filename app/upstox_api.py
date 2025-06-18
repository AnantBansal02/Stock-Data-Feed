import upstox_client
from upstox_client.api import HistoryV3Api
from upstox_client import Configuration, ApiClient
import urllib3
from urllib3.util.retry import Retry
import logging

logger = logging.getLogger(__name__)

# 1. Create custom PoolManager
custom_pool_manager = urllib3.PoolManager(
    num_pools=100,
    maxsize=20,
    retries=Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
    ),
)

# 2. Create config and ApiClient
configuration = Configuration()
api_client = ApiClient(configuration=configuration)

# 3. Patch the internal pool manager safely
api_client.rest_client.pool_manager = custom_pool_manager

# 4. Instantiate the API with this patched client
apiInstance = HistoryV3Api(api_client)

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