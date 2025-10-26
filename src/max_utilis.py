from api_client import MaxAPIClient
from datetime import datetime, timedelta
import asyncio
import pandas as pd


def get_parsed_max_k_line_df(raw_data):
    raw_data =  pd.DataFrame(raw_data , columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'], unit='s')
    raw_data = raw_data.set_index('timestamp')
    return raw_data

if __name__ == '__main__':
    async def main():
        ts = int((datetime.now() - timedelta(days=1)).timestamp()*1000)
        MaxClient = MaxAPIClient()
        data = await MaxClient.get_api_v3_k(symbol='usdttwd', timestamp=ts, period=1, limit=1000)

        res = get_parsed_max_k_line_df(data)
        print(res['close'].iloc[-59:].mean())
        # print(res)
    asyncio.run(main())