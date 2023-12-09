import requests
from datetime import date

def fetch_bitcoin_data() -> None:
    api_url = "https://data.messari.io/api/v1/assets/bitcoin/metrics"

    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()

        if 'data' in data and 'market_data' in data['data']:
            market_data = data['data']['market_data']
            ohlcv_data = market_data.get('ohlcv_last_24_hour', [])

            if ohlcv_data:
                latest_ohlcv = ohlcv_data[-1]  # Taking the latest OHLCV data

                bitcoin_data = {
                    "date": str(date.today()),
                    "volume_last_24_hours": market_data.get('volume_last_24_hours', ''),
                    "open": latest_ohlcv.get('open', ''),
                    "close": latest_ohlcv.get('close', ''),
                    "low": latest_ohlcv.get('low', ''),
                    "high": latest_ohlcv.get('high', '')
                }

                # Displaying the fetched Bitcoin data
                print("Bitcoin Data:")
                for key, value in bitcoin_data.items():
                    print(f"{key}: {value}")

            else:
                print("No OHLCV data found")

        else:
            print("Unable to retrieve Bitcoin data from the API response")

    else:
        print("Failed to fetch Bitcoin data")

if __name__ == "__main__":
    fetch_bitcoin_data()



