import json
import time
import requests
from kafka import KafkaProducer

# Define the Kafka bootstrap servers
bootstrap_servers = 'localhost:9092'  # Adjust with your local Kafka bootstrap servers

# Define the CoinCap API endpoint to get data for 10 coins
coincap_api_url = 'https://api.coincap.io/v2/assets'
coins_to_fetch = 10

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to fetch data from CoinCap API for a specific coin
def fetch_coincap_data(coin_id):
    coin_url = f'{coincap_api_url}/{coin_id}'
    response = requests.get(coin_url)
    data = response.json()
    return data

# Simulate sending messages every 5 seconds
while True:
    timestamp = int(time.time())  # Get the current timestamp

    coins = ['bitcoin', 'ethereum', 'eos', 'stellar', 'litecoin', 'cardano', 'tether', 'iota', 'tron']

    # Fetch data for each coin and send a message
    for coin_id in coins:
        coin_data = fetch_coincap_data(coin_id)
        message = {
            "coin": {
                "id": str(coin_data['data']['id']),
                "rank": int(coin_data['data']['rank']),
                "symbol": str(coin_data['data']['symbol']),
                "name": str(coin_data['data']['name']),
                "supply": float(coin_data['data']['supply']),
                "maxSupply": float(coin_data['data']['maxSupply']) if coin_data['data']['maxSupply'] else None,
                "marketCapUsd": float(coin_data['data']['marketCapUsd']),
                "volumeUsd24Hr": float(coin_data['data']['volumeUsd24Hr']),
                "priceUsd": float(coin_data['data']['priceUsd']),
                "changePercent24Hr": float(coin_data['data']['changePercent24Hr']),
                "vwap24Hr": float(coin_data['data']['vwap24Hr']),
                "explorer": str(coin_data['data']['explorer'])
            },
            "date": coin_data['timestamp']
        }
        print(message)
        producer.send('coin_topic', value=message)

    # Sleep for 60 seconds before sending the next set of messages
    time.sleep(30)

