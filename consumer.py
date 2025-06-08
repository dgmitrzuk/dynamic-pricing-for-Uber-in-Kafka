from kafka import KafkaConsumer
import json
import pandas as pd
from dynamic_pricing import predict_price
from static_pricing import calculate_price

SERVER = 'localhost:9092'
def create_consumer(bootstrap_servers=None, topic='ride_request'):
    return KafkaConsumer(
        topic,
        bootstrap_servers=SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='ride_request_consumer_group'
    )


if __name__ == '__main__':
    consumer = create_consumer()
    for msg in consumer:
        raw = msg.value
        ride_id = msg.value['ride_id']
        df  = pd.DataFrame([raw])
        df = df.drop(columns=['ride_id'])
        df_dummies = pd.get_dummies(df, drop_first=True)
        dynamic_price = predict_price(df_dummies)
        static_price = calculate_price(df)
        dp = dynamic_price[0]
        sp = static_price
        print(f"-> Ride: {ride_id} dynamic price: {dp:.2f} static price: {sp:.2f}")

