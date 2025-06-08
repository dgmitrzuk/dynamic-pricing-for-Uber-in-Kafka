from kafka import KafkaProducer
import json
import random
import uuid
import time

SERVER = 'localhost:9092'
def create_producer():
    return KafkaProducer(
        bootstrap_servers=SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def random_origin_coord():
    lat = random.uniform(52.0977778, 52.3680556)
    lon = random.uniform(20.8516667, 21.2711111)
    return (lat, lon)


def random_destination_coord(origin, radius_km=30):
    max_deg = radius_km / 111
    return (
        origin[0] + random.uniform(-max_deg, max_deg),
        origin[1] + random.uniform(-max_deg, max_deg)
    )


def expected_ride_duration(origin, destination, speed_kmh=65):
    from geopy.distance import geodesic
    distance_km = geodesic(origin, destination).km
    return distance_km / speed_kmh


def send_ride_request(producer):
    ride_id = str(uuid.uuid4())
    origin = random_origin_coord()
    destination = random_destination_coord(origin)
    event = {
        'ride_id': ride_id,
        'Expected_Ride_Duration': expected_ride_duration(origin, destination),
        'Time_of_Booking': random.choice(['Morning', 'Afternoon', 'Evening', 'Night']),
        'Location_Category': random.choice(['Urban', 'Suburban', 'Rural']),
        'Customer_Loyalty_Status': random.choice(['Bronze', 'Silver', 'Gold']),
        'Number_of_Past_Rides': random.randint(0, 200),
        'Average_Ratings': round(random.uniform(1, 5), 2),
        'Vehicle_Type': random.choice(['Economy', 'Premium', 'SUV', 'Luxury']),
        'Number_of_Riders': random.randint(50, 200),
        'Number_of_Drivers': random.randint(30, 150)
    }
    producer.send('ride_request', value=event)
    producer.flush()
    print(f"-> Sent ride request: {ride_id} origin: {origin} destination: {destination}")
    print(f"event: {event}")
    print("\n")




if __name__ == '__main__':
    prod = create_producer()
    while True:
        send_ride_request(prod)
        time.sleep(1)