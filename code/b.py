
from flask import jsonify, request
import redis
import requests

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)



cities_data = [
    {"city": "City1", "lat": 1.0, "lng": 2.0},
    {"city": "City2", "lat": 3.0, "lng": 4.0},
    {"city": "City3", "lat": 5.0, "lng": 6.0},
    {"city": "City4", "lat": 7.0, "lng": 8.0},
    {"city": "City5", "lat": 9.0, "lng": 10.0},
    {"city": "City6", "lat": 11.0, "lng": 12.0},

]


def closest_cities():
    city_name = request.args.get('city')
    page_size = int(request.args.get('page_size', 50))
    page = int(request.args.get('page', 0))

    # Fetch data from Cosmos DB (replace this with actual Cosmos DB query)
    city_data = next((city for city in cities_data if city["name"] == city_name), None)

    if not city_data:
        return jsonify({"error": "City not found"}), 404

    # Process data and calculate Eular distances
    all_cities_distances = []
    for other_city in cities_data:
        if other_city["name"] != city_name:
            distance = calculate_eular_distance(city_data["lat"], city_data["lng"], other_city["lat"], other_city["lng"])
            all_cities_distances.append({"city": other_city["name"], "distance": distance})

    # Sort cities by distance
    sorted_cities = sorted(all_cities_distances, key=lambda x: x["distance"])

    # Save result to Redis (replace this with actual Redis caching)
    redis_key = f"closest_cities:{city_name}"
    redis_client.set(redis_key, jsonify({"result": sorted_cities, "time_of_computing": 100}))

    # Return response
    return jsonify({"result": sorted_cities, "time_of_computing": 100})

def calculate_eular_distance(x1, y1, x2, y2):
    return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5

if __name__ == '__main__':
    print(closest_cities())