import redis
import json

r = redis.Redis(
    host="127.0.0.200", port=6379, db=0, password="123", decode_responses=True
)
data = r.hgetall("driver:gps")
for driver, json_str in data.items():
    parsed = json.loads(json_str)
    print(driver, parsed)
