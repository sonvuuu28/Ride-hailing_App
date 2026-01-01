from flask import Flask, jsonify
import redis
import json

app = Flask(__name__)
r = redis.Redis(host="redis", port=6379, decode_responses=True, password="123")


@app.route("/drivers/gps")
def drivers_gps():
    all_drivers = r.hgetall("driver:gps")

    result = []
    for driver_id, value in all_drivers.items():
        # value là JSON string, parse ra dict
        data = json.loads(value)
        data["lon"] = data.pop("lng")
        data["id"] = driver_id
        result.append(data)

    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
