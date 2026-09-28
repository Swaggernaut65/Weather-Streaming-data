import requests
import uuid
import time
import json
from datetime import datetime
from kafka import KafkaProducer


# ==========================================
# CONFIGURATION
# ==========================================

OPENWEATHER_API_KEY = "Insert API key here"

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "weather.current"

# Time between complete collection rounds
# 600 seconds = 10 minutes
DELAY_SECONDS = 60


# ==========================================
# CITIES
# ==========================================

LOCATIONS = {
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai": (13.0827, 80.2707),
    "Kolkata": (22.5726, 88.3639),
    "Hyderabad": (17.3850, 78.4867)
}


# ==========================================
# FETCH WEATHER FROM OPENWEATHER
# ==========================================

def get_weather(city, latitude, longitude):

    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?lat={latitude}"
        f"&lon={longitude}"
        f"&appid={OPENWEATHER_API_KEY}"
        "&units=metric"
    )

    try:

        response = requests.get(
            url,
            timeout=10
        )

        if response.status_code != 200:

            print(
                f"ERROR fetching {city}: "
                f"{response.status_code}"
            )

            print(response.text)

            return None

        data = response.json()

        # Rain may not exist in every response
        rainfall = data.get(
            "rain",
            {}
        ).get(
            "1h",
            0
        )

        # Create standardized record
        weather_record = {

            "event_id":
                f"OW-{uuid.uuid4().hex[:8].upper()}",

            "timestamp":
                datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),

            "location":
                city,

            "latitude":
                latitude,

            "longitude":
                longitude,

            "temperature_c":
                data["main"]["temp"],

            "feels_like_c":
                data["main"]["feels_like"],

            "humidity_pct":
                data["main"]["humidity"],

            "pressure_hpa":
                data["main"]["pressure"],

            "wind_speed_ms":
                data["wind"].get(
                    "speed",
                    0
                ),

            "wind_direction_deg":
                data["wind"].get(
                    "deg",
                    0
                ),

            "visibility_m":
                data.get(
                    "visibility",
                    0
                ),

            "rain_1h_mm":
                rainfall,

            "cloudiness_pct":
                data["clouds"]["all"],

            "weather_condition":
                data["weather"][0]["main"],

            "weather_description":
                data["weather"][0]["description"],

            "source":
                "OpenWeather"
        }

        return weather_record

    except requests.exceptions.RequestException as e:

        print(
            f"Request failed for {city}: {e}"
        )

        return None


# ==========================================
# CONNECT TO KAFKA
# ==========================================

print("\n==========================================")
print("       CONNECTING TO KAFKA")
print("==========================================")

try:

    producer = KafkaProducer(

        bootstrap_servers=KAFKA_SERVER,

        value_serializer=lambda value:
            json.dumps(value).encode("utf-8")
    )

    print(
        f"Connected to Kafka: {KAFKA_SERVER}"
    )

except Exception as e:

    print("Could not connect to Kafka.")

    print(e)

    exit()


# ==========================================
# START LIVE STREAM
# ==========================================

print("\n==========================================")
print("       LIVE WEATHER STREAM STARTED")
print("==========================================")

print(
    f"Kafka Topic : {KAFKA_TOPIC}"
)

print(
    f"Cities      : {len(LOCATIONS)}"
)

print(
    f"Interval    : {DELAY_SECONDS} seconds"
)

print("\nPress CTRL+C to stop the producer.\n")


round_number = 0


try:

    while True:

        round_number += 1

        print("\n==========================================")
        print(
            f"ROUND {round_number}"
        )
        print("==========================================")

        for city, coordinates in LOCATIONS.items():

            latitude, longitude = coordinates

            print(
                f"Fetching {city}...",
                end=" "
            )

            # Get live weather
            record = get_weather(
                city,
                latitude,
                longitude
            )

            if record:

                # Send directly to Kafka
                producer.send(
                    KAFKA_TOPIC,
                    value=record
                )

                print(
                    f"SUCCESS | "
                    f"{record['temperature_c']}°C | "
                    f"{record['humidity_pct']}% humidity | "
                    f"{record['weather_condition']}"
                )

            else:

                print("FAILED")

            # Small delay between API calls
            time.sleep(2)

        # Make sure all messages are sent
        producer.flush()

        print("\n------------------------------------------")

        print(
            f"Round {round_number} completed."
        )

        print(
            f"Waiting {DELAY_SECONDS} seconds "
            "for next update..."
        )

        print("------------------------------------------")

        time.sleep(DELAY_SECONDS)


except KeyboardInterrupt:

    print("\n\n==========================================")
    print("       STREAM STOPPED")
    print("==========================================")

    print(
        "Producer stopped by user."
    )


except Exception as e:

    print("\nProducer error:")
    print(e)


finally:

    producer.close()

    print("Kafka producer closed.")