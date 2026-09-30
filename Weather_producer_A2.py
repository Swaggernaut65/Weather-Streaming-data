import requests
import csv
import uuid
import time
import os
import json
from datetime import datetime
from kafka import KafkaProducer

# ==========================================
# CONFIGURATION
# ==========================================

OPENWEATHER_API_KEY = "OpenWeathers API key has been used here sir!"

OUTPUT_FILE = "weather_sample_dataset.csv"

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "weather.current"

# Number of rounds
ROUNDS = 2

# Number of seconds between rounds
DELAY_SECONDS = 60

# Cities
LOCATIONS = {
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai": (13.0827, 80.2707),
    "Kolkata": (22.5726, 88.3639),
    "Hyderabad": (17.3850, 78.4867)
}

# ==========================================
# CSV COLUMNS
# ==========================================

FIELDS = [
    "event_id",
    "timestamp",
    "location",
    "latitude",
    "longitude",
    "temperature_c",
    "feels_like_c",
    "humidity_pct",
    "pressure_hpa",
    "wind_speed_ms",
    "wind_direction_deg",
    "visibility_m",
    "rain_1h_mm",
    "cloudiness_pct",
    "weather_condition",
    "weather_description",
    "source"
]

# ==========================================
# DELETE OLD DATASET
# ==========================================

if os.path.exists(OUTPUT_FILE):

    os.remove(OUTPUT_FILE)

    print(f"Deleted existing file: {OUTPUT_FILE}")

else:

    print("No existing dataset found.")


# ==========================================
# FETCH WEATHER DATA
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
                f"Error fetching {city}: "
                f"{response.status_code}"
            )

            print(response.text)

            return None

        data = response.json()

        # Rain may not exist in API response
        rainfall = data.get(
            "rain",
            {}
        ).get(
            "1h",
            0
        )

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
# CREATE DATASET
# ==========================================

all_records = []

print("\n==========================================")
print("     OPENWEATHER DATASET GENERATOR")
print("==========================================")

for round_number in range(
    1,
    ROUNDS + 1
):

    print(
        f"\nROUND {round_number}/{ROUNDS}"
    )

    print("------------------------------------------")

    for city, coordinates in LOCATIONS.items():

        latitude, longitude = coordinates

        print(
            f"Fetching {city}...",
            end=" "
        )

        record = get_weather(
            city,
            latitude,
            longitude
        )

        if record:

            all_records.append(record)

            print("SUCCESS")

        else:

            print("FAILED")

        # Prevent rapid API requests
        time.sleep(1)

    print(
        f"Records collected: "
        f"{len(all_records)}"
    )

    # Wait before next round
    if round_number < ROUNDS:

        print(
            f"Waiting {DELAY_SECONDS} seconds..."
        )

        time.sleep(DELAY_SECONDS)


# ==========================================
# SAVE DATASET
# ==========================================

with open(
    OUTPUT_FILE,
    "w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.DictWriter(
        file,
        fieldnames=FIELDS
    )

    writer.writeheader()

    writer.writerows(all_records)


print("\n==========================================")
print("       DATASET CREATED")
print("==========================================")

print(
    f"File    : {OUTPUT_FILE}"
)

print(
    f"Records : {len(all_records)}"
)


# ==========================================
# CREATE KAFKA PRODUCER
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
# PRODUCE CSV DATA TO KAFKA
# ==========================================

print("\n==========================================")
print("       STARTING KAFKA PRODUCER")
print("==========================================")

try:

    with open(
        OUTPUT_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        reader = csv.DictReader(file)

        count = 0

        for row in reader:

            # Convert numeric CSV values
            row["latitude"] = float(
                row["latitude"]
            )

            row["longitude"] = float(
                row["longitude"]
            )

            row["temperature_c"] = float(
                row["temperature_c"]
            )

            row["feels_like_c"] = float(
                row["feels_like_c"]
            )

            row["humidity_pct"] = int(
                row["humidity_pct"]
            )

            row["pressure_hpa"] = int(
                row["pressure_hpa"]
            )

            row["wind_speed_ms"] = float(
                row["wind_speed_ms"]
            )

            row["wind_direction_deg"] = int(
                row["wind_direction_deg"]
            )

            row["visibility_m"] = int(
                row["visibility_m"]
            )

            row["rain_1h_mm"] = float(
                row["rain_1h_mm"]
            )

            row["cloudiness_pct"] = int(
                row["cloudiness_pct"]
            )

            # Send record to Kafka
            producer.send(
                KAFKA_TOPIC,
                value=row
            )

            count += 1

            print(
                f"Sent record {count}: "
                f"{row['location']} | "
                f"{row['temperature_c']}°C"
            )

            # Small delay to simulate streaming
            time.sleep(1)

    # Make sure all messages are sent
    producer.flush()

    print("\n==========================================")
    print("       KAFKA STREAM COMPLETE")
    print("==========================================")

    print(
        f"Total records sent: {count}"
    )

    print(
        f"Kafka topic: {KAFKA_TOPIC}"
    )

except Exception as e:

    print(
        f"Error producing Kafka data: {e}"
    )

finally:

    producer.close()
