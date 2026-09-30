import json
from kafka import KafkaConsumer
from pymongo import MongoClient


# ==========================================
# CONFIGURATION
# ==========================================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "weather.current"

MONGO_URI = "mongodb+srv://mongoadmin:mongoadmin118@newmongo1.eoazcz4.mongodb.net/?appName=Newmongo1"

DATABASE_NAME = "weather_db"
COLLECTION_NAME = "weather_data"


# ==========================================
# CONNECT TO MONGODB ATLAS
# ==========================================

print("\n==========================================")
print("       CONNECTING TO MONGODB ATLAS")
print("==========================================")

try:

    mongo_client = MongoClient(MONGO_URI)

    # Test connection
    mongo_client.admin.command("ping")

    print("Connected to MongoDB Atlas successfully!")

    db = mongo_client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

except Exception as e:

    print("Could not connect to MongoDB Atlas.")
    print(e)
    exit()


# ==========================================
# CONNECT TO KAFKA
# ==========================================

print("\n==========================================")
print("          CONNECTING TO KAFKA")
print("==========================================")

try:

    consumer = KafkaConsumer(

        KAFKA_TOPIC,

        bootstrap_servers=KAFKA_SERVER,

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        group_id="weather-mongodb-consumer",

        value_deserializer=lambda value:
            json.loads(value.decode("utf-8"))
    )

    print(
        f"Connected to Kafka: {KAFKA_SERVER}"
    )

    print(
        f"Topic: {KAFKA_TOPIC}"
    )

except Exception as e:

    print("Could not connect to Kafka.")
    print(e)

    mongo_client.close()

    exit()


# ==========================================
# CONSUME KAFKA DATA
# ==========================================

print("\n==========================================")
print("       KAFKA → MONGODB STREAM STARTED")
print("==========================================")

print("Waiting for weather data...\n")


try:

    for message in consumer:

        weather_data = message.value

        # Insert Kafka message into MongoDB
        result = collection.insert_one(
            weather_data
        )

        print(
            f"Inserted | "
            f"{weather_data.get('location')} | "
            f"{weather_data.get('temperature_c')}°C | "
            f"{weather_data.get('humidity_pct')}% humidity | "
            f"Mongo ID: {result.inserted_id}"
        )


except KeyboardInterrupt:

    print("\n\n==========================================")
    print("       CONSUMER STOPPED")
    print("==========================================")


except Exception as e:

    print("\nConsumer error:")
    print(e)


finally:

    consumer.close()
    mongo_client.close()

    print("Kafka consumer closed.")
    print("MongoDB connection closed.")
