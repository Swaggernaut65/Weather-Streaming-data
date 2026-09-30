"""
Live Weather Dashboard for Assignment 3.

Reads streaming weather data from MongoDB Atlas and displays:
1. Temperature trend over time, one line per city
2. Average temperature by city
3. Average humidity by city
4. Weather condition distribution

Dashboard auto-refreshes every 10 seconds.

Run:
    python dashboard.py

Then open:
    http://localhost:5000
"""

from flask import Flask, jsonify, render_template_string
from pymongo import MongoClient


# ==========================================
# CONFIGURATION
# ==========================================

MONGO_URI = "mongodb+srv://mongoadmin:mongoadmin118@newmongo1.eoazcz4.mongodb.net/?appName=Newmongo1"

DB_NAME = "weather_db"
COLLECTION_NAME = "weather_data"


# ==========================================
# FLASK APPLICATION
# ==========================================

app = Flask(__name__)

mongo_client = None
collection = None


# ==========================================
# GET TEMPERATURE TREND DATA
# ==========================================

def get_temperature_trend(limit_per_city=30):

    pipeline = [

        {
            "$sort": {
                "timestamp": 1
            }
        },

        {
            "$group": {

                "_id": "$location",

                "points": {
                    "$push": {
                        "timestamp": "$timestamp",
                        "temperature": "$temperature_c"
                    }
                }
            }
        }
    ]

    results = list(
        collection.aggregate(pipeline)
    )

    series = {}

    for result in results:

        series[result["_id"]] = (
            result["points"][-limit_per_city:]
        )

    return series


# ==========================================
# GET AVERAGE TEMPERATURE BY CITY
# ==========================================

def get_average_temperature():

    pipeline = [

        {
            "$group": {

                "_id": "$location",

                "average_temperature": {
                    "$avg": "$temperature_c"
                }
            }
        },

        {
            "$sort": {
                "average_temperature": -1
            }
        }
    ]

    results = list(
        collection.aggregate(pipeline)
    )

    return {
        result["_id"]:
        round(
            result["average_temperature"],
            2
        )

        for result in results
    }


# ==========================================
# GET AVERAGE HUMIDITY BY CITY
# ==========================================

def get_average_humidity():

    pipeline = [

        {
            "$group": {

                "_id": "$location",

                "average_humidity": {
                    "$avg": "$humidity_pct"
                }
            }
        },

        {
            "$sort": {
                "average_humidity": -1
            }
        }
    ]

    results = list(
        collection.aggregate(pipeline)
    )

    return {
        result["_id"]:
        round(
            result["average_humidity"],
            2
        )

        for result in results
    }


# ==========================================
# GET WEATHER CONDITION DISTRIBUTION
# ==========================================

def get_weather_distribution():

    pipeline = [

        {
            "$group": {

                "_id": "$weather_condition",

                "count": {
                    "$sum": 1
                }
            }
        },

        {
            "$sort": {
                "count": -1
            }
        }
    ]

    results = list(
        collection.aggregate(pipeline)
    )

    return {
        result["_id"]:
        result["count"]

        for result in results
    }


# ==========================================
# API SUMMARY
# ==========================================

@app.route("/api/summary")
def api_summary():

    return jsonify({

        "temperature_trend":
            get_temperature_trend(),

        "average_temperature":
            get_average_temperature(),

        "average_humidity":
            get_average_humidity(),

        "weather_distribution":
            get_weather_distribution(),

        "total_readings":
            collection.count_documents({})
    })


# ==========================================
# DASHBOARD HTML
# ==========================================

PAGE_TEMPLATE = """

<!DOCTYPE html>

<html>

<head>

    <meta charset="utf-8">

    <title>
        Live Weather Analytics Dashboard
    </title>

    <script src=
        "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js">
    </script>


    <style>

        body {

            font-family: Arial, sans-serif;

            background: #0f1117;

            color: #e6e6e6;

            margin: 0;

            padding: 24px;

        }


        h1 {

            font-size: 24px;

            margin-bottom: 4px;

        }


        .subtitle {

            color: #9aa0ab;

            margin-bottom: 24px;

            font-size: 13px;

        }


        .grid {

            display: grid;

            grid-template-columns: 1fr 1fr;

            gap: 20px;

        }


        .card {

            background: #171a21;

            border-radius: 10px;

            padding: 16px;

            box-shadow:
                0 1px 3px rgba(0,0,0,0.4);

        }


        .card.full {

            grid-column: 1 / -1;

        }


        .card h2 {

            font-size: 15px;

            margin: 0 0 12px 0;

            color: #d6d9de;

        }


        canvas {

            max-height: 320px;

        }


        .stat {

            font-size: 13px;

            color: #9aa0ab;

            margin-top: 24px;

        }

    </style>

</head>


<body>


    <h1>
        Real-Time Weather Analytics Dashboard
    </h1>


    <div class="subtitle">

        Live weather data from OpenWeather API
        through Kafka and MongoDB Atlas

    </div>


    <div class="grid">


        <!-- TEMPERATURE TREND -->

        <div class="card full">

            <h2>
                Temperature Trend Over Time
            </h2>

            <canvas id="temperatureChart">
            </canvas>

        </div>


        <!-- AVERAGE TEMPERATURE -->

        <div class="card">

            <h2>
                Average Temperature by City
            </h2>

            <canvas id="averageTemperatureChart">
            </canvas>

        </div>


        <!-- AVERAGE HUMIDITY -->

        <div class="card">

            <h2>
                Average Humidity by City
            </h2>

            <canvas id="humidityChart">
            </canvas>

        </div>


        <!-- WEATHER DISTRIBUTION -->

        <div class="card">

            <h2>
                Weather Condition Distribution
            </h2>

            <canvas id="weatherChart">
            </canvas>

        </div>


    </div>


    <div
        class="stat"
        id="statLine"
    >
        Loading...
    </div>


    <script>


        const palette = [

            "#4dabf7",
            "#ff8787",
            "#69db7c",
            "#ffd43b",
            "#b197fc",
            "#63e6be"

        ];


        // ==========================================
        // TEMPERATURE TREND CHART
        // ==========================================

        const temperatureChart =
            new Chart(

                document.getElementById(
                    "temperatureChart"
                ),

                {

                    type: "line",

                    data: {

                        datasets: []

                    },

                    options: {

                        responsive: true,

                        scales: {

                            x: {

                                type: "category",

                                ticks: {

                                    color:
                                        "#9aa0ab"

                                }

                            },

                            y: {

                                ticks: {

                                    color:
                                        "#9aa0ab"

                                },

                                title: {

                                    display: true,

                                    text:
                                        "Temperature (°C)",

                                    color:
                                        "#9aa0ab"

                                }

                            }

                        },

                        plugins: {

                            legend: {

                                labels: {

                                    color:
                                        "#e6e6e6"

                                }

                            }

                        }

                    }

                }

            );


        // ==========================================
        // AVERAGE TEMPERATURE CHART
        // ==========================================

        const averageTemperatureChart =
            new Chart(

                document.getElementById(
                    "averageTemperatureChart"
                ),

                {

                    type: "bar",

                    data: {

                        labels: [],

                        datasets: [

                            {

                                label:
                                    "Average Temperature",

                                data: [],

                                backgroundColor:
                                    "#4dabf7"

                            }

                        ]

                    },

                    options: {

                        responsive: true,

                        scales: {

                            x: {

                                ticks: {

                                    color:
                                        "#9aa0ab"

                                }

                            },

                            y: {

                                ticks: {

                                    color:
                                        "#9aa0ab"

                                },

                                title: {

                                    display: true,

                                    text:
                                        "Temperature (°C)",

                                    color:
                                        "#9aa0ab"

                                }

                            }

                        },

                        plugins: {

                            legend: {

                                display: false

                            }

                        }

                    }

                }

            );


        // ==========================================
        // HUMIDITY CHART
        // ==========================================

        const humidityChart =
            new Chart(

                document.getElementById(
                    "humidityChart"
                ),

                {

                    type: "bar",

                    data: {

                        labels: [],

                        datasets: [

                            {

                                label:
                                    "Average Humidity",

                                data: [],

                                backgroundColor:
                                    "#69db7c"

                            }

                        ]

                    },

                    options: {

                        responsive: true,

                        scales: {

                            x: {

                                ticks: {

                                    color:
                                        "#9aa0ab"

                                }

                            },

                            y: {

                                ticks: {

                                    color:
                                        "#9aa0ab"

                                },

                                title: {

                                    display: true,

                                    text:
                                        "Humidity (%)",

                                    color:
                                        "#9aa0ab"

                                }

                            }

                        },

                        plugins: {

                            legend: {

                                display: false

                            }

                        }

                    }

                }

            );


        // ==========================================
        // WEATHER DISTRIBUTION
        // ==========================================

        const weatherChart =
            new Chart(

                document.getElementById(
                    "weatherChart"
                ),

                {

                    type: "doughnut",

                    data: {

                        labels: [],

                        datasets: [

                            {

                                data: [],

                                backgroundColor:
                                    palette

                            }

                        ]

                    },

                    options: {

                        responsive: true,

                        plugins: {

                            legend: {

                                position: "bottom",

                                labels: {

                                    color:
                                        "#e6e6e6"

                                }

                            }

                        }

                    }

                }

            );


        // ==========================================
        // REFRESH DASHBOARD
        // ==========================================

        async function refresh() {

            try {

                const response =
                    await fetch(
                        "/api/summary"
                    );

                const data =
                    await response.json();


                // ======================================
                // TEMPERATURE TREND
                // ======================================

                const cities =
                    Object.keys(
                        data.temperature_trend
                    );


                const allTimestamps =
                    [
                        ...new Set(

                            cities.flatMap(

                                city =>

                                    data
                                    .temperature_trend
                                    [city]
                                    .map(
                                        point =>
                                            point.timestamp
                                    )

                            )

                        )

                    ].sort();


                temperatureChart
                    .data
                    .labels =

                    allTimestamps.map(

                        timestamp =>

                            timestamp.slice(
                                11,
                                19
                            )

                    );


                temperatureChart
                    .data
                    .datasets =

                    cities.map(

                        (city, index) => {

                            const byTimestamp =
                                Object.fromEntries(

                                    data
                                    .temperature_trend
                                    [city]
                                    .map(

                                        point => [

                                            point.timestamp,

                                            point.temperature

                                        ]

                                    )

                                );


                            return {

                                label: city,

                                data:

                                    allTimestamps.map(

                                        timestamp =>

                                            byTimestamp[
                                                timestamp
                                            ] ?? null

                                    ),

                                borderColor:
                                    palette[
                                        index %
                                        palette.length
                                    ],

                                backgroundColor:
                                    palette[
                                        index %
                                        palette.length
                                    ],

                                spanGaps: true,

                                tension: 0.25

                            };

                        }

                    );


                temperatureChart.update();


                // ======================================
                // AVERAGE TEMPERATURE
                // ======================================

                averageTemperatureChart
                    .data
                    .labels =

                    Object.keys(
                        data.average_temperature
                    );


                averageTemperatureChart
                    .data
                    .datasets[0]
                    .data =

                    Object.values(
                        data.average_temperature
                    );


                averageTemperatureChart.update();


                // ======================================
                // AVERAGE HUMIDITY
                // ======================================

                humidityChart
                    .data
                    .labels =

                    Object.keys(
                        data.average_humidity
                    );


                humidityChart
                    .data
                    .datasets[0]
                    .data =

                    Object.values(
                        data.average_humidity
                    );


                humidityChart.update();


                // ======================================
                // WEATHER DISTRIBUTION
                // ======================================

                weatherChart
                    .data
                    .labels =

                    Object.keys(
                        data.weather_distribution
                    );


                weatherChart
                    .data
                    .datasets[0]
                    .data =

                    Object.values(
                        data.weather_distribution
                    );


                weatherChart.update();


                // ======================================
                // STATUS
                // ======================================

                document
                    .getElementById(
                        "statLine"
                    )
                    .textContent =

                    `Total readings stored: ${
                        data.total_readings
                    } | Last refreshed: ${
                        new Date()
                        .toLocaleTimeString()
                    }`;

            }

            catch (error) {

                console.error(
                    "Dashboard refresh error:",
                    error
                );

            }

        }


        // Initial load

        refresh();


        // Refresh every 10 seconds

        setInterval(
            refresh,
            10000
        );


    </script>


</body>

</html>
"""


# ==========================================
# HOME PAGE
# ==========================================

@app.route("/")
def index():

    return render_template_string(
        PAGE_TEMPLATE
    )


# ==========================================
# START DASHBOARD
# ==========================================

def main():

    global mongo_client
    global collection


    print("\n==========================================")
    print("       WEATHER DASHBOARD")
    print("==========================================")


    try:

        mongo_client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=15000
        )


        # Test MongoDB connection

        mongo_client.admin.command(
            "ping"
        )


        collection = (
            mongo_client
            [DB_NAME]
            [COLLECTION_NAME]
        )


        print(
            "Connected to MongoDB Atlas!"
        )


        print(
            f"Database   : {DB_NAME}"
        )


        print(
            f"Collection : {COLLECTION_NAME}"
        )


        print(
            "Dashboard  : "
            "http://localhost:5000"
        )


        print(
            "\nPress CTRL+C to stop."
        )


        app.run(
            host="0.0.0.0",
            port=5000,
            debug=False
        )


    except Exception as e:

        print(
            "\nCould not connect to MongoDB Atlas."
        )

        print(e)


    finally:

        if mongo_client:

            mongo_client.close()


# ==========================================
# RUN APPLICATION
# ==========================================

if __name__ == "__main__":

    main()
