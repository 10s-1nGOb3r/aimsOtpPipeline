✈️ Airline OTP & Delay Analysis Pipeline
📌 Overview

This project is a Python-based ETL pipeline designed to calculate and analyze:

Departure OTP (On-Time Performance)
Arrival OTP
Delay categorization
Controllable vs Uncontrollable delays
Delay duration buckets
Station-level performance metrics
Monthly & Daily aggregated OTP statistics

The pipeline transforms raw operational flight data into analytical datasets ready for:

Power BI dashboards
MySQL database storage
Performance reporting
Root cause delay analysis

🏗️ Architecture
Raw CSV Files
    │
    ├── otp_pandas_try.csv        (Flight operational data)
    ├── delco_data_try.csv        (Delay code master reference)
    └── station_db.csv            (Station reference data)
        │
        ▼
Data Cleansing & Formatting
        │
        ▼
Delay Calculation Engine
        │
        ▼
Delay Category Assignment
        │
        ▼
Aggregation Layer
        │
        ▼
Output Tables (Daily, Monthly, Station-level)

📂 Project Structure
project_folder/
│
├── input/
│   ├── otp_pandas_try.csv
│   ├── delco_data_try.csv
│   └── station_db.csv
│
├── output/
│   └── (generated result tables)
│
└── otp_calculation.py

📊 Business Logic
1️⃣ Flight Validation

A flight is considered valid for OTP calculation when:

TYPE is J or G
ST = 0 (not cancelled)
FVal = "val"

2️⃣ Delay Definition
A flight is considered late when:
Total Delay Minutes > 15
Otherwise:
On Time

3️⃣ Delay Buckets
Range	Category
16–30 mins	00:16 - 00:30
31–59 mins	00:31 - 00:59
60–119 mins	01:00 - 01:59
120–239 mins	02:00 - 03:59
>240 mins	> 04:00
>
>
4️⃣ Delay Code Assignment Logic

The delay code assigned to a flight is based on:
The delay category with the maximum delay duration
Mapped against master delay code reference table

5️⃣ Delay Cause Categories

The pipeline calculates delay totals for:
STATION HANDLING
DAMAGE TO AIRCRAFT
TECHNICAL
SYSTEM
FLIGHT OPERATIONS & CREW
WEATHER
AIRPORT FACILITIES
MISCELLANEOUS

Additionally classified into:
CONTROLLABLE
UNCONTROLLABLE

📈 Output Tables Generated
Daily Level
OTP per Date
Arrival OTP per Date
Delay category totals
Delay percentages
Monthly Level
OTP per Month
Arrival OTP per Month
Delay composition analysis
Station performance breakdown
Station Level
OTP by Station
Delay distribution by duration bucket
ICAO and Class breakdown

⚙️ Requirements
Python 3.9+
pandas
numpy

Install dependencies:
pip install pandas numpy

▶️ How to Run
From project directory:
python otp_calculation.py
Output tables will be generated in:
/output

🧠 Performance Considerations
Handles time conversion using pd.to_timedelta
Vectorized operations using numpy.where
Uses groupby aggregation for performance summarization
Designed for integration with Power BI or SQL warehouse

🔐 Data Assumptions
Delay values stored in HH:MM format
Flight cancellation logic handled via ST column
Delay codes are consistent with delay master reference
Station codes match station reference table

🚀 Future Improvements
Refactor repeated logic into reusable functions
Add safe division to avoid zero-division errors
Modularize into production-ready architecture
Add logging layer
Convert to scheduled ETL job (Airflow / Cron)
Write results directly to MySQL instead of CSV

👨‍💻 Author
Operational Performance & Data Analytics Pipeline
Designed for airline OTP & delay root cause analysis.
