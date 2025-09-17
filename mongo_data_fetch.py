import pandas as pd
from pymongo import MongoClient

# === MongoDB Connection ===
mongo_url = "mongodb+srv://selvaram58_db_user:cFhijYBal60CGpAi@dgr-demo.dh1kxon.mongodb.net/"
mongo_client = MongoClient(mongo_url)

db = mongo_client["scada_db"]            
collection = db["opcua_data"]

# === Fetch Data from MongoDB ===
cursor = collection.find({}, {"_id": 0})   # exclude the MongoDB _id field
data = list(cursor)

# === Convert to Pandas DataFrame ===
df = pd.DataFrame(data)

print("✅ Data Loaded from MongoDB")
print(df.head())

# === Basic Analytics ===
print("\n📊 Summary Statistics:")
print(df.describe())

# Example: Daily Irradiation Trend
if "timestamp" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)

    # Resample hourly or daily
    daily_irradiation = df["Irradiation"].resample("D").mean()
    print("\n🌞 Daily Average Irradiation:")
    print(daily_irradiation)

    # Example: Sum of all inverter generations per day
    inv_cols = [col for col in df.columns if "Daily_Generation" in col]
    df["Total_Generation"] = df[inv_cols].sum(axis=1)
    daily_gen = df["Total_Generation"].resample("D").sum()
    print("\n⚡ Daily Total Generation:")
    print(daily_gen)

df.to_excel("scada_data_analysis.xlsx")