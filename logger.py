import os
from opcua import Client
from pymongo import MongoClient
import pandas as pd
import time
import pytz  # for timezone conversion

# === Config from Environment Variables (Railway secrets) ===
opc_url_key = "opc.tcp://122.185.135.131:63840"
mongo_url_key = "mongodb+srv://selvaram58_db_user:cFhijYBal60CGpAi@dgr-demo.dh1kxon.mongodb.net/"

opc_url = opc_url_key
mongo_url = mongo_url_key

# === Connect OPC UA ===
opc_client = Client(opc_url)
opc_client.connect()
print("✅ Connected to OPC UA Server")

# === Connect MongoDB Atlas ===
mongo_client = MongoClient(mongo_url)
db = mongo_client["scada_db"]            # database
collection = db["opcua_data"]            # collection
print("✅ Connected to MongoDB Atlas")

# === Tags ===
irradiation_tag = "ns=2;s=I1_WMS_GTI_CUMM_IRRADIATION"
inv_tags = [f"ns=2;s=I1_INV{i}_DATA1" for i in range(1, 19)]

# Map Node objects
irradiation_node = opc_client.get_node(irradiation_tag)
inv_nodes = {tag: opc_client.get_node(tag) for tag in inv_tags}

# IST timezone
IST = pytz.timezone("Asia/Kolkata")

try:
    while True:
        # --- UTC timestamp ---
        timestamp = pd.Timestamp.now(tz='Asia/Kolkata')
        time_string_hh_mm = timestamp.strftime("%H:%M")
        record = {"timestamp": time_string_hh_mm}

        # --- Irradiation ---
        try:
            irradiation_val = irradiation_node.get_value()
        except Exception:
            irradiation_val = None
        record["Irradiation"] = irradiation_val

        # --- Daily Generation per inverter ---
        for i, (tag, node) in enumerate(inv_nodes.items(), start=1):
            try:
                value = node.get_value()
                daily_gen = value[1] if isinstance(value, (list, tuple)) and len(value) > 1 else None
            except Exception:
                daily_gen = None
            record[f"Daily_Generation_INV{i}"] = daily_gen

        # --- Insert into MongoDB ---
        collection.insert_one(record)
        print("Inserted:", record)

        time.sleep(60)  # log every minute

except KeyboardInterrupt:
    print("⏹️ Stopped manually")

finally:
    opc_client.disconnect()
    mongo_client.close()
    print("🔌 Disconnected")

