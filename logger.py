import os
from opcua import Client
from pymongo import MongoClient
import pandas as pd
import time

# === Config from Environment Variables (Railway secrets) ===
opc_url = os.getenv("OPC_URL")     
mongo_url = os.getenv("MONGO_URI")

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

try:
    while True:
        timestamp = pd.Timestamp.now()
        record = {"timestamp": timestamp}

        # Get Irradiation directly
        try:
            irradiation_val = irradiation_node.get_value()
        except Exception:
            irradiation_val = None
        record["Irradiation"] = irradiation_val

        # Get Daily Generation values
        for i, (tag, node) in enumerate(inv_nodes.items(), start=1):
            try:
                value = node.get_value()
                # Only 2nd element (index 1) if array/list
                daily_gen = value[1] if isinstance(value, (list, tuple)) and len(value) > 1 else None
            except Exception:
                daily_gen = None

            record[f"Daily_Generation_INV{i}"] = daily_gen

        # Insert into MongoDB
        collection.insert_one(record)
        print("Inserted:", record)

        time.sleep(60)  # log every 5 minutes (adjust as needed)

except KeyboardInterrupt:
    print("⏹️ Stopped manually")

finally:
    opc_client.disconnect()
    mongo_client.close()
    print("🔌 Disconnected")