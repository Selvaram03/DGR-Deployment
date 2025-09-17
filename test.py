from opcua import Client

# === OPC UA Server URL ===
opc_url = "opc.tcp://192.168.27.234:63840"

# === Connect OPC UA ===
client = Client(opc_url)
client.connect()
print("✅ Connected to OPC UA Server")

# === Inverter Tags (INV1 → INV18) ===
inv_tags = [f"ns=2;s=I1_INV{i}_DATA1" for i in range(1, 19)]

# === Test Each Inverter Node ===
for i, tag in enumerate(inv_tags, start=1):
    try:
        node = client.get_node(tag)
        value = node.get_value()

        # Check if list/tuple → print all elements for inspection
        if isinstance(value, (list, tuple)):
            print(f"🔎 INV{i} ({tag}) → Full Value: {value}")
            if len(value) > 1:
                print(f"   ✅ Extracted Daily Generation: {value[1]}")
            else:
                print(f"   ⚠️ No index 1 found in value")
        else:
            print(f"🔎 INV{i} ({tag}) → Single Value: {value}")

    except Exception as e:
        print(f"❌ Error reading INV{i} ({tag}): {e}")

# === Disconnect ===
client.disconnect()
print("🔌 Disconnected from OPC UA")