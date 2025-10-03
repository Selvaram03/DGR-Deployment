from opcua import Client, ua
import pandas as pd

# === OPC UA Server URL ===
opc_url = "OPC LINK"

# === Connect Client ===
client = Client(opc_url)
client.connect()
print("✅ Connected to OPC UA Server")

# === List to store node data ===
nodes_data = []

# === Recursive Browser Function ===
def browse_node(node, level=0, max_depth=3):
    """
    Recursively browse OPC UA nodes up to a certain depth and fetch values.
    """
    try:
        children = node.get_children()
        for child in children:
            try:
                node_id = child.nodeid.to_string()
                browse_name = child.get_browse_name().Name
                display_name = child.get_display_name().Text

                # Try reading value (if readable)
                try:
                    val = child.get_value()
                except:
                    val = None

                # Store data in the list
                nodes_data.append({
                    "NodeId": node_id,
                    "BrowseName": browse_name,
                    "DisplayName": display_name,
                    "Value": val
                })

                # Recurse deeper if within max depth
                if level < max_depth:
                    browse_node(child, level + 1, max_depth=2)

            except Exception as e:
                print("⚠️ Error reading child:", e)
    except Exception as e:
        print("⚠️ Error browsing node:", e)

# === Start Browsing from Objects Node ===
objects = client.get_objects_node()
browse_node(objects, level=0, max_depth=2)

# === Disconnect Client ===
client.disconnect()
print("🔌 Disconnected")

# === Save to Excel ===
df = pd.DataFrame(nodes_data)
excel_file = "Umri_tags.xlsx"
df.to_excel(excel_file, index=False)
print(f"📊 Data saved to {excel_file}")
