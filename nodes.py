from opcua import Client, ua

# === OPC UA Server URL ===
opc_url = "opc.tcp://192.168.27.234:63840"

# === Connect Client ===
client = Client(opc_url)
client.connect()
print("✅ Connected to OPC UA Server")

# === Recursive Browser Function ===
def browse_node(node, level=0, max_depth=3):
    """
    Recursively browse OPC UA nodes up to a certain depth.
    """
    try:
        children = node.get_children()
        for child in children:
            try:
                node_id = child.nodeid.to_string()
                browse_name = child.get_browse_name().Name
                display_name = child.get_display_name().Text
                indent = "  " * level
                print(f"{indent}- NodeId: {node_id}, BrowseName: {browse_name}, DisplayName: {display_name}")

                # Try reading value (if readable)
                try:
                    val = child.get_value()
                    print(f"{indent}   🔎 Value: {val}")
                except:
                    pass

                # Recurse deeper if within max depth
                if level < max_depth:
                    browse_node(child, level + 1, max_depth)

            except Exception as e:
                print("⚠️ Error reading child:", e)
    except Exception as e:
        print("⚠️ Error browsing node:", e)

# === Start Browsing from Root ===
root = client.get_root_node()
print("🌳 Root Node:", root)

print("\n📂 Browsing Objects Node:")
objects = client.get_objects_node()
browse_node(objects, level=0, max_depth=2)

# === Disconnect ===
client.disconnect()
print("🔌 Disconnected")
