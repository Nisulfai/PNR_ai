#Narik INFRA DARI SIGI 
import requests
import time
import pandas as pd
import json  # ← Anda lupa import json

# URL endpoint ArcGIS REST API
BASE_URL ="https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bm_jembatan_gantung_v3/MapServer/0/query?"#JembatanGantung
# Parameter tetap
PARAMS = {
    "where": "1=1",
    "outFields": "*",
    "f": "geojson",
    "returnGeometry": "true",  # ← Ubah jadi True agar GeoJSON bisa dibuat
    "resultRecordCount": 2000
}

# Inisialisasi
all_records = []
all_features = []  # ← Diperlukan untuk GeoJSON
offset = 0
batch = 0

while True:
    print(f"Request batch ke-{batch+1} dengan offset {offset}...")
    PARAMS["resultOffset"] = offset

    try:
        response = requests.get(BASE_URL, params=PARAMS)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Gagal mengambil data di offset {offset}: {e}")
        break

    features = data.get("features", [])
    if not features:
        print("Tidak ada data lagi.")
        break

    for f in features:
        props = f.get("properties", {})
        geom = f.get("geometry", {})

        # Simpan record ke CSV
        all_records.append(props)

        # Simpan fitur ke GeoJSON
        feature = {
            "type": "Feature",
            "geometry": geom,
            "properties": props
        }
        all_features.append(feature)

    print(f"✔ Dapat {len(features)} record, total sejauh ini: {len(all_records)}")

    offset += PARAMS["resultRecordCount"]
    batch += 1
    time.sleep(1)

# Simpan ke file GeoJSON
geojson_output = {
    "type": "FeatureCollection",
    "features": all_features
}
with open("JembatanGantung_sigi.geojson", "w", encoding="utf-8") as f:
    json.dump(geojson_output, f, ensure_ascii=False, indent=2)

# Simpan ke file CSV
df = pd.DataFrame(all_records)
df.to_csv("JembatanGantung.csv", index=False, encoding="utf-8-sig")

print(f"\n✅ Selesai. Total {len(df)} baris disimpan ke 'JembatanGantung_sigi.csv' dan 'JembatanGantung_sigi.geojson'.")
