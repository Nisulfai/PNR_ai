import json
import pandas as pd
import os
import glob
from shapely.geometry import shape
from haversine import haversine
from utils.maps_api import get_place_coords
from utils.image_handler import get_street_view_image, get_top_down_image
from utils.ocr import detect_text
from utils.yolo import detect_yolo
from config import *

os.makedirs(GOOGLE_IMG_FOLDER, exist_ok=True)
os.makedirs(TOPDOWN_IMG_FOLDER, exist_ok=True)
os.makedirs(BBOX_FOLDER, exist_ok=True)
os.makedirs(BATCH_FOLDER, exist_ok=True)

def proses_geojson_batch(geojson_path, start_index=0, batch_size=5000):
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson = json.load(f)
    features = geojson['features']
    end_index = min(start_index + batch_size, len(features))
    hasil = []
    for i, feat in enumerate(features[start_index:end_index], start=start_index+1):
        try:
            p = feat['properties']
            lat, lon = float(p['latitude']), float(p['longitude'])
            nama = p['nama_infrastruktur'].strip().lower()
            cocok, all_text, jenis_detected = False, "", ""
            print(f"[{i}] {nama}")
            g_lat, g_lon, nama_google = get_place_coords(nama, lat, lon)
            jarak = haversine((lat, lon), (g_lat, g_lon))*1000 if g_lat else ""
            dekat = "Yes" if g_lat and jarak < 300 else ("No Data" if not g_lat else "No")

            top_img = get_top_down_image(lat, lon)
            if top_img:
                safe = re.sub(r"[\\/*?:\"<>|]", "_", nama)
                top_path = f"{TOPDOWN_IMG_FOLDER}/{safe}_{i}_top.jpg"
                top_img.save(top_path)
                det, jenis = detect_yolo(top_path)
                if det:
                    jenis_detected = jenis

            for head in [0, 90, 180, 270]:
                img = get_street_view_image(lat, lon, head)
                if img:
                    img.save(f"{GOOGLE_IMG_FOLDER}/{lat}_{lon}_h{head}.jpg")
                    text = detect_text(img)
                    all_text += text + "\n"
                    if any(nama in line.lower() for line in text.split("\n")):
                        cocok = True
                        break

            ver_final = "Cocok" if (cocok or dekat == "Yes") else "Tidak Cocok"
            hasil.append({
                'nama_infrastruktur': p['nama_infrastruktur'],
                'latitude': lat, 'longitude': lon,
                'google_lat': g_lat, 'google_lon': g_lon,
                'bendung_ditemukan': nama_google,
                'selisih_jarak_m': jarak,
                'Bendung_Terdeteksi_YOLO': "Yes" if jenis_detected else "No",
                'Jenis_Terdeteksi_YOLO': jenis_detected,
                'Koordinat Dekat (<300m)': dekat,
                'Verifikasi Nama': "Cocok" if cocok else "Tidak Cocok",
                'Verifikasi Final': ver_final,
                'Nama yang Ditemukan Sekitar': all_text.strip(),
                'jenis_infrastruktur': "Bendung"
                'Kab/kota': "Bendung"#nanti diganti 
                'Provinsi': "Bendung"#nanti diganti dengan kode yang lain 
            })
        except Exception as e:
            print(f"❌ Error: {e}")
            continue
    df = pd.DataFrame(hasil)
    df.to_csv(f"{BATCH_FOLDER}/Bendung_{start_index}_{end_index}.csv", index=False)


def gabungkan_batch(folder, output_final):
    files = glob.glob(os.path.join(folder, "*.csv"))
    df_all = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    df_all.to_csv(output_final, index=False)


if __name__ == "__main__":
    geojson_input = r"D:/pnr_nitip/bendung_sigi.geojson"
    batch_size = 5000
    with open(geojson_input, 'r', encoding='utf-8') as f:
        total = len(json.load(f)['features'])
    for start in range(0, total, batch_size):
        proses_geojson_batch(geojson_input, start, batch_size)
    gabungkan_batch(BATCH_FOLDER, r"D:/pnr_nitip/Bendung_verified_1.csv")