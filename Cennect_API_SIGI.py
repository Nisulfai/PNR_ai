import requests
import time
import pandas as pd
import json
import os

# Pastikan folder output ada
os.makedirs("output_sigi", exist_ok=True)

# Daftar URL REST API SIGI per infrastruktur
urls = {
    "Pengaman_Pantai": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2021_pengaman_pantai/FeatureServer/0/query?",
    "Aset_Tanah_PUPR": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_siman_tanah/FeatureServer/0/query?",
    "Bendung": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bendung_fulltable/FeatureServer/0/query?",
    "Bendungan_Konstruksi": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2022_bendungan_konstruksi/FeatureServer/0/query?",
    "Bendungan_Operasional": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2022_bendungan_operasional/FeatureServer/0/query?",
    "Bendungan_Rencana": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2022_bendungan_rencana/FeatureServer/0/query?",
    "BPB": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bpb_fulltable_v2/FeatureServer/0/query?",
    "Daerah_Irigasi": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_irigasi_fulltable/MapServer/0/query?",
    "Danau": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_danau_fulltable/FeatureServer/0/query?",
    "Efektivitas_Drainase": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bm_efektivitas_drainase/MapServer/0/query?",
    "Embung": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2021_embung/FeatureServer/0/query?",
    "Gerbang_Tol": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bpjt_gerbangtol_v3/FeatureServer/0/query?",
    "Intake_Sungai": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/intake_fulltable/FeatureServer/0/query?",
    "IPA_SPAM": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2022_spam/FeatureServer/0/query?",
    "IPAL": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_ipal_fulltable_v2/FeatureServer/0/query?",
    "IPLT": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2021_iplt_v2/FeatureServer/0/query?",
    "Jalan_Daerah": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/jalan_daerah_v2/MapServer/0/query?",
    "Jalan_Nasional": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bm_jalan_nasional_iri_v3/FeatureServer/0/query?",
    "Jalan_Perbatasan_Kalimantan": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bm_jalan_perbatasan_kalimantan/FeatureServer/0/query?",
    "Jalan_Tol_Konstruksi": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bpjt_tol_konstruksi_v3/FeatureServer/0/query?",
    "Jalan_Tol_Operasi": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bpjt_tol_operasi_v3/FeatureServer/0/query?",
    "Jembatan_Gantung": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/jembatan_gantung/FeatureServer/0/query?",
    "Jembatan_Khusus": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/jembatan_khusus/FeatureServer/0/query?",
    "Jembatan_Nasional": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bm_jembatan_nasional_v3/FeatureServer/0/query?",
    "Jembatan_Tol": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bpjt_tol_jembatan/FeatureServer/0/query?",
    "Ketersediaan_Air": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2021_ketersediaan_air/FeatureServer/0/query?",
    "Lereng": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bm_lereng/FeatureServer/0/query?",
    "Madrasah": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ck_madrasah_v2/FeatureServer/0/query?",
    "Mata_Air": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_mataair_fulltable/FeatureServer/0/query?",
    "Neraca_Air": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2021_neraca_air/FeatureServer/0/query?",
    "Overpass_Tol": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bpjt_tol_overpass/FeatureServer/0/query?",
    "PAH_ABSAH": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/pah_absah_fulltable/FeatureServer/0/query?",
    "Pasar": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ck_pasar_v2/FeatureServer/0/query?",
    "Pengendali_Sedimen": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_pengendali_sedimen/FeatureServer/0/query?",
    "PKP": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_pkp_v2/FeatureServer/0/query?",
    "PLBN": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/plbn/FeatureServer/0/query?",
    "Pompa_Banjir": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_pompabanjir_fulltable/FeatureServer/0/query?",
    "Pos_Curah_Hujan": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_poscurahhujan_fulltable/FeatureServer/0/query?",
    "Pos_Duga_Air": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_posdugaair_fulltable/FeatureServer/0/query?",
    "Pos_Klimatologi": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_posklimatologi_fulltable/FeatureServer/0/query?",
    "PTKIN": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ck_ptkin_v2/FeatureServer/0/query?",
    "PTN": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ck_ptn_v2/FeatureServer/0/query?",
    "Rest_Area": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/restarea/FeatureServer/0/query?",
    "Rumah_Sakit": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/rumahsakit_fulltable/FeatureServer/0/query?",
    "Sarana_Olahraga": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/saranaolahraga_fulltable_v2/FeatureServer/0/query?",
    "Sekolah": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/sekolah/FeatureServer/0/query?",
    "Simpang_Susun_Tol": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bpjt_tol_simpangsusun/FeatureServer/0/query?",
    "Situ": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2021_situ/FeatureServer/0/query?",
    "Sumur": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/igt_2021_airtanah_sumurbor/FeatureServer/0/query?",
    "TPA": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/tpa_v2/FeatureServer/0/query?",
    "Underpass_Tol": "https://sigi.pu.go.id/portalpupr/rest/services/sigi_postgis/ast_bpjt_tol_underpass/FeatureServer/0/query?"
}

# Parameter umum
PARAMS = {
    "where": "1=1",
    "outFields": "*",
    "f": "geojson",
    "returnGeometry": "true",
    "resultRecordCount": 2000
}

# Loop per infrastruktur
for name, BASE_URL in urls.items():
    print(f"\n🔄 Memproses data: {name}")
    all_records = []
    all_features = []
    offset = 0
    batch = 0

    while True:
        print(f"  ↳ Batch ke-{batch+1}, offset {offset}")
        PARAMS["resultOffset"] = offset

        try:
            response = requests.get(BASE_URL, params=PARAMS)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"  ❌ Gagal mengambil data pada offset {offset}: {e}")
            break

        features = data.get("features", [])
        if not features:
            print("  ✅ Tidak ada data lagi.")
            break

        for f in features:
            props = f.get("properties", {})
            geom = f.get("geometry", {})

            # Tambahkan ke CSV & GeoJSON
            all_records.append(props)
            all_features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": props
            })

        print(f"  ✔ Dapat {len(features)} record, total sejauh ini: {len(all_records)}")

        offset += PARAMS["resultRecordCount"]
        batch += 1
        time.sleep(1)

    # Simpan GeoJSON
    geojson_output = {
        "type": "FeatureCollection",
        "features": all_features
    }
    geojson_file = f"output_sigi/{name}_sigi.geojson"
    with open(geojson_file, "w", encoding="utf-8") as f:
        json.dump(geojson_output, f, ensure_ascii=False, indent=2)

    # Simpan CSV
    df = pd.DataFrame(all_records)
    csv_file = f"output_sigi/{name}_sigi.csv"
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")

    print(f"✅ Selesai simpan: {csv_file} & {geojson_file} (Total {len(df)} baris)")
