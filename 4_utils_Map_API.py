import requests
from config import API_KEY

def get_place_coords(nama, lat, lon):
    query = f"bendung {nama} dekat {lat},{lon}"
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": API_KEY}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            loc = results[0]["geometry"]["location"]
            return loc["lat"], loc["lng"], results[0].get("name", "")
    return None, None, ""
