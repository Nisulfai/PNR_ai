from PIL import Image
from io import BytesIO
import requests
from config import API_KEY

def get_street_view_image(lat, lon, heading=0):
    url = "https://maps.googleapis.com/maps/api/streetview"
    params = {
        "size": "640x640",
        "location": f"{lat},{lon}",
        "heading": heading,
        "fov": 90,
        "key": API_KEY
    }
    r = requests.get(url, params=params)
    return Image.open(BytesIO(r.content)) if r.status_code == 200 else None

def get_top_down_image(lat, lon, zoom=18):
    url = f"https://maps.googleapis.com/maps/api/staticmap?center={lat},{lon}&zoom={zoom}&size=640x640&maptype=satellite&key={API_KEY}"
    r = requests.get(url)
    return Image.open(BytesIO(r.content)) if r.ok else None

