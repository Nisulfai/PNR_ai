import pytesseract
import cv2
import numpy as np
import re
import base64
from io import BytesIO
import requests
from config import API_KEY

pytesseract.pytesseract.tesseract_cmd = r"C:\\Program Files\\Tesseract-OCR\\tesseract.exe"

def detect_text(image):
    text = detect_text_google_vision(image)
    if not text:
        img_cv = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
        text = pytesseract.image_to_string(img_cv)
    return clean_text(text)

def detect_text_google_vision(image):
    img_byte_arr = BytesIO()
    image.save(img_byte_arr, format='JPEG')
    encoded = base64.b64encode(img_byte_arr.getvalue()).decode()
    body = {
        "requests": [{
            "image": {"content": encoded},
            "features": [{"type": "TEXT_DETECTION"}]
        }]
    }
    url = f"https://vision.googleapis.com/v1/images:annotate?key={API_KEY}"
    res = requests.post(url, json=body)
    try:
        return res.json()['responses'][0]['textAnnotations'][0]['description']
    except:
        return ""

def clean_text(raw_text):
    return "\n".join([
        re.sub(r"[^a-zA-Z0-9\s-]", "", line).strip()
        for line in raw_text.split("\n")
        if len(line.strip()) > 3 and "google" not in line.lower()
    ])