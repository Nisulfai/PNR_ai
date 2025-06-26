from ultralytics import YOLO
import cv2
import os
from config import MODEL_PATH, BBOX_FOLDER

model = YOLO(MODEL_PATH)

def detect_yolo(image_path):
    result = model(image_path)
    jenis = []
    if len(result[0].boxes) > 0:
        for box in result[0].boxes:
            cls = int(box.cls[0])
            jenis.append(model.names[cls])
        draw_bbox(image_path, result)
    return len(result[0].boxes) > 0, ", ".join(set(jenis))

def draw_bbox(image_path, results):
    img = cv2.imread(image_path)
    for box in results[0].boxes:
        x1, y1, x2, y2 = map(int, box.xyxy[0])
        label = model.names[int(box.cls[0])]
        cv2.rectangle(img, (x1, y1), (x2, y2), (0,255,0), 2)
        cv2.putText(img, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0,255,0), 2)
    output_path = os.path.join(BBOX_FOLDER, os.path.basename(image_path))
    cv2.imwrite(output_path, img)

