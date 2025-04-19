from fastapi import APIRouter, File, UploadFile
from pydantic import BaseModel
import cv2
import numpy as np
from typing import List
import os
from fastapi.responses import JSONResponse
import json
from typing import Annotated
from ultralytics import YOLO


router = APIRouter()

# Load the YOLOv8 model
MODEL_PATH = "./yolov8x.pt"
model = YOLO(MODEL_PATH)

# Define the allowed class IDs to include in results
CLASS_ID = [1, 2, 3, 5, 7]  # e.g., 1: bicycle, 2: car, etc.

# Define structure for each detected object
class DetectedObject(BaseModel):
    class_object: str
    coordinates: List[float]
    confidence: float
    class_id: int
    classname: str

# Define overall detection response format
class DetectionResponse(BaseModel):
    image_shape: List[int]
    total: int
    objects: List[DetectedObject]

@router.post("/detect", response_model=DetectionResponse)
async def detect_objects(file: UploadFile = File(...)):
    # Read uploaded image content
    contents = await file.read()

    # Decode image from binary data
    nparr = np.frombuffer(contents, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    if img is None:
        return {"error": "Could not decode image."}

    # Perform YOLO inference
    results = model.predict(img)
    prediction = results[0]

    # Extract box coordinates, scores, and class IDs
    boxes = prediction.boxes.xyxy.cpu().numpy()
    scores = prediction.boxes.conf.cpu().numpy()
    class_ids = prediction.boxes.cls.cpu().numpy().astype(int)
    labels = [model.names[int(cls)] for cls in class_ids]

    objects = []
    for i in range(len(boxes)):
        class_id = int(class_ids[i])

        # Filter only desired classes
        if class_id in CLASS_ID:
            x1, y1, x2, y2 = map(float, boxes[i])
            obj = DetectedObject(
                class_object=labels[i],
                coordinates=[x1, y1, x2, y2],
                confidence=round(float(scores[i]), 2),
                class_id=class_id,
                classname=labels[i]
            )
            objects.append(obj)

    # Prepare and return response
    response = DetectionResponse(
        image_shape=list(img.shape[:2]),  # [height, width]
        total=len(objects),
        objects=objects
    )

    return response

if __name__ == "__main__":
    pass