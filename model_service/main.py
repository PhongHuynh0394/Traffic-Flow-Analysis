from fastapi import FastAPI, File, UploadFile
from pydantic import BaseModel
import cv2
import numpy as np
from typing import List
import uvicorn
import os
from fastapi.responses import JSONResponse
import json
from typing import Annotated
# import torch
# from ultralytics import YOLO
# from ultralytics.nn.modules import Conv
# from ultralytics.nn.tasks import DetectionModel
# from torch.nn.modules.container import Sequential

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello world"}

@app.post("/upload-image/")
async def upload_image(file: Annotated[bytes, File()]):
    #  This endpoint accepts any uploaded image but always returns mock data
    mock_file_path = os.path.join(os.path.dirname(__file__), "mock_data.json")
    with open(mock_file_path, "r") as f:
        mock_data = json.load(f)
    return JSONResponse(content=mock_data)

# class DetectionResult(BaseModel):
#     boxes: List[List[float]]
#     labels: List[str]
#     scores: List[float]

# #download the yolov8n.pt file from the given url
# # https://huggingface.co/Ultralytics/YOLOv8/tree/main

# # Load the YOLOv8 model
# MODEL = "./model_service/yolov8x.pt"

# # Cách 1: Tắt hoàn toàn chế độ weights_only
# os.environ['TORCH_FORCE_WEIGHTS_ONLY'] = '0'

# # Cách 2: Thêm tất cả class cần thiết vào safe globals
# torch.serialization.add_safe_globals([DetectionModel, Conv, Sequential])

# # Cách 3: Ghi đè hàm torch.load tạm thời
# original_load = torch.load
# torch.load = lambda *args, **kwargs: original_load(*args, **{**kwargs, 'weights_only': False})

# try:
#     # Load model
#     model = YOLO(MODEL)  # Hoặc 'yolov8x.pt' nếu có file
#     model.fuse()
# finally:
#     # Khôi phục hàm load gốc
#     torch.load = original_load

# @app.post("/detect", response_model=DetectionResult)
# async def detect_objects(file: UploadFile = File(...)):
#     # Read and decode the uploaded image
#     contents = await file.read()
#     nparr = np.frombuffer(contents, np.uint8)  # Use np.frombuffer
#     img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

#     # Run YOLOv8 inference
#     results = model.predict(img)  # Returns a Results object

#     # Extract predictions
#     predictions = results[0]  # First image's results
#     boxes = predictions.boxes.xyxy.cpu().numpy().tolist()  # Bounding boxes
#     scores = predictions.boxes.conf.cpu().numpy().tolist()  # Confidence scores
#     labels = [model.names[int(cls)] for cls in predictions.boxes.cls.cpu().numpy()]  # Class labels

#     # Draw bounding boxes on the image
#     for i, box in enumerate(boxes):
#         x1, y1, x2, y2 = map(int, box)
#         label = f"{labels[i]}: {scores[i]:.2f}"
#         cv2.rectangle(img, (x1, y1), (x2, y2), (0, 255, 0), 2)
#         cv2.putText(img, label, (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 0, 0), 2)

#     # Save the output image locally
#     output_image_path = "output.jpg"
#     cv2.imwrite(output_image_path, img)

#     # Return detection results
#     return DetectionResult(boxes=boxes, labels=labels, scores=scores)

if __name__ == "__main__":
    host = "0.0.0.0"
    uvicorn.run("main:app",
                host=host,
                port=8000,
                reload=True)
    