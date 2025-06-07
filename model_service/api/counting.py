from fastapi import APIRouter, File, UploadFile
from ultralytics import YOLO
from schemas.counting_data import DetectionResponse
from services.predict_service import process_image

router = APIRouter()

# Load the YOLOv8 model
MODEL_PATH = "./yolov8m.pt"
model = YOLO(MODEL_PATH)

# Define the allowed class IDs to include in results
CLASS_ID = [1, 2, 3, 5, 7]  # e.g., 1: bicycle, 2: car, etc.


@router.post("/object_counting/predict", response_model=DetectionResponse)
async def detect_objects(file: UploadFile = File(...)):
    # Read uploaded image content
    contents = await file.read()

     # Process image and get detection results
    result = process_image(contents, model, CLASS_ID)

    if result is None:
        return {"error": "Could not decode image."}

    objects, shape = result

    response = DetectionResponse(
        image_shape=list(shape),
        total=len(objects),
        objects=objects
    )

    return response

if __name__ == "__main__":
    pass   