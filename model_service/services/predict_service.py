import cv2
import numpy as np
from typing import List
from ultralytics import YOLO
from schemas.counting_data import DetectedObject

def process_image(contents: bytes, model: YOLO, allowed_class_ids: List[int]) -> List[DetectedObject] | None:
    """
    Decode the image from binary data, run YOLO inference, and filter predictions by allowed class IDs.
    
    Args:
        contents (bytes): Binary image data.
        model (YOLO): Loaded YOLOv8 model.
        allowed_class_ids (List[int]): List of class IDs to keep in results.
    
    Returns:
        List[DetectedObject] or None: List of detected objects matching class filters or None if image fails to decode.
    """
    # Decode image from binary
    nparr = np.frombuffer(contents, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    # If decoding fails
    if img is None:
        return None

    # Perform YOLO prediction
    results = model.predict(img)
    prediction = results[0]

    # Extract boxes, scores, class IDs
    boxes = prediction.boxes.xyxy.cpu().numpy()
    scores = prediction.boxes.conf.cpu().numpy()
    class_ids = prediction.boxes.cls.cpu().numpy().astype(int)
    labels = [model.names[int(cls)] for cls in class_ids]

    # Filter and build DetectedObject list
    objects = []
    for i in range(len(boxes)):
        class_id = int(class_ids[i])
        if class_id in allowed_class_ids:
            x1, y1, x2, y2 = map(float, boxes[i])
            objects.append(
                DetectedObject(
                    class_object=labels[i],
                    coordinates=[x1, y1, x2, y2],
                    confidence=round(float(scores[i]), 2),
                    class_id=class_id,
                    classname=labels[i]
                )
            )

    return objects, img.shape[:2]  # objects and image shape [height, width]
