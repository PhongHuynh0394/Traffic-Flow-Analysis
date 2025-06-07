from pydantic import BaseModel
from typing import List

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