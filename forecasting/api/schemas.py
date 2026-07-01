from typing import Optional

from pydantic import BaseModel
from enum import Enum


class ThresholdType(str, Enum):
    low_missed_sales = "low_missed_sales"
    low_wastage = "low_wastage"

class DayPeriod(str, Enum):
    morning = "Morning"
    day = "Day"
    evening = "Evening"
    night = "Night"

class PredictRequest(BaseModel):
    route: str
    expected_pax: int
    day_period: DayPeriod

class PredictItemsResponse(BaseModel):
    item_id: str
    predicted_quantity: int

class PredictItemResponse(BaseModel):
    item_id: str
    threshold_type: ThresholdType
    threshold_value: float
    predicted_quantity: int
    historical_average: float
    estimated_accuracy: Optional[float] = None

class PredictCategoriesResponse(BaseModel):
    category_name: str
    predicted_quantity: int