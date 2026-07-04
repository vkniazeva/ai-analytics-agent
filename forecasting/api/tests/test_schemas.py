import pytest
from pydantic import ValidationError

from forecasting.api.schemas import (
    ThresholdType,
    DayPeriod,
    PredictRequest,
    PredictItemsResponse,
    PredictItemResponse,
    PredictCategoriesResponse
)


# Test ThresholdType enum
def test_threshold_type_valid_values():
    assert ThresholdType.low_missed_sales.value == "low_missed_sales"
    assert ThresholdType.low_wastage.value == "low_wastage"


def test_threshold_type_enum_members():
    threshold_types = list(ThresholdType)
    assert len(threshold_types) == 2
    assert ThresholdType.low_missed_sales in threshold_types
    assert ThresholdType.low_wastage in threshold_types


# Test DayPeriod enum
def test_day_period_valid_values():
    assert DayPeriod.morning.value == "Morning"
    assert DayPeriod.day.value == "Day"
    assert DayPeriod.evening.value == "Evening"
    assert DayPeriod.night.value == "Night"


def test_day_period_enum_members():
    day_periods = list(DayPeriod)
    assert len(day_periods) == 4
    assert DayPeriod.morning in day_periods
    assert DayPeriod.day in day_periods
    assert DayPeriod.evening in day_periods
    assert DayPeriod.night in day_periods


# Test PredictRequest
def test_predict_request_valid():
    request = PredictRequest(
        route="city_001 _ city_029",
        expected_pax=89,
        day_period=DayPeriod.morning
    )

    assert request.route == "city_001 _ city_029"
    assert request.expected_pax == 89
    assert request.day_period == DayPeriod.morning


def test_predict_request_day_period_string():
    # Should accept string that matches enum value
    request = PredictRequest(
        route="city_001 _ city_029",
        expected_pax=150,
        day_period="Day"
    )

    assert request.day_period == DayPeriod.day


def test_predict_request_missing_field():
    with pytest.raises(ValidationError) as exc_info:
        PredictRequest(
            route="city_001 _ city_029",
            expected_pax=89
            # missing day_period
        )

    errors = exc_info.value.errors()
    assert any(error['loc'] == ('day_period',) for error in errors)


def test_predict_request_invalid_pax_type():
    with pytest.raises(ValidationError):
        PredictRequest(
            route="city_001 _ city_029",
            expected_pax="invalid",  # Should be int
            day_period=DayPeriod.morning
        )


def test_predict_request_invalid_day_period():
    with pytest.raises(ValidationError):
        PredictRequest(
            route="city_001 _ city_029",
            expected_pax=89,
            day_period="InvalidPeriod"
        )


def test_predict_request_negative_pax():
    # Negative pax should be accepted (validation is business logic, not schema)
    request = PredictRequest(
        route="city_001 _ city_029",
        expected_pax=-10,
        day_period=DayPeriod.morning
    )

    assert request.expected_pax == -10


# Test PredictItemsResponse
def test_predict_items_response_valid():
    response = PredictItemsResponse(
        item_id="T3L4D001",
        predicted_quantity=5
    )

    assert response.item_id == "T3L4D001"
    assert response.predicted_quantity == 5


def test_predict_items_response_zero_quantity():
    response = PredictItemsResponse(
        item_id="T3L4D001",
        predicted_quantity=0
    )

    assert response.predicted_quantity == 0


def test_predict_items_response_missing_field():
    with pytest.raises(ValidationError) as exc_info:
        PredictItemsResponse(
            item_id="T3L4D001"
            # missing predicted_quantity
        )

    errors = exc_info.value.errors()
    assert any(error['loc'] == ('predicted_quantity',) for error in errors)


def test_predict_items_response_invalid_quantity_type():
    with pytest.raises(ValidationError):
        PredictItemsResponse(
            item_id="T3L4D001",
            predicted_quantity="invalid"  # Should be int
        )


# Test PredictItemResponse
def test_predict_item_response_valid():
    response = PredictItemResponse(
        item_id="T3L4D001",
        threshold_type=ThresholdType.low_missed_sales,
        threshold_value=0.5,
        predicted_quantity=5,
        historical_average=3.5,
        estimated_accuracy=0.85
    )

    assert response.item_id == "T3L4D001"
    assert response.threshold_type == ThresholdType.low_missed_sales
    assert response.threshold_value == 0.5
    assert response.predicted_quantity == 5
    assert response.historical_average == 3.5
    assert response.estimated_accuracy == 0.85


def test_predict_item_response_optional_accuracy():
    response = PredictItemResponse(
        item_id="T3L4D001",
        threshold_type=ThresholdType.low_wastage,
        threshold_value=0.3,
        predicted_quantity=2,
        historical_average=1.8
        # estimated_accuracy is optional
    )

    assert response.estimated_accuracy is None


def test_predict_item_response_threshold_type_string():
    response = PredictItemResponse(
        item_id="T3L4D001",
        threshold_type="low_wastage",
        threshold_value=0.3,
        predicted_quantity=2,
        historical_average=1.8
    )

    assert response.threshold_type == ThresholdType.low_wastage


def test_predict_item_response_missing_required_field():
    with pytest.raises(ValidationError) as exc_info:
        PredictItemResponse(
            item_id="T3L4D001",
            threshold_type=ThresholdType.low_missed_sales,
            threshold_value=0.5,
            predicted_quantity=5
            # missing historical_average
        )

    errors = exc_info.value.errors()
    assert any(error['loc'] == ('historical_average',) for error in errors)


def test_predict_item_response_invalid_threshold_value_type():
    with pytest.raises(ValidationError):
        PredictItemResponse(
            item_id="T3L4D001",
            threshold_type=ThresholdType.low_missed_sales,
            threshold_value="invalid",  # Should be float
            predicted_quantity=5,
            historical_average=3.5
        )


# Test PredictCategoriesResponse
def test_predict_categories_response_valid():
    response = PredictCategoriesResponse(
        category_name="Beverages",
        predicted_quantity=15
    )

    assert response.category_name == "Beverages"
    assert response.predicted_quantity == 15


def test_predict_categories_response_zero_quantity():
    response = PredictCategoriesResponse(
        category_name="Snacks",
        predicted_quantity=0
    )

    assert response.predicted_quantity == 0


def test_predict_categories_response_missing_field():
    with pytest.raises(ValidationError) as exc_info:
        PredictCategoriesResponse(
            category_name="Beverages"
            # missing predicted_quantity
        )

    errors = exc_info.value.errors()
    assert any(error['loc'] == ('predicted_quantity',) for error in errors)


def test_predict_categories_response_invalid_quantity_type():
    with pytest.raises(ValidationError):
        PredictCategoriesResponse(
            category_name="Beverages",
            predicted_quantity=15.5  # Should be int
        )


# Test model serialization
def test_predict_request_json_serialization():
    request = PredictRequest(
        route="city_001 _ city_029",
        expected_pax=89,
        day_period=DayPeriod.morning
    )

    json_data = request.model_dump()

    assert json_data["route"] == "city_001 _ city_029"
    assert json_data["expected_pax"] == 89
    assert json_data["day_period"] == "Morning"


def test_predict_item_response_json_serialization():
    response = PredictItemResponse(
        item_id="T3L4D001",
        threshold_type=ThresholdType.low_missed_sales,
        threshold_value=0.5,
        predicted_quantity=5,
        historical_average=3.5,
        estimated_accuracy=0.85
    )

    json_data = response.model_dump()

    assert json_data["item_id"] == "T3L4D001"
    assert json_data["threshold_type"] == "low_missed_sales"
    assert json_data["threshold_value"] == 0.5
    assert json_data["predicted_quantity"] == 5
    assert json_data["historical_average"] == 3.5
    assert json_data["estimated_accuracy"] == 0.85


def test_predict_item_response_json_with_none():
    response = PredictItemResponse(
        item_id="T3L4D001",
        threshold_type=ThresholdType.low_wastage,
        threshold_value=0.3,
        predicted_quantity=2,
        historical_average=1.8,
        estimated_accuracy=None
    )

    json_data = response.model_dump()

    assert json_data["estimated_accuracy"] is None
