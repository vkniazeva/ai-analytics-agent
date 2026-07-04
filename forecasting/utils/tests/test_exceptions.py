import pytest

from forecasting.utils.exceptions import ValidationError, ModelDegradationError


# Test ValidationError
def test_validation_error_is_exception():
    assert issubclass(ValidationError, Exception)


def test_validation_error_can_be_raised():
    with pytest.raises(ValidationError):
        raise ValidationError("Test error")


def test_validation_error_with_message():
    message = "Invalid data format"
    with pytest.raises(ValidationError, match=message):
        raise ValidationError(message)


def test_validation_error_can_be_caught():
    try:
        raise ValidationError("Test error")
    except ValidationError as e:
        assert str(e) == "Test error"


def test_validation_error_empty_message():
    with pytest.raises(ValidationError):
        raise ValidationError()


# Test ModelDegradationError
def test_model_degradation_error_is_exception():
    assert issubclass(ModelDegradationError, Exception)


def test_model_degradation_error_can_be_raised():
    with pytest.raises(ModelDegradationError):
        raise ModelDegradationError("Model performance degraded")


def test_model_degradation_error_with_message():
    message = "Accuracy dropped by 15%"
    with pytest.raises(ModelDegradationError, match=message):
        raise ModelDegradationError(message)


def test_model_degradation_error_can_be_caught():
    try:
        raise ModelDegradationError("Performance drop detected")
    except ModelDegradationError as e:
        assert str(e) == "Performance drop detected"


def test_model_degradation_error_empty_message():
    with pytest.raises(ModelDegradationError):
        raise ModelDegradationError()


# Test exception inheritance
def test_validation_error_inherits_from_exception():
    error = ValidationError("test")
    assert isinstance(error, Exception)


def test_model_degradation_error_inherits_from_exception():
    error = ModelDegradationError("test")
    assert isinstance(error, Exception)


# Test both exceptions can be caught as Exception
def test_can_catch_validation_error_as_exception():
    try:
        raise ValidationError("test")
    except Exception as e:
        assert isinstance(e, ValidationError)


def test_can_catch_model_degradation_error_as_exception():
    try:
        raise ModelDegradationError("test")
    except Exception as e:
        assert isinstance(e, ModelDegradationError)


# Test exceptions are different
def test_exceptions_are_different_types():
    assert ValidationError != ModelDegradationError


def test_validation_error_not_caught_by_model_degradation():
    with pytest.raises(ValidationError):
        try:
            raise ValidationError("test")
        except ModelDegradationError:
            pass


def test_model_degradation_error_not_caught_by_validation():
    with pytest.raises(ModelDegradationError):
        try:
            raise ModelDegradationError("test")
        except ValidationError:
            pass
