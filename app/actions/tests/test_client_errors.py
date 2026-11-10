import httpx

from app.actions.client import RumiNotFoundException, RumiUnauthorizedException
from app.services.errors import classify_error
from app.services.retry_policies import is_retryable_failure


def _status_error(status_code):
    request = httpx.Request("GET", "https://rumi-api.innogando.com/v1/users/user123/farms")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f"Client error '{status_code}'", request=request, response=response)


def test_rumi_unauthorized_is_classified_as_auth_failure():
    exc = RumiUnauthorizedException(_status_error(401), "Unauthorized access")

    classified = classify_error(exc)

    assert classified.error_type == "auth"
    assert classified.title == "Authentication failed"
    assert classified.message == "Unauthorized access"
    assert classified.status_code == 401
    assert exc.status_code == 401
    assert not is_retryable_failure(exc)


def test_rumi_not_found_is_classified_and_not_retried():
    exc = RumiNotFoundException(_status_error(404), "User not found")

    classified = classify_error(exc)

    assert classified.error_type == "bad_response"
    assert classified.message == "User not found"
    assert classified.status_code == 404
    assert exc.status_code == 404
    assert not is_retryable_failure(exc)


def test_rumi_exceptions_keep_their_text():
    exc = RumiUnauthorizedException(Exception("boom"), "Unauthorized access")

    assert str(exc) == "'401: Unauthorized access, Error: boom'"
