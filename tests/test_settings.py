import os

from app.settings import get_settings, reset_settings_cache


def test_settings_loads_defaults():
    os.environ.pop("CONTACT_RATE_LIMIT", None)
    os.environ.pop("CONTACT_RATE_WINDOW", None)
    reset_settings_cache()
    s = get_settings()
    assert s.mongo_uri.startswith("mongodb://"), "Expected default Mongo URI"
    assert s.contact_rate_limit == 5, "Default contact rate limit should be 5"


def test_settings_warning_list_is_list():
    s = get_settings()
    warnings = s.recommended_warnings()
    assert isinstance(warnings, list)


def test_warnings_present_when_no_security():
    # Ensure no captcha or token set in env for this test context
    for var in ["HCAPTCHA_SECRET", "RECAPTCHA_SECRET", "CONTACT_TOKEN"]:
        os.environ.pop(var, None)
    # Force re-load (pydantic BaseSettings is cached by our get_settings).
    # In a more elaborate setup we'd expose a reset, but for initial smoke test
    # just call again (same object) and check contents.
    s = get_settings()
    warnings = s.recommended_warnings()
    assert any("unprotected" in w for w in warnings)
