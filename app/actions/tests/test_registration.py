import pytest

from datetime import datetime, timezone
from app.actions.configurations import PullFarmObservationsConfig
from app.services.self_registration import register_integration_in_gundi


@pytest.mark.asyncio
async def test_register_rumi_skips_fetch_farm_observations(mocker, mock_gundi_client_v2):
    # fetch_farm_observations only runs when pull_observations triggers it,
    # so it must not be registered (and therefore never scheduled) in Gundi.
    mocker.patch(
        "app.services.self_registration.get_webhook_handler",
        side_effect=NotImplementedError,
    )

    await register_integration_in_gundi(gundi_client=mock_gundi_client_v2, type_slug="rumi")

    registered = mock_gundi_client_v2.register_integration_type.call_args.args[0]
    actions = {action["value"]: action for action in registered["actions"]}
    assert "fetch_farm_observations" not in actions
    assert actions["pull_observations"]["is_periodic_action"] is True
    assert actions["auth"]["is_periodic_action"] is False


def test_fetch_farm_observations_config_parses_triggered_overrides():
    # pull_observations sends the sub-action its settings as config_overrides
    # built from PullFarmObservationsConfig.dict(); they must parse back.
    triggered = PullFarmObservationsConfig(
        start="2026-11-09T19:00:00.000000Z",
        farm_id="farm123",
        farm_name="Test Farm",
        user_id="user123",
        token="testtoken",
    )

    parsed = PullFarmObservationsConfig.parse_obj(triggered.dict())

    assert parsed == triggered
    assert parsed.start == datetime(2026, 11, 9, 19, 0, tzinfo=timezone.utc)
    assert parsed.stop.tzinfo is not None


def test_fetch_farm_observations_config_treats_naive_start_as_utc():
    parsed = PullFarmObservationsConfig.parse_obj({
        "start": "2026-11-09T19:00:00",
        "farm_id": "farm123",
        "farm_name": "Test Farm",
        "user_id": "user123",
        "token": "testtoken",
    })

    assert parsed.start == datetime(2026, 11, 9, 19, 0, tzinfo=timezone.utc)
