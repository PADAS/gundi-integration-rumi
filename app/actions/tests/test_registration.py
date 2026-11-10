import pytest

from datetime import datetime, timezone
from app.actions.configurations import PullFarmObservationsConfig
from app.conftest import async_return
from app.services.action_runner import execute_action
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
    )

    parsed = PullFarmObservationsConfig.parse_obj(triggered.dict())

    assert parsed == triggered
    # The Rumi token stays in the integration's auth settings, out of the
    # triggering message (and so out of Pub/Sub, logs and activity events).
    assert "token" not in triggered.dict()
    assert parsed.start == datetime(2026, 11, 9, 19, 0, tzinfo=timezone.utc)
    assert parsed.stop.tzinfo is not None


def test_fetch_farm_observations_config_treats_naive_start_as_utc():
    parsed = PullFarmObservationsConfig.parse_obj({
        "start": "2026-11-09T19:00:00",
        "farm_id": "farm123",
        "farm_name": "Test Farm",
        "user_id": "user123",
    })

    assert parsed.start == datetime(2026, 11, 9, 19, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_triggered_fetch_farm_observations_runs_with_overrides_only(
        mocker, integration_v2, mock_config_manager, mock_publish_event
):
    # The action has no stored configuration in Gundi; everything it needs
    # arrives in config_overrides from pull_observations.
    mock_config_manager.get_action_configuration.return_value = async_return(None)
    integration_v2.configurations[2].data = {"user_id": "user123", "token": "testtoken"}
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    get_observations = mocker.patch("app.actions.client.get_farm_observations", return_value=None)
    triggered = PullFarmObservationsConfig(
        start="2026-11-09T19:00:00.000000Z",
        farm_id="farm123",
        farm_name="Test Farm",
        user_id="user123",
    )

    result = await execute_action(
        integration_id=str(integration_v2.id),
        action_id="fetch_farm_observations",
        config_overrides=triggered.dict(),
    )

    assert result == {"observations_extracted": 0}
    # Internal actions have no stored configuration, so the runner must not
    # look one up (a cache miss reloads the integration from Gundi).
    mock_config_manager.get_action_configuration.assert_not_called()
    get_observations.assert_called_once()
    assert get_observations.call_args.args[2].farm_id == "farm123"
    assert get_observations.call_args.args[3].token.get_secret_value() == "testtoken"
