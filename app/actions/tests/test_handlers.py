import httpx
import pytest
import respx

from datetime import datetime, timezone
from app import settings
from app.actions.handlers import action_auth, action_pull_observations, action_fetch_farm_observations
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, PullFarmObservationsConfig
from app.actions.client import Farm, FarmLocation, RumiUnauthorizedException, RumiNotFoundException, get_farm_observations


@pytest.mark.asyncio
async def test_action_auth_success(mocker):
    integration = mocker.Mock()
    action_config = mocker.Mock(spec=AuthenticateConfig)
    mocker.patch('app.actions.client.get_farms', return_value=[{"id": "farm1"}])

    result = await action_auth(integration, action_config)

    assert result == {"valid_credentials": True}

@pytest.mark.asyncio
async def test_action_auth_unauthorized(mocker):
    integration = mocker.Mock()
    action_config = mocker.Mock(spec=AuthenticateConfig)
    mocker.patch('app.actions.client.get_farms', side_effect=RumiUnauthorizedException(Exception(), "Unauthorized access"))

    result = await action_auth(integration, action_config)

    assert result == {"valid_credentials": False, "status_code": 401, "message": "Invalid token"}

@pytest.mark.asyncio
async def test_action_auth_not_found(mocker):
    integration = mocker.Mock()
    action_config = mocker.Mock(spec=AuthenticateConfig)
    mocker.patch('app.actions.client.get_farms', side_effect=RumiNotFoundException(Exception(), "User not found"))

    result = await action_auth(integration, action_config)

    assert result == {"valid_credentials": False, "status_code": 404, "message": "Invalid user_id"}

@pytest.mark.asyncio
async def test_action_pull_observations_triggers_fetch_farm_observations_action(mocker, integration_v2, mock_publish_event):
    settings.TRIGGER_ACTIONS_ALWAYS_SYNC = False
    settings.INTEGRATION_COMMANDS_TOPIC = "rumi-actions-topic"

    mocker.patch('app.actions.client.get_farms', return_value=[
        Farm.parse_obj({"id": "farm1", "name": "Farm 1"})
    ])
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.trigger_action", return_value=None)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.execute_action", return_value=None)

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"user_id": "user", "token": "faketoken123"}

    action_config = PullObservationsConfig(default_lookback_days=5)

    result = await action_pull_observations(integration, action_config)

    assert result == {"farms_triggered": 1}

@pytest.mark.asyncio
async def test_action_pull_observations_no_farms(mocker, integration_v2, mock_publish_event):
    mocker.patch('app.actions.client.get_farms', return_value=[])
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.trigger_action", return_value=None)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.execute_action", return_value=None)

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"user_id": "user", "token": "faketoken123"}

    action_config = PullObservationsConfig(default_lookback_days=5)

    result = await action_pull_observations(integration, action_config)

    assert result == {"farms_triggered": 0}

@pytest.mark.asyncio
async def test_action_pull_observations_unauthorized(mocker, integration_v2, mock_publish_event):
    mocker.patch('app.actions.client.get_farms', side_effect=RumiUnauthorizedException(Exception(), "Unauthorized access"))
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.trigger_action", return_value=None)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.execute_action", return_value=None)

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"user_id": "user", "token": "faketoken123"}

    action_config = PullObservationsConfig(default_lookback_days=5)

    with pytest.raises(RumiUnauthorizedException):
        await action_pull_observations(integration, action_config)


# --- action_fetch_farm_observations ---

RUMI_BASE_URL = "https://innogando-backend-prod-01.innogando.com"

MOCK_TIMELAPSE_RESPONSE = [
    {
        "official_tag": "TAG001",
        "rumi_id": "DEVICE001",
        "name": "Cow 1",
        "locations": [
            {"_time": "2026-04-21T10:00:00Z", "location": "40.38::-1.61"},
            {"_time": "2026-04-21T11:00:00Z", "location": "40.39::-1.62"},
        ],
    }
]

MOCK_FARM_LOCATIONS = [
    FarmLocation.parse_obj({"_location": "40.38::-1.61", "_time": "2026-04-21T10:00:00Z", "device_name": "DEVICE001", "official_tag": "TAG001"}),
    FarmLocation.parse_obj({"_location": "40.39::-1.62", "_time": "2026-04-21T11:00:00Z", "device_name": "DEVICE001", "official_tag": "TAG001"}),
]

MOCK_ANIMALS_INFO = {
    "cow": [{"rumi_id": "DEVICE001", "name": "Cow 1", "official_tag": "TAG001"}]
}


@pytest.fixture
def farm_action_config():
    return PullFarmObservationsConfig(
        start=datetime(2026, 4, 21, 0, 0, 0, tzinfo=timezone.utc),
        stop=datetime(2026, 4, 22, 0, 0, 0, tzinfo=timezone.utc),
        farm_id="farm123",
        farm_name="Test Farm",
        user_id="user123",
        token="testtoken",
    )


@pytest.mark.asyncio
async def test_action_fetch_farm_observations_success(mocker, integration_v2, mock_publish_event, farm_action_config):
    mocker.patch("app.actions.client.get_farm_observations", return_value=MOCK_FARM_LOCATIONS)
    mocker.patch("app.actions.handlers.get_animals_info", return_value=MOCK_ANIMALS_INFO)
    mocker.patch("app.actions.handlers.send_observations_to_gundi", return_value=[{"id": "obs1"}, {"id": "obs2"}])
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    result = await action_fetch_farm_observations(integration_v2, farm_action_config)

    assert result == {"observations_extracted": 2}


@pytest.mark.asyncio
async def test_action_fetch_farm_observations_no_observations(mocker, integration_v2, mock_publish_event, farm_action_config):
    mocker.patch("app.actions.client.get_farm_observations", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    result = await action_fetch_farm_observations(integration_v2, farm_action_config)

    assert result == {"observations_extracted": 0}


@pytest.mark.asyncio
async def test_get_farm_observations_chunks_long_window(mocker):
    """A window longer than 48h is split into two timelapse requests."""
    integration = mocker.Mock()
    integration.id = "429face7-855e-4e01-9cc8-fe69bf437cd9"
    config = PullFarmObservationsConfig(
        start=datetime(2026, 4, 19, 0, 0, 0, tzinfo=timezone.utc),
        stop=datetime(2026, 4, 22, 0, 0, 0, tzinfo=timezone.utc),  # 3-day window
        farm_id="farm123",
        farm_name="Test Farm",
        user_id="user123",
        token="testtoken",
    )

    with respx.mock:
        route = respx.get(f"{RUMI_BASE_URL}/farms/farm123/rumi/realtime/timelapse").mock(
            return_value=httpx.Response(200, json=MOCK_TIMELAPSE_RESPONSE)
        )
        result = await get_farm_observations(integration, RUMI_BASE_URL, config)

    assert route.call_count == 2  # 3-day window → two 48h chunks
    assert len(result) == 4        # 2 locations per chunk × 2 chunks
