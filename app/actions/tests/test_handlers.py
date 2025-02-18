import pytest

from app import settings
from app.actions.handlers import action_auth, action_pull_observations
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig
from app.actions.client import Farm, RumiUnauthorizedException, RumiNotFoundException


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
async def test_action_pull_observations_no_farms(mocker):
    integration = mocker.Mock()
    action_config = mocker.Mock(spec=PullObservationsConfig)
    auth_config = mocker.Mock()
    mocker.patch('app.actions.handlers.get_auth_config', return_value=auth_config)
    mocker.patch('app.actions.client.get_farms', return_value=[])

    result = await action_pull_observations(integration, action_config)

    assert result == {"farms_triggered": 0}

@pytest.mark.asyncio
async def test_action_pull_observations_unauthorized(mocker):
    integration = mocker.Mock()
    action_config = mocker.Mock(spec=PullObservationsConfig)
    auth_config = mocker.Mock()
    mocker.patch('app.actions.handlers.get_auth_config', return_value=auth_config)
    mocker.patch('app.actions.client.get_farms', side_effect=RumiUnauthorizedException(Exception(), "Unauthorized access"))

    with pytest.raises(RumiUnauthorizedException):
        await action_pull_observations(integration, action_config)
