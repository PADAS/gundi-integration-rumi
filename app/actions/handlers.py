import httpx
import logging

import app.actions.client as client

from datetime import datetime, timedelta, timezone
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, PullFarmObservationsConfig, get_auth_config
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


RUMI_BASE_URL = "https://innogando-backend-prod-01.innogando.com"


def transform(farm, observation):
    return {
        "source_name": observation.device_name,
        "source": observation.official_tag,
        "type": "tracking-device",
        "subject_type": "vehicle",
        "recorded_at": observation.time,
        "location": {
            "lat": observation.location[0],
            "lon": observation.location[1]
        },
        "additional": {
            "farm_id": farm.farm_id,
            "farm_name": farm.farm_name
        }
    }


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing 'auth' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or RUMI_BASE_URL

    try:
        response = await client.get_farms(integration, base_url, action_config)
        if not response:
            logger.error(f"Failed to authenticate with integration {integration.id} using {action_config}")
            return {"valid_credentials": False, "message": "Bad credentials"}
        return {"valid_credentials": True}
    except client.RumiUnauthorizedException as e:
        return {"valid_credentials": False, "status_code": e.status_code, "message": "Invalid token"}
    except client.RumiNotFoundException as e:
        return {"valid_credentials": False, "status_code": e.status_code, "message": "Invalid user_id"}
    except httpx.HTTPStatusError as e:
        return {"error": True, "status_code": e.response.status_code}


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or RUMI_BASE_URL
    auth_config = get_auth_config(integration)

    try:
        farms = await client.get_farms(integration, base_url, auth_config)
        if farms:
            logger.info(f"Found {len(farms)} farms for integration {integration.id} User ID: {auth_config.user_id}")
            now = datetime.now(timezone.utc)
            farms_triggered = 0
            for farm in farms:
                logger.info(f"Triggering 'action_fetch_farm_observations' action for farm {farm.id} to extract observations...")
                device_state = await state_manager.get_state(
                    integration_id=integration.id,
                    action_id="pull_observations",
                    source_id=farm.id
                )
                if not device_state:
                    logger.info(f"Setting initial lookback days for device {farm.id} to {action_config.default_lookback_days}")
                    start = (now - timedelta(days=action_config.default_lookback_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    logger.info(f"Setting begin time for device {farm.id} to {device_state.get('updated_at')}")
                    start = device_state.get("updated_at")

                config = {
                    "start": start,
                    "stop": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "farm_id": farm.id,
                    "farm_name": farm.name,
                    "user_id": auth_config.user_id,
                    "token": auth_config.token.get_secret_value(),
                }
                parsed_config = PullFarmObservationsConfig.parse_obj(config)
                await trigger_action(integration.id, "fetch_farm_observations", config=parsed_config)
                farms_triggered += 1
            return {"farms_triggered": farms_triggered}
        else:
            logger.warning(f"No farms found for integration {integration.id} User ID: {auth_config.user_id}")
            return {"farms_triggered": 0}
    except (client.RumiUnauthorizedException, client.RumiNotFoundException) as e:
        message = f"Failed to authenticate with integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise e
    except httpx.HTTPStatusError as e:
        message = f"'pull_observations' action error with integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise e


@activity_logger()
async def action_fetch_farm_observations(integration, action_config: PullFarmObservationsConfig):
    logger.info(f"Executing action 'fetch_farm_observations' for integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or RUMI_BASE_URL
    observations_extracted = 0

    try:
        observations = await client.get_farm_observations(integration, base_url, action_config)
        if observations:
            logger.info(f"Extracted {len(observations)} observations for farm {action_config.farm_id}")
            transformed_data = [transform(action_config, ob) for ob in observations]

            for i, batch in enumerate(generate_batches(transformed_data, 200)):
                logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Farm: {action_config.farm_id}')
                response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                observations_extracted += len(response)

            # Save latest device updated_at
            latest_time = max(observations, key=lambda obs: obs.time).time
            state = {"updated_at": latest_time}

            await state_manager.set_state(
                integration_id=integration.id,
                action_id="pull_observations",
                state=state,
                source_id=action_config.farm_id
            )

            return {"observations_extracted": observations_extracted}
        else:
            logger.warning(f"No observations found for farm {action_config.farm_id}")
            return {"observations_extracted": 0}
    except (client.RumiUnauthorizedException, client.RumiNotFoundException) as e:
        message = f"Failed to authenticate with integration {integration.id} using {action_config}. Exception: {e}"
        logger.exception(message)
        raise e
    except httpx.HTTPStatusError as e:
        message = f"'fetch_farm_observations' action error with integration {integration.id} using {action_config}. Exception: {e}"
        logger.exception(message)
        raise e
