import pydantic

from datetime import datetime, timezone
from app.actions.core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action, UIOptions, FieldWithUIOptions, GlobalUISchemaOptions


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    user_id: str
    token: pydantic.SecretStr = pydantic.Field(..., format="password")

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "user_id",
            "token",
        ],
    )


class PullObservationsConfig(PullActionConfiguration):
    default_lookback_days: int = FieldWithUIOptions(
        2,
        title="Default Lookback Days",
        description="Initial number of days to look back for observations Min: 1, Default: 2",
        ge=1,
        le=5,
        ui_options=UIOptions(
            widget="range",  # This will be rendered ad a range slider
        )
    )


class PullFarmObservationsConfig(PullActionConfiguration):
    start: str
    stop: str = datetime.now(timezone.utc).isoformat()
    locations: str = "all"
    farm_id: str
    farm_name: str
    user_id: str
    token: str


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)
