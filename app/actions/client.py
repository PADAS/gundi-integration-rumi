import logging
import httpx
import pydantic
import stamina

from datetime import datetime, timezone
from typing import Optional
from app.services.state import IntegrationStateManager


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


class Farm(pydantic.BaseModel):
    id: str = pydantic.Field(alias='_id')
    name: str
    nif: Optional[str]
    rega: Optional[str]

    class Config:
        allow_population_by_field_name = True


class FarmLocation(pydantic.BaseModel):
    location: tuple[float, float] = pydantic.Field(alias='_location')
    time: datetime = pydantic.Field(alias='_time')
    device_name: str
    official_tag: str

    @pydantic.validator('time', pre=True, always=True)
    def parse_time_string(cls, v):
        return datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)

    @pydantic.validator('location', pre=True, always=True)
    def split_location(cls, v):
        lat, lon = v.split("::")
        return float(lat), float(lon)

    class Config:
        allow_population_by_field_name = True


class RumiNotFoundException(Exception):
    def __init__(self, error: Exception, message: str, status_code=404):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


class RumiUnauthorizedException(Exception):
    def __init__(self, error: Exception, message: str, status_code=401):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_farms(integration, base_url, auth):
    async with httpx.AsyncClient(timeout=120) as session:
        logger.info(f"-- Getting farms for integration ID: {integration.id} User ID: {auth.user_id} --")

        url = f"{base_url}/users/{auth.user_id}/farms"

        try:
            response = await session.get(url, headers={"Authorization": f"Token {auth.token.get_secret_value()}"})
            if response.is_error:
                logger.error(f"Error 'get_farms'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                return [Farm.parse_obj(item) for item in parsed_response]
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise RumiUnauthorizedException(e, "Unauthorized access")
            elif e.response.status_code == 404:
                raise RumiNotFoundException(e, "User not found")
            raise e


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_farm_observations(integration, base_url, config):
    async with httpx.AsyncClient(timeout=120) as session:
        url = f"{base_url}/farms/{config.farm_id}/rumi/location/history"
        params = {
            "start": config.start.isoformat(),
            "stop": config.stop.isoformat(),
            "locations": config.locations,
            "user_id": config.user_id,
        }

        logger.info(f"-- Getting observations for integration ID: {integration.id} Farm: {config.farm_id} --")

        try:
            response = await session.get(url, params=params, headers={"Authorization": f"Token {config.token}"})
            if response.is_error:
                logger.error(f"Error 'get_farm_observations'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                obs = [FarmLocation.parse_obj(ob) for ob in parsed_response]
                return obs
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise RumiUnauthorizedException(e, "Unauthorized access")
            elif e.response.status_code == 404:
                raise RumiNotFoundException(e, "User not found")
            raise e
