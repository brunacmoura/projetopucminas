from datetime import datetime

from beanie import PydanticObjectId
from fastapi import status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse

from src.app.dtos.tally_dtos import TallyActivatedPeriodResponseDto, TallyActivateDto
from src.app.services.log_service import LogService
from src.app.settings import Rig
from src.core.domain.events.publisher import EventPublisher
from src.domain.entities.logs.log import User
from src.domain.entities.logs.tally_log import TallyLog
from src.domain.entities.simulation.events.events import (
    TallyHasBeenActivatedEvent,
    TallyHasBeenDeactivatedEvent,
)
from src.domain.entities.tallies.tally import ETallyStatus, Tally
from src.domain.entities.tallies.tally_simulation import TallySimulation
from src.domain.entities.tallies.tally_stand import TallyStand
from src.domain.entities.tools_management.tool_categories.cross_over import CrossOver
from src.domain.enums.tubular_status import ETubularStatus
from src.domain.exceptions.domain_exception import NotFoundException
from src.domain.exceptions.tally_exceptions import TubularTransferredException
from src.domain.factories.log_factory import LogFactory
from src.infra.repositories.tally_repository import TallyRepository


class TallyService:
    def __init__(
        self,
        current_rig: Rig,
        tally_log_service: LogService[TallyLog],
        event_publisher: EventPublisher,
    ) -> None:
        self.current_rig = current_rig
        self.tally_log_service = tally_log_service
        self.event_publisher = event_publisher

    async def resolve_tubulars(self, tally_stands: list[TallyStand]):
        for stand in tally_stands:
            for substand in stand.substands:
                substand.tubular.model_class = (
                    CrossOver
                    if substand.tubular.ref.collection == "cross_overs"
                    else substand.tubular.model_class
                )
                substand.tubular = await substand.tubular.fetch_one(link=substand.tubular)

    async def get_active_tally(self) -> JSONResponse:
        active_tally: Tally = await Tally.find_one(
            Tally.status == ETallyStatus.Active, fetch_links=True
        )

        if not active_tally:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="There is no tally currently active.",
            )

        tally_simulation: TallySimulation = await TallySimulation.find_one(
            TallySimulation.tally.id == active_tally.id, fetch_links=True
        )

        await self.resolve_tubulars(tally_simulation.tally.stands)

        return tally_simulation

    async def activate_tally(self, body: TallyActivateDto, user: User) -> JSONResponse:
        tally: Tally = await Tally.find_one(
            Tally.id == body.tally_id, Tally.status != ETallyStatus.Draft, fetch_links=True
        )

        if not tally:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Tally {body.tally_id} not found",
            )

        current_active_tally: Tally = await Tally.find_one(
            Tally.status == ETallyStatus.Active, fetch_links=True
        )

        if current_active_tally and self.__is_the_same_tally(current_active_tally, tally):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The tally is already active",
            )

        await self.__has_tubular_transferred_in_tally(tally)

        if current_active_tally and not self.__is_the_same_tally(current_active_tally, tally):
            current_active_tally.deactivate()

            await current_active_tally.save()
            await self.__save_tally_log(current_active_tally, user)

        tally.activate()

        await tally.save()
        await self.__save_tally_log(tally, user)

        await self.event_publisher.publish(
            TallyHasBeenActivatedEvent(
                rig=self.current_rig.name,
                tally_id=str(tally.id),
                user=user.email,
                activation_date=tally.updated_at,
            )
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "tally_id": str(tally.id),
                "message": "Tally successfully activated",
            },
        )

    async def deactivate_tally(self, body: TallyActivateDto, user: User) -> JSONResponse:
        tally: Tally = await Tally.find_one(Tally.id == body.tally_id, fetch_links=True)

        if not tally:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Tally {body.tally_id} not found",
            )

        if tally.status != ETallyStatus.Active:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="This tally isn't active",
            )

        tally.deactivate()

        await tally.save()
        await self.__save_tally_log(tally, user)

        await self.event_publisher.publish(
            TallyHasBeenDeactivatedEvent(
                rig=self.current_rig.name,
                tally_id=str(tally.id),
                user=user.email,
                deactivation_date=tally.updated_at,  # type: ignore
            )
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "tally_id": str(tally.id),
                "message": "Tally successfully deactivated",
            },
        )

    async def get_tally_information_on_activated_period(
        self, id: PydanticObjectId
    ) -> TallyActivatedPeriodResponseDto:
        tally = await TallyRepository.get_tally_information_on_activated_period(id)

        if not tally:
            raise NotFoundException(Tally.__name__, id)

        uptime_minutes: int = tally.total_uptime_in_minutes
        drilled_meters: float = tally.accumulated_drilled_meters
        rotated_hours: float = tally.accumulated_rotated_seconds
        activated_date: datetime = None
        deactivated_date: datetime = None

        if tally.logs and tally.logs[0].tally_status == ETallyStatus.Active:
            uptime_minutes = tally.total_uptime_in_minutes - tally.logs[0].dp_uptime_minutes
            drilled_meters = tally.accumulated_drilled_meters - tally.logs[0].dp_drilled_meters
            rotated_hours = (
                tally.accumulated_rotated_seconds - tally.logs[0].dp_rotating_seconds
            ) / 3600
            activated_date = tally.logs[0].created_at
        elif tally.logs and tally.logs[0].tally_status == ETallyStatus.Deactivated:
            uptime_minutes = tally.logs[0].dp_uptime_minutes - tally.logs[1].dp_uptime_minutes
            drilled_meters = tally.logs[0].dp_drilled_meters - tally.logs[1].dp_drilled_meters
            rotated_hours = (
                tally.logs[0].dp_rotating_seconds - tally.logs[1].dp_rotating_seconds
            ) / 3600
            activated_date = tally.logs[1].created_at
            deactivated_date = tally.logs[0].created_at

        response = TallyActivatedPeriodResponseDto(
            id=tally.id,
            uptime_minutes=uptime_minutes,
            drilled_meters=drilled_meters,
            rotated_hours=rotated_hours,
            activated_date=activated_date,
            deactivated_date=deactivated_date,
        )

        return response

    def __is_the_same_tally(self, ts_one: Tally, ts_two: Tally) -> bool:
        if ts_one.id == ts_two.id:
            return True
        return False

    async def __has_tubular_transferred_in_tally(self, tally: Tally):
        tubulars_transferred: list[str] = []

        for substand in tally.substands:
            if not substand.tubular.ref.collection == "cross_overs":
                substand.tubular = await substand.tubular.fetch_one(link=substand.tubular)
                if substand.tubular.status == ETubularStatus.TRANSFERRED:
                    tubulars_transferred.append(substand.tubular.serial_number)

        if tubulars_transferred:
            raise TubularTransferredException(tubulars_transferred)

    async def __save_tally_log(self, tally: Tally, user: User):
        tally_log = LogFactory.make(
            model=TallyLog,
            tally=tally,
            tally_status=tally.status,
            tally_operation=tally.operation_description,
            tally_well=tally.well,
            dp_drilled_meters=tally.accumulated_drilled_meters,
            dp_rotating_seconds=tally.accumulated_rotated_seconds,
            dp_acid_circulation_hours=tally.accumulated_acid_circulation_seconds,
            dp_uptime_minutes=tally.total_uptime_in_minutes,
            user=user,
        )

        await self.tally_log_service.create(log=tally_log)
