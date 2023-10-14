from io import BytesIO

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from xlsxwriter import Workbook

from src.app.dtos.tubulars_dtos import CreateNewTubularDto, Property, UpdateTubularDto
from src.app.settings import Rig
from src.domain.entities.tools_management.tool_categories.cross_over import CrossOver
from src.domain.entities.tools_management.tool_classification import ToolClassification
from src.domain.enums.tubular_created_by_resource import ETubularCreatedByResource
from src.domain.enums.tubular_status import ETubularStatus
from src.domain.exceptions.domain_exception import NotFoundException
from src.domain.factories.tubular_factory import TubularFactory  # type: ignore
from src.domain.utils.tubular_report_utils import TubularReportUtils
from src.domain.utils.tubular_utils import TubularUtils  # type: ignore
from src.infra.repositories.tubular_repository import TubularRepository
from src.infra.repositories.views.tubular_views import TubularReportView


class TubularsService:
    def __init__(self, current_rig: Rig) -> None:
        self.current_rig = current_rig

    async def create(self, body: CreateNewTubularDto) -> JSONResponse:
        TubularType = TubularUtils.get_type(body.tubular_type)

        if not TubularType:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "tag": "invalidTubularType",
                    "message": "Invalid tubular type.",
                },
            )

        serial_number: Property = body.get_serial_number()

        if not serial_number:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "tag": "serialNumberNotFound",
                    "message": "No serial number found in the request body.",
                },
            )

        serial_number.value = TubularUtils.clean_serial_number_prefix_suffix(
            serial_number.value
        )

        if body.tubular_type == "cross_over":
            return await self.__create_cross_over(body, serial_number)
        else:
            return await self.__create_tubular(body, serial_number)

    async def __create_tubular(self, body: CreateNewTubularDto, sn: Property):
        tool_classification = await ToolClassification.find_one(
            ToolClassification.id == body.tool_classification_id
        )

        if not tool_classification:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "tag": "notFoundToolClassification",
                    "message": "The requested tool classification could not be found.",
                },
            )

        sn_exists = await self.__check_serial_number_already_exists(
            tubular_type=body.tubular_type,
            serial_number=sn.value,
            tool_classification=tool_classification,
        )

        if sn_exists:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "tag": "tubularSnAlreadyExists",
                    "message": "Serial number already exists to the specified Tool Classification.",
                },
            )

        if body.with_default_values:
            tubular = TubularFactory.create_with_default_values(
                body.tubular_type,
                body.properties,
                tool_classification=tool_classification,
                location_history=[self.current_rig.name],
                status=ETubularStatus.WITHOUT_INSPECTION,
                created_by=ETubularCreatedByResource.MANUAL,
            )
        else:
            tubular = TubularFactory.create(
                body.tubular_type,
                body.properties,
                tool_classification=tool_classification,
                location_history=[self.current_rig.name],
                status=ETubularStatus.WITHOUT_INSPECTION,
                created_by=ETubularCreatedByResource.MANUAL,
            )

        result = await tubular.insert()

        return JSONResponse(
            content={"_id": str(result.id)},
            status_code=status.HTTP_201_CREATED,
        )

    async def __create_cross_over(self, body: CreateNewTubularDto, sn: Property):
        sn_exists = await CrossOver.find_one(CrossOver.serial_number == sn.value)

        if sn_exists:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "tag": "tubularSnAlreadyExists",
                    "message": "Serial number already exists.",
                },
            )

        tubular = TubularFactory.create(
            body.tubular_type,
            body.properties,
            location_history=[self.current_rig.name],
            status=ETubularStatus.OPERATIONAL,
            created_by=ETubularCreatedByResource.MANUAL,
        )

        result = await tubular.insert()

        return JSONResponse(
            content={"_id": str(result.id)},
            status_code=status.HTTP_201_CREATED,
        )

    async def update(self, body: UpdateTubularDto) -> JSONResponse:
        TubularType = TubularUtils.get_type(body.tubular_type)
        if not TubularType:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid tubular type.",
            )

        tubular: TubularType = await TubularType.find_one(TubularType.id == body.id)
        if not tubular:
            raise NotFoundException(entity="Tubular", ids=body.id, data={"id": body.id})

        if tubular.status == ETubularStatus.TRANSFERRED:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot update a tubular transferred to another rig.",
            )

        serial_number = body.get_serial_number()
        existing_tubular = None

        if serial_number:
            existing_tubular: TubularType = await TubularType.find_one(
                TubularType.serial_number == serial_number
            )

        if existing_tubular and (tubular.serial_number != existing_tubular.serial_number):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Serial number already exists.",
            )

        # TODO os erros não são atualizados ao fazer o update das propriedades. Corrigir.
        await TubularRepository.update(tubular=tubular, values=body.get_properties_as_dict())

        return JSONResponse(
            content={"mss": "Updated"},
            status_code=status.HTTP_200_OK,
        )

    async def report(self) -> BytesIO:
        tools_classification: list[
            ToolClassification
        ] = await ToolClassification.find().to_list()

        if not tools_classification:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No tool classification found.",
            )

        report = BytesIO()
        workbook = Workbook(report)
        count: int = 0

        for tool in tools_classification:
            tubulars: list[
                TubularReportView
            ] = await TubularRepository.get_tubulars_by_tool_classification_id(
                tool.tubular_type, tool.id
            )

            count += 1
            ws_name = str(count) + "-" + tool.nominal_values.connection
            worksheet = workbook.add_worksheet(ws_name)
            worksheet = self.__add_worksheet_header(worksheet)

            if tubulars:
                row: int = 1
                for tubular in tubulars:
                    tubular.tool_type = tool.ui_name
                    worksheet = self.__populate_worksheet(tubular, worksheet, row)
                    row += 1
            else:
                message: str = f"No tubulars found to {tool.ui_name}"
                worksheet.write(1, 0, message)

        workbook.close()
        report.seek(0)

        return report

    async def __check_serial_number_already_exists(
        self, tubular_type: str, serial_number: str, tool_classification: ToolClassification
    ) -> bool:
        tubular_with_sn = await TubularRepository.get_tubular(
            tubular_type, serial_number, tool_classification.id
        )

        tubular_with_sn_suffix = await TubularRepository.get_tubular_by_suffix(
            tubular_type, serial_number, tool_classification.id
        )

        if tubular_with_sn or tubular_with_sn_suffix:
            return True

        return False

    def __add_worksheet_header(self, worksheet):
        headers: dict = TubularReportUtils.get_report_headers()
        column: int = 0

        for key in headers:
            value = headers.get(key)
            worksheet.write(0, column, value)
            column += 1

        return worksheet

    def __populate_worksheet(self, tubular, worksheet, row):
        headers: dict = TubularReportUtils.get_report_headers()
        props: dict = {}
        column: int = 0

        for key, value in tubular:
            prop = {key: value}
            props.update(prop)

        for key in headers:
            value = props.get(key)
            worksheet.write(row, column, value)
            column += 1

        return worksheet
