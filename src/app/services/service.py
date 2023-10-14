from abc import ABC
from typing import Any

from fastapi import status
from fastapi.responses import JSONResponse

from src.core.domain.exceptions import DomainException


class Service(ABC):
    def error(self, exception: DomainException, entity: Any = None, **kwargs):
        if entity:
            raise exception(entity=entity, **kwargs)
        raise exception(**kwargs)

    def response(self, content: Any):
        return JSONResponse(status_code=status.HTTP_200_OK, content=content)
