from typing import Self

from fastapi_mongo_base.models import UserOwnedEntity

from .schemas import TranscribeTaskSchema


class TranscribeTask(TranscribeTaskSchema, UserOwnedEntity):
    async def start_processing(
        self, *, force_restart: bool = False, sync: bool = False, **kwargs: object
    ) -> Self:
        from . import services

        self.task_status = "processing"
        return await services.process_transcribe(
            self, force_restart=force_restart, sync=sync, **kwargs
        )
