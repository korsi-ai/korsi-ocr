from fastapi_mongo_base.models import UserOwnedEntity

from .schemas import TranslateSchema


class TranslateTask(TranslateSchema, UserOwnedEntity):
    async def start_processing(self) -> None:
        from . import services

        self.task_status = "processing"
        return await services.process_translate(self)
