import logging
import math

from soniox import SonioxClient
from soniox.types import (
    TranscriptionConfig,
    TranscriptionJobStatus,
    TranscriptionWebhook,
)

from server.config import Settings
from utils import conditions, finance, texttools

from .models import TranscribeTask

soniox = SonioxClient(Settings.soniox_api_key)


async def process_transcribe(
    task: TranscribeTask,
    *,
    force_restart: bool = False,
    sync: bool = False,
    **kwargs: object,
) -> TranscribeTask:
    logging.info("Starting processing for task %s", task.id)

    quota = await finance.check_quota(
        task.user_id, task.audio_duration, raise_exception=False
    )
    if quota < 1:
        task.task_status = "error"
        await task.save_report("insufficient_quota")
        return task

    job = await soniox.transcribe_url_async(
        task.file_url,
        TranscriptionConfig(
            language_hints=["fa", "en"],
            enable_language_identification=True,
            enable_speaker_diarization=True,
            client_reference_id=task.uid,
            webhook_url=task.item_webhook_url,
        ),
    )

    # job_id = await speechmatics.Speechmatics().create_transcribe_job(
    #     task.file_url,
    #     task.item_webhook_url,
    #     # secret_token=task.secret_token,
    #     # diarization=task.diarization,
    #     language=(
    #         # task.source_language.abbreviation
    #         # if task.source_language != "auto"
    #         # else
    #         "auto"
    #     ),
    #     # enhanced=task.enhanced,
    # )
    task.transcription_job_id = job.id
    task.task_status = "processing"
    await task.save()
    if not sync:
        return task

    await conditions.Conditions().wait_condition(task.uid)

    task = await TranscribeTask.get_item(task.uid, user_id=task.user_id)
    job_result = await soniox.get_transcription_job_async(task.transcription_job_id)

    if job_result.status != TranscriptionJobStatus.COMPLETED:
        task.task_status = "error"
        await task.save_report("transcription_failed")
        return task

    return await process_transcription_webhook(
        task,
        TranscriptionWebhook(
            id=job_result.id,
            status=job_result.status,
        ),
    )


async def save_result(
    task: TranscribeTask,
    result: str,
    usage_amount: float | None = None,
    usage_id: str | None = None,
) -> TranscribeTask:
    task.result = texttools.normalize_text(result)
    task.task_status = "completed"
    task.usage_amount = usage_amount
    task.usage_id = usage_id
    return await task.save()


async def process_transcription_webhook(
    task: TranscribeTask,
    # data: speechmatics.TranscribeWebhookSchema
    data: TranscriptionWebhook,
) -> TranscribeTask:
    # Process the webhook data
    # Extract the sentences and timings from the data
    translation_cost = 0

    if task.transcription_job_id != data.id:
        return await process_error_webhook(task, "Transcription job ID does not match")
    if data.status != TranscriptionJobStatus.COMPLETED:
        return await process_error_webhook(task, "Transcription job status is error")
    if data.status == TranscriptionJobStatus.ERROR:
        return await process_error_webhook(task, "Transcription job status is error")

    job_result = await soniox.get_transcription_job_async(task.transcription_job_id)

    transcription_cost = math.ceil(
        (job_result.audio_duration_ms / 60 / 1000) * Settings.minutes_price
    )
    total_cost = transcription_cost + translation_cost
    await finance.meter_cost(task.user_id, total_cost)
    logging.info(
        "%s %s %s %s",
        task.uid,
        job_result.audio_duration_ms,
        total_cost,
        transcription_cost,
    )

    task.task_status = "completed"
    await task.save_report("Task processed successfully")
    result = await soniox.get_transcription_result_async(task.transcription_job_id)

    await conditions.Conditions().release_condition(task.uid)
    return await save_result(task, result.text, transcription_cost)


async def process_error_webhook(
    task: TranscribeTask, message: str = ""
) -> TranscribeTask:
    # speechmatic_task: speechmatics.JobDetails = (
    #     await speechmatics.Speechmatics().get_transcribe_job(
    #        task.transcription_job_id
    #     )
    # )
    job = await soniox.get_transcription_job_async(task.transcription_job_id)

    task.task_status = "error"
    await task.save_report(f"{message}\n\n{job.error_message}", emit=False)
    logging.warning("Transcription rejected %s", f"{message}\n\n{job.error_message}")

    await task.save_and_emit()
    await conditions.Conditions().release_condition(task.uid)
    return task
