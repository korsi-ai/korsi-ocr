import logging
import math

from server.config import Settings
from utils import conditions, finance, speechmatics, texttools

from .models import TranscribeTask


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

    job_id = await speechmatics.Speechmatics().create_transcribe_job(
        task.file_url,
        task.item_webhook_url,
        # secret_token=task.secret_token,
        # diarization=task.diarization,
        language=(
            # task.source_language.abbreviation
            # if task.source_language != "auto"
            # else
            "auto"
        ),
        # enhanced=task.enhanced,
    )
    task.transcription_job_id = job_id
    task.task_status = "processing"
    await task.save()

    if sync:
        # logging.info(f"Waiting for condition {subtitle_task.uid}")
        await conditions.Conditions().wait_condition(task.uid)

        task = await TranscribeTask.get_item(task.uid, user_id=task.user_id)

    usage = await finance.meter_cost(task.user_id, task.audio_duration)
    result = await speechmatics.Speechmatics().get_transcript(task.transcription_job_id)
    await save_result(
        task,
        result,
        usage_amount=usage.amount,
        usage_id=usage.uid,
    )

    return task


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
    task: TranscribeTask, data: speechmatics.TranscribeWebhookSchema
) -> TranscribeTask:
    # Process the webhook data
    # Extract the sentences and timings from the data
    translation_cost = 0
    transcription_cost = math.ceil((data.job.duration / 60) * Settings.minutes_price)

    total_cost = transcription_cost + translation_cost
    await finance.meter_cost(task.user_id, total_cost)
    logging.info(
        "%s %s %s %s", task.uid, data.job.duration, total_cost, transcription_cost
    )

    task.task_status = "completed"
    await task.save_report("Task processed successfully")

    await conditions.Conditions().release_condition(task.uid)
    return task


async def process_error_webhook(task: TranscribeTask) -> TranscribeTask:
    speechmatic_task: speechmatics.JobDetails = (
        await speechmatics.Speechmatics().get_transcribe_job(task.transcription_job_id)
    )

    task.task_status = "error"
    for error in speechmatic_task.errors:
        await task.save_report(error.message, emit=False)
        logging.warning("Transcription rejected %s", error.message)

    await task.save_and_emit()
    await conditions.Conditions().release_condition(task.uid)
    return task
