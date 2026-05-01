import os
import json
import re
import time
from collections import Counter

from redis import Redis
from rq import Queue, Worker
from sqlalchemy import create_engine, text

QUEUE_NAME = "pipeline"

STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "but",
    "if",
    "then",
    "of",
    "in",
    "on",
    "at",
    "to",
    "for",
    "with",
    "by",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "this",
    "that",
}


def _redis_conn():
    return Redis.from_url(os.environ["REDIS_URL"])


def _queue():
    return Queue(QUEUE_NAME, connection=_redis_conn())


def enqueue_stage1(job_id, input_text):
    _queue().enqueue(stage1, job_id, input_text)


def _db_engine():
    return create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)


def _set_stage_running(job_id, stage):
    engine = _db_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE jobs
                SET status = 'running', current_stage = :stage, failed_stage = NULL, error = NULL, updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id, "stage": stage},
        )


def _mark_failed(job_id, failed_stage, error_message):
    engine = _db_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE jobs
                SET status = 'failed', failed_stage = :failed_stage, error = :error, updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id, "failed_stage": failed_stage, "error": str(error_message)},
        )


def _mark_completed(job_id):
    engine = _db_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE jobs
                SET status = 'completed', current_stage = 5, failed_stage = NULL, error = NULL, updated_at = NOW()
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id},
        )


def _job_dir(job_id):
    path = os.path.join("/data", str(job_id))
    os.makedirs(path, exist_ok=True)
    return path


def _read_text(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _write_text(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path, content):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(content, f)


def stage1(job_id, input_text):
    try:
        _set_stage_running(job_id, 1)
        time.sleep(0.15)
        base = _job_dir(job_id)
        _write_text(os.path.join(base, "stage1.txt"), input_text)
        _queue().enqueue(stage2, job_id)
    except Exception as exc:
        _mark_failed(job_id, 1, exc)


def stage2(job_id):
    try:
        _set_stage_running(job_id, 2)
        time.sleep(0.15)
        base = _job_dir(job_id)
        input_text = _read_text(os.path.join(base, "stage1.txt"))
        _write_text(os.path.join(base, "stage2.txt"), input_text.lower())
        _queue().enqueue(stage3, job_id)
    except Exception as exc:
        _mark_failed(job_id, 2, exc)


def stage3(job_id):
    try:
        _set_stage_running(job_id, 3)
        time.sleep(0.15)
        base = _job_dir(job_id)
        input_text = _read_text(os.path.join(base, "stage2.txt"))
        tokens = re.findall(r"\b\w+\b", input_text)
        _write_json(os.path.join(base, "stage3.json"), tokens)
        _queue().enqueue(stage4, job_id)
    except Exception as exc:
        _mark_failed(job_id, 3, exc)


def stage4(job_id):
    try:
        _set_stage_running(job_id, 4)
        time.sleep(0.15)
        base = _job_dir(job_id)
        tokens = _read_json(os.path.join(base, "stage3.json"))
        filtered = [token for token in tokens if token not in STOPWORDS]
        _write_json(os.path.join(base, "stage4.json"), filtered)
        _queue().enqueue(stage5, job_id)
    except Exception as exc:
        _mark_failed(job_id, 4, exc)


def stage5(job_id):
    try:
        _set_stage_running(job_id, 5)
        time.sleep(0.15)
        base = _job_dir(job_id)
        tokens = _read_json(os.path.join(base, "stage4.json"))
        if not tokens:
            raise ValueError("no tokens available after stopword removal")

        counts = Counter(tokens)
        _write_json(os.path.join(base, "stage5.json"), dict(counts))

        top5 = counts.most_common(5)
        engine = _db_engine()
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM top_words WHERE job_id = :job_id"), {"job_id": job_id})
            for word, count in top5:
                conn.execute(
                    text(
                        """
                        INSERT INTO top_words (job_id, word, count)
                        VALUES (:job_id, :word, :count)
                        """
                    ),
                    {"job_id": job_id, "word": word, "count": int(count)},
                )

        _mark_completed(job_id)
    except Exception as exc:
        _mark_failed(job_id, 5, exc)

if __name__ == "__main__":
    redis_conn = _redis_conn()
    Worker([QUEUE_NAME], connection=redis_conn).work()
