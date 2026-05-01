"""Pipeline service — student stub.

You are responsible for implementing every endpoint in the spec.
The starter only sets up an empty Flask app and the SQLAlchemy binding.
"""

import os
import uuid

from flask import Flask, jsonify, request
from redis import Redis
from sqlalchemy import text

from models import Job, db
from worker import enqueue_stage1


def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)

    with app.app_context():
        db.create_all()

    @app.post("/jobs")
    def create_job():
        payload = request.get_json(silent=True) or {}
        text_payload = payload.get("text")
        if not isinstance(text_payload, str):
            return jsonify({"error": "text must be a string"}), 400

        job = Job(id=str(uuid.uuid4()), status="pending", current_stage=1)
        db.session.add(job)
        db.session.commit()

        enqueue_stage1(job.id, text_payload)
        return jsonify({"job_id": job.id}), 202

    @app.get("/jobs/<job_id>")
    def get_job(job_id):
        job = Job.query.get(job_id)
        if job is None:
            return jsonify({"error": "job not found"}), 404

        return jsonify(
            {
                "job_id": job.id,
                "status": job.status,
                "current_stage": job.current_stage,
                "failed_stage": job.failed_stage,
                "error": job.error,
            }
        )

    @app.get("/health")
    def health():
        db_ok = False
        redis_ok = False
        volume_writable = False

        try:
            db.session.execute(text("SELECT 1"))
            db_ok = True
        except Exception:
            db_ok = False

        try:
            redis_client = Redis.from_url(os.environ["REDIS_URL"])
            redis_ok = bool(redis_client.ping())
        except Exception:
            redis_ok = False

        health_file = f"/data/.health-{uuid.uuid4().hex}"
        try:
            with open(health_file, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(health_file)
            volume_writable = True
        except Exception:
            volume_writable = False

        status_ok = db_ok and redis_ok and volume_writable
        return (
            jsonify(
                {
                    "status": "ok" if status_ok else "degraded",
                    "db": "up" if db_ok else "down",
                    "redis": "up" if redis_ok else "down",
                    "volume_writable": volume_writable,
                }
            ),
            200,
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
