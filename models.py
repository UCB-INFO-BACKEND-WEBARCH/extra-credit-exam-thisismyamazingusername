"""SQLAlchemy models — student stub.

Define at minimum:
  - Job(id, status, current_stage, failed_stage?, error?, created_at, updated_at)
  - TopWord(job_id, word, count)

You may add more columns/tables as needed.
"""

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
import uuid

db = SQLAlchemy()


def _new_job_id():
  return str(uuid.uuid4())


class Job(db.Model):
  __tablename__ = "jobs"

  id = db.Column(db.String(36), primary_key=True, default=_new_job_id)
  status = db.Column(db.String(32), nullable=False, default="pending")
  current_stage = db.Column(db.Integer, nullable=False, default=1)
  failed_stage = db.Column(db.Integer, nullable=True)
  error = db.Column(db.Text, nullable=True)
  created_at = db.Column(db.DateTime(timezone=True), server_default=func.now(), nullable=False)
  updated_at = db.Column(
    db.DateTime(timezone=True),
    server_default=func.now(),
    onupdate=func.now(),
    nullable=False,
  )


class TopWord(db.Model):
  __tablename__ = "top_words"

  id = db.Column(db.Integer, primary_key=True, autoincrement=True)
  job_id = db.Column(db.String(36), db.ForeignKey("jobs.id"), nullable=False, index=True)
  word = db.Column(db.Text, nullable=False)
  count = db.Column(db.Integer, nullable=False)
