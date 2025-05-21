#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# ...
from __future__ import annotations

from typing import Any, Sequence
from functools import cached_property

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook
from airflow.utils.context import Context


class EmrServerlessJobFailureSensor(BaseSensorOperator):
    """
    Polls an EMR Serverless job run:
      - raises AirflowException immediately if the job state is in JOB_FAILURE_STATES
      - returns True (and succeeds) when the job state is in JOB_SUCCESS_STATES
      - otherwise keeps waiting/poking
    """

    template_fields: Sequence[str] = (
        "application_id",
        "job_run_id",
        "aws_conn_id",
        "region_name",
    )

    def __init__(
        self,
        *,
        application_id: str,
        job_run_id: str,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        poke_interval: int = 30,
        timeout: int = 60 * 60,
        **kwargs: Any,
    ) -> None:
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.application_id = application_id
        self.job_run_id = job_run_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def poke(self, context: Context) -> bool:
        # Fetch the current job state
        response = self.hook.conn.get_job_run(
            applicationId=self.application_id,
            jobRunId=self.job_run_id,
        )
        state = response["jobRun"]["state"]
        self.log.info("EMR Serverless job %s on app %s is in state %s",
                      self.job_run_id, self.application_id, state)

        # If the job failed or was cancelled, immediately fail the task
        if state in EmrServerlessHook.JOB_FAILURE_STATES:
            details = response["jobRun"].get("stateDetails")
            raise AirflowException(
                f"EMR Serverless job {self.job_run_id} failed (state={state}): {details}"
            )

        # If the job succeeded, mark this sensor as success
        if state in EmrServerlessHook.JOB_SUCCESS_STATES:
            return True

        # Otherwise, keep waiting
        return False

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook with the right connection/region."""
        return EmrServerlessHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
        )
