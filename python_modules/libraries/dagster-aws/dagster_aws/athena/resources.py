import csv
import io
import os
import uuid

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

import boto3
from botocore.stub import Stubber


class AthenaError(Exception):
    pass


class AthenaTimeout(AthenaError):
    pass


class AthenaResource:
    def __init__(self, client, workgroup_name="primary", retry_interval=5, max_retries=120):
        self.client = client
        self.workgroup_name = workgroup_name
        self.max_retries = max_retries
        self.retry_interval = retry_interval

    def execute_query(self, query_string, fetch_results=False):
        execution_id = self.client.start_query_execution(
            QueryString=query_string, WorkGroup=self.workgroup_name
        )["QueryExecutionId"]
        self._poll(execution_id)
        if fetch_results:
            return self._results(execution_id)

    def _poll(self, execution_id):
        retries = self.max_retries
        state = "QUEUED"

        while retries >= 0 and state in ["QUEUED", "RUNNING"]:
            execution = self.client.get_query_execution(QueryExecutionId=execution_id)[
                "QueryExecution"
            ]
            state = execution["Status"]["State"]
            if self.max_retries > 0:
                retries -= 1

        if retries < 0:
            raise AthenaTimeout()

        if state != "SUCCEEDED":
            raise AthenaError(execution["Status"]["StateChangeReason"])

    def _results(self, execution_id):
        execution = self.client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
        s3 = boto3.resource("s3")
        output_location = execution["ResultConfiguration"]["OutputLocation"]
        bucket = urlparse(output_location).netloc
        prefix = urlparse(output_location).path.lstrip("/")

        results = []
        rows = s3.Bucket(bucket).Object(prefix).get()["Body"].read().decode().splitlines()
        reader = csv.reader(rows)
        for row in reader:
            results.append(tuple(row))

        return results


class FakeAthenaResource(AthenaResource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.retry_interval = 0
        self.stubber = Stubber(self.client)

        s3 = boto3.resource("s3")
        self.bucket = s3.Bucket("fake-athena-results-bucket")
        self.bucket.create()

    def execute_query(
        self, query_string, fetch_results=False, expected_states=None, expected_results=None
    ):  # pylint: disable=arguments-differ
        if not expected_states:
            expected_states = ["QUEUED", "RUNNING", "SUCCEEDED"]
        if not expected_results:
            expected_results = [("1",)]

        self.stubber.activate()

        execution_id = str(uuid.uuid4())
        self._stub_start_query_execution(execution_id, query_string)
        self._stub_get_query_execution(execution_id, expected_states)
        if expected_states[-1] == "SUCCEEDED" and fetch_results:
            self._fake_results(execution_id, expected_results)

        result = super().execute_query(query_string, fetch_results=fetch_results)

        self.stubber.deactivate()
        self.stubber.assert_no_pending_responses()

        return result

    def _stub_start_query_execution(self, execution_id, query_string):
        self.stubber.add_response(
            method="start_query_execution",
            service_response={"QueryExecutionId": execution_id},
            expected_params={"QueryString": query_string, "WorkGroup": self.workgroup_name},
        )

    def _stub_get_query_execution(self, execution_id, states):
        for state in states:
            self.stubber.add_response(
                method="get_query_execution",
                service_response={
                    "QueryExecution": {
                        "Status": {"State": state, "StateChangeReason": "state change reason"},
                    }
                },
                expected_params={"QueryExecutionId": execution_id},
            )

    def _fake_results(self, execution_id, expected_results):
        with io.StringIO() as results:
            writer = csv.writer(results)
            for row in expected_results:
                # Athena writes all non-null columns as strings in its CSV output
                stringified = tuple([str(item) for item in row if item])
                writer.writerow(stringified)
            results.seek(0)

            self.bucket.Object(execution_id + ".csv").put(Body=results.read())

        self.stubber.add_response(
            method="get_query_execution",
            service_response={
                "QueryExecution": {
                    "ResultConfiguration": {
                        "OutputLocation": os.path.join(
                            "s3://", self.bucket.name, execution_id + ".csv"
                        )
                    }
                }
            },
            expected_params={"QueryExecutionId": execution_id},
        )
