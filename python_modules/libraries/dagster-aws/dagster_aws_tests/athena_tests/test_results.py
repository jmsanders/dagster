import pytest

from botocore.stub import Stubber, ANY
from datetime import date, datetime
from dagster_aws.athena.results import AthenaResults


@pytest.mark.parametrize(
    ("athena_type", "athena_value", "expected"),
    [
        ("boolean", "true", True),
        ("tinyint", "1", 1),
        ("smallint", "1", 1),
        ("int", "1", 1),
        ("integer", "1", 1),
        ("bigint", "1", 1),
        ("double", "1.0", 1.0),
        ("real", "1.0", 1.0),
        ("decimal", "1.0", 1.0),
        ("decimal", "1.00", 1.0),
        ("decimal", "1.00", 1.0),
        ("char", "hello", "hello"),
        ("varchar", "hello", "hello"),
        ("string", "hello", "hello"),
        ("date", "2001-01-01", date(2001, 1, 1)),
        ("timestamp", "2001-01-01 00:00:00.001", datetime(2001, 1, 1, 0, 0, 0, 1000)),
    ],
)
def test_fetchall(mock_athena_client, athena_type, athena_value, expected):
    stubber = Stubber(mock_athena_client)
    stubber.activate()
    stubber.add_response(
        method="get_query_results",
        service_response={
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "column_name",}],},
                    {"Data": [{"VarCharValue": athena_value,}],},
                ],
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "column_name", "Type": athena_type,}]
                },
            }
        },
        expected_params={"QueryExecutionId": ANY},
    )

    results = AthenaResults(client=mock_athena_client, execution_id="an execution id")
    assert results.fetchall() == [(expected,)]


@pytest.mark.parametrize(
    ("athena_type",),
    [
        ("boolean",),
        ("tinyint",),
        ("smallint",),
        ("int",),
        ("integer",),
        ("bigint",),
        ("double",),
        ("real",),
        ("decimal",),
        ("char",),
        ("varchar",),
        ("string",),
        ("date",),
        ("timestamp",),
    ],
)
def test_fetchall_null(mock_athena_client, athena_type):
    stubber = Stubber(mock_athena_client)
    stubber.activate()
    stubber.add_response(
        method="get_query_results",
        service_response={
            "ResultSet": {
                "Rows": [{"Data": [{"VarCharValue": "column_name",}],}, {"Data": [{}],},],
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "column_name", "Type": athena_type,}]
                },
            }
        },
        expected_params={"QueryExecutionId": ANY},
    )

    results = AthenaResults(client=mock_athena_client, execution_id="an execution id")
    assert results.fetchall() == [(None,)]


def test_fetchall_paginated(mock_athena_client):
    stubber = Stubber(mock_athena_client)
    stubber.activate()
    stubber.add_response(
        method="get_query_results",
        service_response={
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "column_name",}],},
                    {"Data": [{"VarCharValue": "1",}],},
                    {"Data": [{"VarCharValue": "2",}],},
                ],
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "column_name", "Type": "integer",}]},
            },
            "NextToken": "page2",
        },
        expected_params={"QueryExecutionId": ANY},
    )
    stubber.add_response(
        method="get_query_results",
        service_response={
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "column_name",}],},
                    {"Data": [{"VarCharValue": "3",}],},
                ],
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "column_name", "Type": "integer",}]},
            },
        },
        expected_params={"QueryExecutionId": ANY, "NextToken": "page2"},
    )

    results = AthenaResults(client=mock_athena_client, execution_id="an execution id")
    assert results.fetchall() == [(1,), (2,), (3,)]
