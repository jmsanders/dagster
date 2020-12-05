import boto3
import pytest
from moto import mock_athena


@pytest.fixture
def mock_athena_client(mock_s3_resource):  # pylint: disable=unused-argument
    with mock_athena():
        yield boto3.client("athena", region_name="us-east-1")
