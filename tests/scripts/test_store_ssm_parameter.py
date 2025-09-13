import boto3
import pytest
from moto import mock_aws

from scripts.store_ssm_parameter import store_parameter_in_ssm

PARAM_NAME = "/test/param"
REGION = "us-east-1"


@mock_aws(config={"core": {"service_whitelist": ["ssm"]}})
def test_store_parameter_basic():
    client = boto3.client("ssm", region_name=REGION)

    response = store_parameter_in_ssm(PARAM_NAME, "v1.2.3", region_name=REGION)
    assert "Version" in response

    result = client.get_parameter(Name=PARAM_NAME)
    assert result["Parameter"]["Value"] == "v1.2.3"
    assert result["Parameter"]["Name"] == PARAM_NAME
    assert result["Parameter"]["Type"] == "String"


@mock_aws(config={"core": {"service_whitelist": ["ssm"]}})
def test_store_parameter_stringlist():
    client = boto3.client("ssm", region_name=REGION)
    value_list = "one,two,three"

    store_parameter_in_ssm(PARAM_NAME, value_list, type_="StringList", region_name=REGION)

    result = client.get_parameter(Name=PARAM_NAME)
    assert result["Parameter"]["Value"] == value_list
    assert result["Parameter"]["Type"] == "StringList"


@mock_aws(config={"core": {"service_whitelist": ["ssm"]}})
def test_store_parameter_securestring():
    client = boto3.client("ssm", region_name=REGION)
    secure_value = "secret123"

    store_parameter_in_ssm(PARAM_NAME, secure_value, type_="SecureString", region_name=REGION)

    result = client.get_parameter(Name=PARAM_NAME, WithDecryption=True)
    assert result["Parameter"]["Value"] == secure_value
    assert result["Parameter"]["Type"] == "SecureString"


@mock_aws(config={"core": {"service_whitelist": ["ssm"]}})
def test_store_parameter_overwrite():
    client = boto3.client("ssm", region_name=REGION)

    store_parameter_in_ssm(PARAM_NAME, "v1.0", region_name=REGION)
    result1 = client.get_parameter(Name=PARAM_NAME)
    assert result1["Parameter"]["Value"] == "v1.0"

    store_parameter_in_ssm(PARAM_NAME, "v2.0", region_name=REGION)
    result2 = client.get_parameter(Name=PARAM_NAME)
    assert result2["Parameter"]["Value"] == "v2.0"


@mock_aws(config={"core": {"service_whitelist": ["ssm"]}})
def test_store_parameter_no_overwrite_behavior():
    boto3.client("ssm", region_name=REGION)
    store_parameter_in_ssm(PARAM_NAME, "v1.0", region_name=REGION)

    with pytest.raises(Exception):
        store_parameter_in_ssm(PARAM_NAME, "v2.0", overwrite=False, region_name=REGION)


@mock_aws(config={"core": {"service_whitelist": ["ssm"]}})
def test_store_parameter_different_region():
    custom_region = "us-west-2"
    client = boto3.client("ssm", region_name=custom_region)

    store_parameter_in_ssm(PARAM_NAME, "region-test", region_name=custom_region)
    result = client.get_parameter(Name=PARAM_NAME)
    assert result["Parameter"]["Value"] == "region-test"
