from unittest.mock import patch

import pytest

from scripts.get_codeartifact_auth_token import get_codeartifact_auth_token, get_codeartifact_endpoint, \
    write_github_output, setup_codeartifact_auth


def test_get_codeartifact_auth_token_success():
    mock_token = "mock-token"
    with patch("boto3.client") as mock_client:
        instance = mock_client.return_value
        instance.get_authorization_token.return_value = {"authorizationToken": mock_token}

        token = get_codeartifact_auth_token("my-domain", "123456789012")
        assert token == mock_token
        instance.get_authorization_token.assert_called_once_with(
            domain="my-domain",
            domainOwner="123456789012"
        )


def test_get_codeartifact_auth_token_failure():
    with patch("boto3.client") as mock_client:
        instance = mock_client.return_value
        instance.get_authorization_token.side_effect = Exception("AWS error")

        with pytest.raises(RuntimeError, match="Failed to get CodeArtifact token"):
            get_codeartifact_auth_token("my-domain", "123456789012")


def test_get_codeartifact_endpoint_success():
    mock_endpoint = "https://example.com/pypi/"
    with patch("boto3.client") as mock_client:
        instance = mock_client.return_value
        instance.get_repository_endpoint.return_value = {"repositoryEndpoint": mock_endpoint}

        endpoint = get_codeartifact_endpoint("my-domain", "123456789012", "my-repo")
        assert endpoint == mock_endpoint
        instance.get_repository_endpoint.assert_called_once_with(
            domain="my-domain",
            domainOwner="123456789012",
            repository="my-repo",
            format="pypi"
        )


def test_get_codeartifact_endpoint_failure():
    with patch("boto3.client") as mock_client:
        instance = mock_client.return_value
        instance.get_repository_endpoint.side_effect = Exception("AWS error")

        with pytest.raises(RuntimeError, match="Failed to get CodeArtifact endpoint"):
            get_codeartifact_endpoint("my-domain", "123456789012", "my-repo")


def test_write_github_output(tmp_path, monkeypatch):
    output_file = tmp_path / "output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))

    write_github_output(token="abc123", endpoint="https://example.com")
    content = output_file.read_text(encoding="utf-8")
    assert "token=abc123" in content
    assert "endpoint=https://example.com" in content


def test_setup_codeartifact_auth(tmp_path, monkeypatch):
    mock_token = "token123"
    mock_endpoint = "https://endpoint.example.com"
    with patch("boto3.client") as mock_client:
        instance = mock_client.return_value
        instance.get_authorization_token.return_value = {"authorizationToken": mock_token}
        instance.get_repository_endpoint.return_value = {"repositoryEndpoint": mock_endpoint}

        output_file = tmp_path / "github_output.txt"
        monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))

        token, endpoint = setup_codeartifact_auth("domain", "owner", "repo")

        assert token == mock_token
        assert endpoint == mock_endpoint

        content = output_file.read_text(encoding="utf-8")
        assert "token=token123" in content
        assert "endpoint=https://endpoint.example.com" in content
