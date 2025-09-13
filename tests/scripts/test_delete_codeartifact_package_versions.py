from unittest.mock import MagicMock, patch

import pytest

from scripts.delete_codeartifact_package_versions import delete_codeartifact_package_versions


def test_delete_codeartifact_no_versions():
    mock_client = MagicMock()
    mock_client.list_package_versions.return_value = {"versions": []}

    with patch("boto3.client", return_value=mock_client):
        delete_codeartifact_package_versions(
            package_name="mypackage",
            domain="mydomain",
            repository="myrepo",
            owner="123456789012",
        )

    mock_client.list_package_versions.assert_called_once_with(
        domain="mydomain",
        domainOwner="123456789012",
        repository="myrepo",
        format="pypi",
        package="mypackage",
    )
    mock_client.delete_package_versions.assert_not_called()


def test_delete_codeartifact_with_versions():
    mock_client = MagicMock()
    mock_client.list_package_versions.return_value = {
        "versions": [{"version": "1.0.0"}, {"version": "1.1.0"}]
    }

    with patch("boto3.client", return_value=mock_client):
        delete_codeartifact_package_versions(
            package_name="mypackage",
            domain="mydomain",
            repository="myrepo",
            owner="123456789012",
        )

    mock_client.list_package_versions.assert_called_once()
    # expected_calls = [
    #     (({"domain": "mydomain",
    #        "domainOwner": "123456789012",
    #        "repository": "myrepo",
    #        "format": "pypi",
    #        "package": "mypackage",
    #        "versions": ["1.0.0"]},)),
    #     (({"domain": "mydomain",
    #        "domainOwner": "123456789012",
    #        "repository": "myrepo",
    #        "format": "pypi",
    #        "package": "mypackage",
    #        "versions": ["1.1.0"]},)),
    # ]
    assert mock_client.delete_package_versions.call_count == 2
    called_versions = [call.kwargs["versions"][0] for call in mock_client.delete_package_versions.call_args_list]
    assert called_versions == ["1.0.0", "1.1.0"]


def test_delete_codeartifact_client_error():
    mock_client = MagicMock()
    mock_client.list_package_versions.side_effect = Exception("AWS error")

    with patch("boto3.client", return_value=mock_client):
        with pytest.raises(RuntimeError, match="Failed to delete CodeArtifact Package versions: AWS error"):
            delete_codeartifact_package_versions(
                package_name="mypackage",
                domain="mydomain",
                repository="myrepo",
                owner="123456789012",
            )
