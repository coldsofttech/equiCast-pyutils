import argparse
import os

import boto3


def get_codeartifact_auth_token(domain: str, domain_owner: str) -> str:
    client = boto3.client("codeartifact")
    try:
        response = client.get_authorization_token(
            domain=domain,
            domainOwner=domain_owner
        )
        return response["authorizationToken"]
    except Exception as e:
        raise RuntimeError(f"Failed to get CodeArtifact token: {e}")


def get_codeartifact_endpoint(domain: str, domain_owner: str, repository: str, fmt: str = "pypi") -> str:
    client = boto3.client("codeartifact")
    try:
        response = client.get_repository_endpoint(
            domain=domain,
            domainOwner=domain_owner,
            repository=repository,
            format=fmt
        )
        return response["repositoryEndpoint"]
    except Exception as e:
        raise RuntimeError(f"Failed to get CodeArtifact endpoint: {e}")


def write_github_output(**kwargs):
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            for key, value in kwargs.items():
                f.write(f"{key}={value}\n")


def setup_codeartifact_auth(domain: str, domain_owner: str, repository: str):
    token = get_codeartifact_auth_token(domain, domain_owner)
    endpoint = get_codeartifact_endpoint(domain, domain_owner, repository)
    write_github_output(token=token, endpoint=endpoint)
    return token, endpoint


def main():
    parser = argparse.ArgumentParser(description="Fetch CodeArtifact token and endpoint.")
    parser.add_argument("--domain", required=True, help="CodeArtifact domain name")
    parser.add_argument("--domain-owner", required=True, help="CodeArtifact domain owner AWS account ID")
    parser.add_argument("--repository", required=True, help="CodeArtifact repository name")
    args = parser.parse_args()

    token, endpoint = setup_codeartifact_auth(
        domain=args.domain,
        domain_owner=args.domain_owner,
        repository=args.repository
    )


if __name__ == "__main__":
    main()
