import argparse

import boto3


def delete_codeartifact_package_versions(package_name: str, domain: str, repository: str, owner: str,
                                         fmt: str = "pypi"):
    client = boto3.client("codeartifact")

    try:
        response = client.list_package_versions(
            domain=domain,
            domainOwner=owner,
            repository=repository,
            format=fmt,
            package=package_name,
        )
        versions = [v["version"] for v in response.get("versions", [])]

        if not versions:
            print(f"No existing versions found for package '{package_name}'.")
            return

        print(f"Deleting existing versions for '{package_name}': {versions}")

        for version in versions:
            client.delete_package_versions(
                domain=domain,
                domainOwner=owner,
                repository=repository,
                format=fmt,
                package=package_name,
                versions=[version]
            )
            print(f"Deleted version: {version}")
    except Exception as e:
        raise RuntimeError(f"Failed to delete CodeArtifact Package versions: {e}")


def main():
    parser = argparse.ArgumentParser(description="Delete all versions of a package from AWS CodeArtifact")
    parser.add_argument("--domain", required=True, help="CodeArtifact domain")
    parser.add_argument("--domain-owner", required=True, help="AWS account ID of domain owner")
    parser.add_argument("--repository", required=True, help="CodeArtifact repository")
    parser.add_argument("--package-name", required=True, help="Package name")
    parser.add_argument("--format", default="pypi", help="Package format (default: pypi)")

    args = parser.parse_args()

    delete_codeartifact_package_versions(
        package_name=args.package_name,
        domain=args.domain,
        repository=args.repository,
        owner=args.domain_owner,
        fmt=args.format,
    )


if __name__ == "__main__":
    main()
