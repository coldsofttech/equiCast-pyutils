import argparse

import boto3


def store_parameter_in_ssm(parameter_name: str, value: str, type_: str = "String", overwrite: bool = True,
                           region_name: str = None):
    client = boto3.client("ssm", region_name=region_name)

    try:
        response = client.put_parameter(
            Name=parameter_name,
            Value=value,
            Type=type_,
            Overwrite=overwrite
        )
        print(f"✅ Successfully stored {parameter_name} = {value}")
        return response
    except Exception as e:
        raise RuntimeError(f"Failed to store SSM parameter: {e}")


def main():
    parser = argparse.ArgumentParser(description="Store a parameter in AWS SSM Parameter Store")
    parser.add_argument("--name", required=True, help="SSM parameter name, e.g. /equicast/pyutils/version")
    parser.add_argument("--value", required=True, help="Value to store")
    parser.add_argument("--type", default="String", choices=["String", "StringList", "SecureString"],
                        help="Parameter type")
    parser.add_argument("--region", default="eu-west-1", help="AWS region")
    parser.add_argument("--no-overwrite", action="store_true", help="Do not overwrite if parameter exists")

    args = parser.parse_args()

    store_parameter_in_ssm(
        parameter_name=args.name,
        value=args.value,
        type_=args.type,
        overwrite=not args.no_overwrite,
        region_name=args.region
    )


if __name__ == "__main__":
    main()
