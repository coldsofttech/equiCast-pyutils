import os
from pathlib import Path

RATE_PER_GB = 0.05  # USD
THRESHOLD = 1.0  # USD for coloring


def calculate_cost(total_bytes: int):
    total_gb = total_bytes / (1024 ** 3)
    cost = total_gb * RATE_PER_GB
    color = "red" if cost > THRESHOLD else "green"
    return total_gb, cost, color


def get_total_bytes(artifact_dir: Path):
    artifacts = list(artifact_dir.glob("*"))
    if not artifacts:
        raise FileNotFoundError("No artifacts found")
    return sum(f.stat().st_size for f in artifacts)


def write_github_outputs(total_gb: float, cost: float):
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"artifact_size_gb={total_gb:.6f}\n")
            f.write(f"estimate_cost={cost:.6f}\n")


def write_github_summary(total_gb: float, cost: float, color: str):
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("### 💰 CodeArtifact Cost Estimate\n")
            f.write("| Resource     | Storage Size (GB) | Estimated Monthly Cost |\n")
            f.write("|--------------|-------------------|------------------------|\n")
            f.write(f"| CodeArtifact | {total_gb:.6f} | <span style='color:{color}'>${cost:.6f}</span> |\n")


def main():
    artifact_dir = Path("dist")
    try:
        total_bytes = get_total_bytes(artifact_dir)
    except FileNotFoundError as e:
        print(e)
        exit(1)

    total_gb, cost, color = calculate_cost(total_bytes)

    print(f"Total Artifact Size (GB): {total_gb:.6f}")
    print(f"Estimated CodeArtifact Storage Cost: ${cost:.6f} ({color})")

    write_github_summary(total_gb, cost, color)
    write_github_outputs(total_gb, cost)


if __name__ == "__main__":
    main()
