import os
from pathlib import Path

RATE_PER_GB = 0.05  # USD
THRESHOLD = 1.0  # USD for coloring

artifact_dir = Path("dist")
artifacts = list(artifact_dir.glob("*"))

if not artifacts:
    print("No artifacts found in dist/")
    exit(1)

total_bytes = sum(f.stat().st_size for f in artifacts)
total_gb = total_bytes / (1024 ** 3)
cost = total_gb * RATE_PER_GB
color = "red" if cost > THRESHOLD else "green"

print(f"Total Artifact Size (GB): {total_gb:.6f}")
print(f"Estimated CodeArtifact Storage Cost: ${cost:.6f} ({color})")

summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
if summary_path:
    with open(summary_path, "a") as f:
        f.write("### 💰 CodeArtifact Cost Estimate\n")
        f.write("| Resource     | Storage Size (GB) | Estimated Monthly Cost |\n")
        f.write("|--------------|-------------------|------------------------|\n")
        f.write(f"| CodeArtifact | {total_gb:.6f} | <span style='color:{color}'>${cost:.6f}</span> |\n")

print(f"::set-output name=artifact_size_gb::{total_gb:.6f}")
print(f"::set-output name=estimate_cost::{cost:.6f}")
