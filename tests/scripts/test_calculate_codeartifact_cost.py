import pytest

from scripts.calculate_codeartifact_cost import calculate_cost, get_total_bytes, write_github_outputs, \
    write_github_summary


def test_calculate_cost_under_threshold():
    total_bytes = 10 * 1024 ** 3  # 10 GB
    total_gb, cost, color = calculate_cost(total_bytes)
    assert round(total_gb, 6) == 10.0
    assert round(cost, 6) == 0.5  # 10 GB * 0.05 USD
    assert color == "green"


def test_calculate_cost_over_threshold():
    total_bytes = 30 * 1024 ** 3  # 30 GB
    total_gb, cost, color = calculate_cost(total_bytes)
    assert round(total_gb, 6) == 30.0
    assert round(cost, 6) == 1.5
    assert color == "red"


def test_get_total_bytes(tmp_path):
    f1 = tmp_path / "a.txt"
    f1.write_bytes(b"x" * 1024)  # 1 KB
    f2 = tmp_path / "b.txt"
    f2.write_bytes(b"x" * 2048)  # 2 KB

    total_bytes = get_total_bytes(tmp_path)
    assert total_bytes == 1024 + 2048


def test_get_total_bytes_no_files(tmp_path):
    with pytest.raises(FileNotFoundError):
        get_total_bytes(tmp_path)


def test_write_github_outputs(tmp_path, monkeypatch):
    output_file = tmp_path / "output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_file))

    write_github_outputs(1.234567, 0.061728)
    content = output_file.read_text(encoding="utf-8")
    assert "artifact_size_gb=1.234567" in content
    assert "estimate_cost=0.061728" in content


def test_write_github_summary(tmp_path, monkeypatch):
    summary_file = tmp_path / "summary.txt"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_file))

    write_github_summary(1.234567, 0.061728, "green")
    content = summary_file.read_text(encoding="utf-8")
    assert "### 💰 CodeArtifact Cost Estimate" in content
    assert "| CodeArtifact | 1.234567 | <span style='color:green'>$0.061728</span> |" in content
