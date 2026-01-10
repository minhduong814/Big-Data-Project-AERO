import os
import tempfile
from flow.helpers import rename_files


def test_rename_files_creates_expected_name(tmp_path):
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    src = extracted / "On_Time_Reporting_Carrier_On_Time_Performance_abc.csv"
    src.write_text("a,b,c")

    new_path = rename_files(str(extracted), 1999, 1)
    assert new_path is not None
    assert os.path.basename(new_path) == "1999_01.csv"
    assert (extracted / "1999_01.csv").exists()


def test_rename_files_returns_none_if_no_match(tmp_path):
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    (extracted / "other.csv").write_text("x")

    result = rename_files(str(extracted), 2000, 2)
    assert result is None
