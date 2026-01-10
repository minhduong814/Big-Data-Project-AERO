import os
import tempfile
from flow.helpers import rename_files


def test_rename_files_happy_path(tmp_path):
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    src = extracted / "On_Time_Reporting_Carrier_On_Time_Performance_test.csv"
    src.write_text("1,2,3")

    result = rename_files(str(extracted), 2020, 12)
    assert result is not None
    assert os.path.basename(result) == "2020_12.csv"


def test_rename_files_no_match(tmp_path):
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    (extracted / "not_matching.csv").write_text("x")

    result = rename_files(str(extracted), 2020, 12)
    assert result is None
