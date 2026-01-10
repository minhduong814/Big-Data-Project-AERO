import os


def rename_files(extracted_dir, year, month):
    """Rename extracted CSV files to a standard year_month CSV name.
    This is pure logic and safe to unit-test.
    """
    for file_name in os.listdir(extracted_dir):
        if file_name.startswith("On_Time_Reporting_Carrier_On_Time_Performance"):
            old_file_path = os.path.join(extracted_dir, file_name)
            new_file_path = os.path.join(extracted_dir, f"{year}_{month:02d}.csv")
            os.rename(old_file_path, new_file_path)
            return new_file_path
    return None
