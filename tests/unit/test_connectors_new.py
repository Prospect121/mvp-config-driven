from datacore.connectors import excel_ms, sheets, sftp


def test_sftp_read_local_path(spark, tmp_path):
    path = tmp_path / "sample.csv"
    path.write_text("id,name\n1,Alice\n2,Bob", encoding="utf-8")

    df = sftp.read({"path": str(path), "format": "csv", "options": {"header": "true"}}, spark)
    assert df.count() == 2


def test_sheets_values_to_dataframe(spark):
    rows = [["id", "value"], ["1", "foo"], ["2", "bar"]]
    df = sheets.read({"values": rows[1:], "header": rows[0]}, spark)
    assert sorted(df.columns) == ["id", "value"]
    assert df.count() == 2


def test_excel_rows_to_dataframe(spark):
    df = excel_ms.read({"rows": [[1, 20]], "header": ["id", "score"]}, spark)
    assert df.count() == 1
