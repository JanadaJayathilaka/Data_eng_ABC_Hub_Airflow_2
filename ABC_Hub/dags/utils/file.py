from pathlib import Path


def read_sql(sql_file: str | Path) -> str:
    with open(sql_file, "r", encoding="utf-8") as file:
        return file.read()