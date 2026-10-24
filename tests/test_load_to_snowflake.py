import csv
import tempfile
import unittest
from pathlib import Path

from ingestion.load_to_snowflake import csv_columns, load_csvs, normalize_identifier


class RecordingCursor:
    def __init__(self, described_columns=None):
        self.statements = []
        self.described_columns = described_columns or []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchall(self):
        return [(column,) for column in self.described_columns]


class LoadToSnowflakeTests(unittest.TestCase):
    def test_normalizes_identifiers(self):
        self.assertEqual(normalize_identifier("order-items"), "ORDER_ITEMS")
        self.assertEqual(normalize_identifier("2024 sales"), "T_2024_SALES")
        self.assertEqual(normalize_identifier(""), "COLUMN")

    def test_reads_and_deduplicates_csv_headers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "orders.csv"
            path.write_text("Order ID,Order-ID,,\n1,1,empty,blank\n", encoding="utf-8")

            self.assertEqual(
                csv_columns(path),
                ["ORDER_ID", "ORDER_ID_2", "COLUMN_3", "COLUMN_4"],
            )

    def test_load_creates_raw_table_and_copies_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "orders.csv"
            with path.open("w", encoding="utf-8", newline="") as output:
                writer = csv.writer(output)
                writer.writerow(["order_id", "amount"])
                writer.writerow(["1", "19.95"])
            cursor = RecordingCursor(["ORDER_ID", "AMOUNT"])

            tables = load_csvs(Path(directory), cursor, "RETAILFLOW", "RAW")

            self.assertEqual(tables, ["ORDERS"])
            statements = "\n".join(cursor.statements)
            self.assertIn('CREATE TABLE IF NOT EXISTS "RETAILFLOW"."RAW"."ORDERS"', statements)
            self.assertIn('"ORDER_ID" VARCHAR, "AMOUNT" VARCHAR', statements)
            self.assertIn("COPY INTO", statements)
            self.assertIn("FILES=('orders.csv.gz')", statements)

    def test_rejects_header_change_for_existing_table(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "orders.csv"
            path.write_text("order_id,amount\n1,19.95\n", encoding="utf-8")
            cursor = RecordingCursor(["ORDER_ID", "TOTAL"])

            with self.assertRaisesRegex(ValueError, "does not match existing"):
                load_csvs(Path(directory), cursor, "RETAILFLOW", "RAW")

    def test_requires_input_files(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(FileNotFoundError, "No CSV files"):
                load_csvs(Path(directory), RecordingCursor(), "RETAILFLOW", "RAW")


if __name__ == "__main__":
    unittest.main()
