import unittest

from src.schema_contract import missing_required_columns, validate_required_columns


class RequiredColumnContractTests(unittest.TestCase):
    def test_accepts_schema_when_all_required_columns_exist(self):
        validate_required_columns(
            ["tpep_pickup_datetime", "trip_distance", "extra"],
            ["tpep_pickup_datetime", "trip_distance"],
        )

    def test_reports_all_missing_columns_in_stable_order(self):
        missing = missing_required_columns(
            ["VendorID"], ["trip_distance", "tpep_pickup_datetime", "trip_distance"]
        )
        self.assertEqual(missing, ["tpep_pickup_datetime", "trip_distance"])

    def test_fails_with_actionable_message_when_required_columns_are_missing(self):
        with self.assertRaisesRegex(
            ValueError,
            r"Source schema is missing required column\(s\): tpep_pickup_datetime, trip_distance",
        ):
            validate_required_columns(["VendorID"], ["trip_distance", "tpep_pickup_datetime"])

    def test_empty_required_contract_accepts_any_schema(self):
        validate_required_columns([], [])


if __name__ == "__main__":
    unittest.main()
