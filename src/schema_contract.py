"""Small, Spark-independent helpers for validating source schemas."""

from collections.abc import Iterable


def missing_required_columns(columns: Iterable[str], required_columns: Iterable[str]) -> list[str]:
    """Return required column names that are absent from an incoming schema."""
    available = set(columns)
    return sorted(set(required_columns) - available)


def validate_required_columns(columns: Iterable[str], required_columns: Iterable[str]) -> None:
    """Fail fast when an incoming schema does not satisfy its required-column contract."""
    missing = missing_required_columns(columns, required_columns)
    if missing:
        raise ValueError(
            "Source schema is missing required column(s): " + ", ".join(missing)
        )
