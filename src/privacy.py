from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, sha2, concat_ws


def anonymize_columns(df: DataFrame, columns: list[str], salt: str = "lgpd-demo-salt") -> DataFrame:
    """Aplica hash determinístico em atributos sensíveis.

    Objetivo: preservar joins analíticos sem expor PII em texto puro.
    """
    output_df = df
    for column_name in columns:
        if column_name in output_df.columns:
            # Governança em primeiro lugar: salt + hash para evitar exposição de valores em texto puro.
            output_df = output_df.withColumn(
                column_name,
                sha2(concat_ws("||", lit(salt), col(column_name).cast("string")), 256),
            )
    return output_df
