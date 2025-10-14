import logging
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import TimestampType, StructType

log = logging.getLogger(__name__)

class GitHubEventParser:
    @staticmethod
    def parse(
            df: DataFrame,
            schema: StructType,
            field_mapping: Dict[str, str]
    ) -> DataFrame:
        """
        Parse Kafka JSON payloads into a typed DataFrame.

        :param df: input DataFrame with a string JSON column named 'value' - e.g. Kafka Stream
        :param schema: StructType describing the JSON structure
        :param field_mapping: dict mapping from JSON path to desired column name
        :return: flattened DataFrame with selected columns renamed
        """

        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))

        select_expressions = []
        for json_path, col_name in field_mapping.items():
            select_expressions.append(col(json_path).alias(col_name))

        return parsed_df.select(*select_expressions)



