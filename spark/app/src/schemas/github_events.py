from pyspark.sql.types import StructType, StringType, StructField

github_event_schema = StructType([
    StructField("type", StringType()),
    StructField("repo", StructType([
        StructField("name", StringType())
    ])),
    StructField("created_at", StringType())
])

github_event_field_map = {
    "data.repo.name": "repo_name",
    "data.type": "type",
    "data.created_at": "created_at"
}