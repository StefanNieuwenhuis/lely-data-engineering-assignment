# import pytest
# from pyspark import Row
#
#
# from src.processors.github_event_stream_processor import GitHubEventStreamProcessor
# from src.schemas.github_events import github_event_schema, github_event_field_map
#
# def test_parse_events_basic(spark_session):
#     data = [
#         Row(value='{"repo": {"name": "apache/spark"}, "type": "PullRequestEvent", "created_at": "2025-10-14T08:00:38Z"}'),
#         Row(value='{"repo": {"name": "apache/kafka"}, "type": "IssuesEvent", "created_at": "2025-10-14T08:01:00Z"}')
#     ]
#
#     df = spark_session.createDataFrame(data)
#
#     result_df = GitHubEventStreamProcessor.parse_events(df, github_event_schema, github_event_field_map)
#     result = result_df.collect()
#
#     assert result_df.columns == ["repo_name", "type", "created_at"]
#     assert result[0]["repo_name"] == "apache/spark"
#     assert result[0]["type"] == "PullRequestEvent"
#     assert result[0]["created_at"] == "2025-10-14T08:00:38Z"
#
#     assert result[1]["repo_name"] == "apache/kafka"
#     assert result[1]["type"] == "IssuesEvent"
#     assert result[1]["created_at"] == "2025-10-14T08:01:00Z"