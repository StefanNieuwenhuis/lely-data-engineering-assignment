from typing import Any, Iterable

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout


class AveragePRIntervalProcessor:
    @staticmethod
    def update_state(key:Any, pdfs:Iterable[pd.DataFrame], state:GroupState)->Iterable[pd.DataFrame]:
        """
        Compute running average time between PRs per repo

        :param key: the current group identifier
        :param pdfs: an iterator of one or more pandas DataFrames, each containing new rows for a group in the current micro-batch
        :param state: state object for this group
        :return:
        """
        (repo_name,) = key
        # Retrieve or initialize state

        if state.exists:
            # Spark already has stored state for this key from previous micro-batches -
            # i.e. Spark already saw PR events for this repo and has stored values.
            last_ts, count, total_diff = state.get
        else:
            # new repository: initialize state
            last_ts, count, total_diff = None, 0, 0.0

        new_events = False

        # iterate over each DataFrame in the current micro-batch
        for pdf in pdfs:
            if pdf.empty:
                continue

            new_events = True
            # Ensure timestamps are sorted for correct diff calculation
            pdf = pdf.sort_values("created_at")

            for ts in pdf["created_at"]:
                if last_ts is not None:
                    diff = (ts - last_ts).total_seconds()
                    total_diff += diff
                    count += 1
                last_ts = ts

        # Update state
        state.update((last_ts, count, total_diff))

        # Compute running average
        avg_interval = total_diff / count if count > 0 else 0.0

        # Return updated result as pandas DataFrame
        if new_events or state.hasTimedOut:
            yield pd.DataFrame([{
                "repo_name": repo_name,
                "avg_interval_seconds": avg_interval,
                "pr_count": count,
                "last_pull_request_ts": last_ts,
                "updated_at": pd.Timestamp.utcnow()
            }])

    def run(self, df: DataFrame, state_schema, output_schema, timeout_conf: str = GroupStateTimeout.ProcessingTimeTimeout)-> DataFrame:
        return (
            df
            .filter(col("type") == "PullRequestEvent")
            .select(col("repo_name"), col("created_at"))
            .groupBy(col("repo_name"))
            .applyInPandasWithState(
                func=self.update_state,
                stateStructType=state_schema,
                outputStructType=output_schema,
                outputMode="update",
                timeoutConf=timeout_conf
            )
        )