from modal.pars_modal import extract_events, parse_file
from pathlib import Path
import pandas as pd
from typing import Dict

def pandas_counter(pa):
    if "eventTypePerformed" in pa.columns:
        perfm_ev_count = pa["eventTypePerformed"].value_counts().to_frame()
    else:
        perfm_ev_count = pd.DataFrame()
    if "eventType" in pa.columns:
        ty_ev_count = pa["eventType"].value_counts().to_frame()
        unknown_events = pa["eventType"].fillna("unknown").value_counts().to_frame()
    else:
        ty_ev_count = pd.DataFrame()
        unknown_events = pd.DataFrame()
    res_coun_ev = pd.concat([perfm_ev_count, ty_ev_count, unknown_events], axis=1).fillna(0).astype(int)
    return res_coun_ev

def avareng_event_time(pa):
    if "durationMs" in pa.columns:
        _ = pa["durationMs"].mean()
    if "serverLatencyMs" in pa.columns:
        latency_avg = pa.groupby("eventType")["serverLatencyMs"].mean().sort_values(ascending=False)
        return latency_avg

def top_server_event(pa):
    if "callerIpAddress" in pa.columns:
        top_server = pa["callerIpAddress"].value_counts()
        return top_server
    else:
        return None

def events_duration(pa):
    if "time" in pa.columns:
        end_time = pd.to_datetime(pa["time"], utc=True)
    elif "eventTime" in pa.columns:
        end_time = pd.to_datetime(pa["eventTime"], utc=True)
    else:
        raise ValueError("Не найден столбец времени начала ('time' или 'eventTime').")

    if "time_global" in pa.columns:
        start_time = pd.to_datetime(pa["time_global"], utc=True)
    else:
        raise ValueError("Не найден столбец времени конца ('timestamp').")

    duration_seconds = (start_time - end_time).dt.total_seconds()

    return duration_seconds

def pandas_sorted_by_serv_or_acc(pa):
    results = {}
    if "account_id" in pa.columns:
        results["account_id"] = pa["account_id"].value_counts()
    if "callerIpAddress" in pa.columns:
        results["callerIpAddress"] = pa["callerIpAddress"].value_counts()
    if not results:
        return pd.DataFrame()
    if len(results) == 1:
        single_key = list(results.keys())[0]
        return next(iter(results.values())).to_frame()
    combined_df = pd.concat(results, axis=1).fillna(0).astype(int)
    return combined_df


def slow_or_faild_events(pa):
    duration_seconds = events_duration(pa)
    print(duration_seconds)
    mask_status = pa["statusCode"] != 200

    if "serverLatencyMs" in pa.columns:
        mask_latency = pa["serverLatencyMs"] > 50
    else:
        mask_latency = pd.Series([False] * len(pa), index=pa.index)
    combined_mask = mask_status | mask_latency

    if combined_mask.any():
        cols = ["eventType", "statusCode"]
        if "serverLatencyMs" in pa.columns:
            cols.append("serverLatencyMs")
        else:
            pa = pa.copy()
            pa["serverLatencyMs"] = 0
            cols.append("serverLatencyMs")

        slow_or_failed_df = pa.loc[combined_mask, cols].copy()
        slow_or_failed_df["duration_seconds"] = duration_seconds.loc[combined_mask]
        return slow_or_failed_df
    else:
        return pd.DataFrame()


def main():
    log_path = Path(r"D:\Program Files (x86)\PyProject\QA_TestAssignment\testLogs\changes_output.txt")
    pa = pd.DataFrame(parse_file(log_path))
    pandas_counter(pa)
    avareng_event_time(pa)
    top_server_event(pa).head()
    slow_or_faild_events(pa)

if __name__ == "__main__":
    main()
