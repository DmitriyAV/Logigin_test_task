from pathlib import Path
from typing import Dict
import pandas as pd
import matplotlib.pyplot as plt
from dto.main import pandas_counter_event, pandas_sorted_by_serv_or_acc, events_duration
from modal.pars_modal import parse_file

def plot_event_counts(counts, download_path):
    ax = counts.plot(kind="bar", figsize=(10, 6))
    ax.set_xlabel("Event Type")
    ax.set_ylabel("Count")
    ax.set_title("Bar Chart: Count of Events by Type")
    plt.xticks(rotation=45)
    plt.tight_layout()
    if download_path:
        plt.savefig(download_path)
    plt.show()


def plot_pie_charts(df, download_path):
    fig, axs = plt.subplots(1, 2, figsize=(16, 8))
    if "account_id" in df.columns:
        counts_account = df["account_id"]
        axs[0].pie(counts_account, labels=counts_account.index, autopct='%1.1f%%', startangle=140)
        axs[0].set_title("Events by Account ID")
    else:
        axs[0].text(0.5, 0.5, "No data for account_id", ha="center", va="center")
        axs[0].set_title("Events by Account ID")
    if "callerIpAddress" in df.columns:
        counts_ip = df["callerIpAddress"]
        axs[1].pie(counts_ip, labels=counts_ip.index, autopct='%1.1f%%', startangle=140)
        axs[1].set_title("Events by Caller IP Address")
    else:
        axs[1].text(0.5, 0.5, "No data for callerIpAddress", ha="center", va="center")
        axs[1].set_title("Events by Caller IP Address")
    if download_path:
        plt.savefig(download_path)
    plt.show()


import matplotlib.pyplot as plt


def plot_latency_duration_line_chart(durat, download_path):
    durat_list = list(durat)

    plt.figure(figsize=(10, 6))
    plt.plot(durat_list, marker='o', linestyle='-', label='Latency (ms)')
    plt.xlabel("Time")
    plt.ylabel("Duration (ms)")
    plt.title("Latency/Duration Over Time")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    if download_path:
        plt.savefig(download_path)
    plt.show()


def main() -> None:
    log_path = Path(r"D:\Program Files (x86)\PyProject\QA_TestAssignment\testLogs\changes_output.txt")
    pa = pd.DataFrame(parse_file(log_path))
    event_counts = pandas_counter_event(pa)
    serverIP_ID = pandas_sorted_by_serv_or_acc(pa)
    durations = events_duration(pa)
    download_path_bar = "bar_chart.png"
    download_path_pie = "pie_chart.png"
    download_path_line = "line_chart.png"

    plot_event_counts(event_counts, download_path=download_path_bar)
    plot_pie_charts(serverIP_ID, download_path=download_path_pie)
    plot_latency_duration_line_chart(durations, download_path=download_path_line)

if __name__ == "__main__":
    main()
