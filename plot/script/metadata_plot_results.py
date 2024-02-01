import pandas as pd
import sys
import matplotlib.pyplot as plt
from datetime import timedelta
import seaborn as sn
from common import *

PLOT_COLORS = [
    "#B0E3E6",
    "#FAD7AC",
]

SERVERS_NUM = 10
CORES_NUM = 32
EXTENSION_ACTIVATED = ["true", "false"]


def normalize_times(servers):
    for i, row in servers.iterrows():
        diff = row["boot_time"] - row["provision_time"]
        if diff <= SCHEDULER_QUANTUM:
            servers.at[i, "boot_time"] = row["provision_time"]
            servers.at[i, "stop_time"] = row["stop_time"] - diff


def normalize_names(servers):
    servers.rename(index=lambda r: r.split("-", 1)[0], inplace=True)


def load_times_steady(path):
    print(f"Loading: {path} ...")
    servers = pd.read_parquet(
        path,
        columns=["server_id", "timestamp", "provision_time", "boot_time", "stop_time"],
    )
    print(f"Loaded: {path}")

    grouped_servers = servers.groupby("server_id")[
        ["provision_time", "boot_time"]
    ].min()

    grouped_servers["stop_time"] = servers.groupby("server_id")["stop_time"].max()

    servers = None  # To free memory.

    normalize_times(grouped_servers)
    normalize_names(grouped_servers)

    results = pd.DataFrame()

    results["response_time"] = (
        grouped_servers["stop_time"]
        - grouped_servers["boot_time"]
        + timedelta(seconds=1)
    ).apply(lambda x: timedelta(seconds=1) if x < timedelta(seconds=1) else x)

    results["waiting_time"] = (
        grouped_servers["boot_time"]
        - grouped_servers["provision_time"]
        + timedelta(seconds=1)
    ).apply(lambda x: timedelta(seconds=1) if x < timedelta(seconds=1) else x)

    return results


def legend_name_for_api(is_activated: bool):
    if is_activated == "true":
        return "metadata-aware"
    return "submit order"


def plot_results(
    storage_buffers,
    times,
    dataset: str,
):
    plt.rcParams.update(
        {
            "legend.title_fontsize": TEXT_FONTSIZE,
            "font.size": TEXT_FONTSIZE,
            "figure.figsize": (14 * cm, 7.2 * cm),
            "text.usetex": True,
            "font.family": "libertine",
        }
    )

    fig, axes = plt.subplots(1, 2)
    fig.subplots_adjust(bottom=0.15)

    storage_buffers.index = storage_buffers.index.map(
        lambda x: x.value / 3.6e12
    )  # To hours.
    i = 0
    for api_activated in EXTENSION_ACTIVATED:
        col = f"{legend_name_for_api(api_activated)}"
        color = PLOT_COLORS[i]
        marker = MARKERS[i]
        marker_size = MARKER_SIZE
        if marker == "*":
            marker_size *= 1.5

        ax = storage_buffers[col].plot.line(
            color=color,
            ax=axes[0],
            legend=False,
            linewidth=LINEWIDTH,
            marker=marker,
            markevery=MARKER_EVERY * 2.5,
            markersize=marker_size * 1.3,
        )

        i += 1

    ax.xaxis.offsetText.set_visible(False)
    ax.yaxis.offsetText.set_visible(False)
    ax.set_ylabel(
        r"\begin{center} Storage buffers"
        + r"\\("
        + r"x$10^{11}$"
        + ") [B]"
        + r"\end{center}",
        multialignment="center",
    )
    ax.set_xlabel(f"Timestamp [{TIME_UNIT_ACRONYM[TIME_UNIT]}]")

    storage_buffers_ax = ax

    _, df_time = times
    i = 0
    for api_activated in EXTENSION_ACTIVATED:
        col = f"{legend_name_for_api(api_activated)}"
        color = PLOT_COLORS[i]
        marker = MARKERS[i]
        marker_size = MARKER_SIZE
        if marker == "*":
            marker_size *= 1.5

        ax = sn.ecdfplot(
            df_time[[col]],
            log_scale=True,
            ax=axes[1],
            legend=False,
            palette=[color],
            linewidth=LINEWIDTH,
            marker=marker,
            markevery=MARKER_EVERY,
            markersize=marker_size,
        )

        i += 1

    ax.legend(
        title="Object access",
        labels=list(df_time.columns),
        handles=storage_buffers_ax.lines,
        loc="upper center",
        bbox_to_anchor=(-0.2, -0.3),
        fancybox=True,
        ncol=2,
        columnspacing=0.5,
        fontsize=TEXT_FONTSIZE,
        title_fontsize=TEXT_FONTSIZE,
    )

    ax.set_xlabel(f"Total time [{TIME_UNIT_ACRONYM[TIME_UNIT]}]")

    left, right = axes
    left.annotate(
        "",
        xy=(2.2e14 / 3.6e12, 1.9e11),
        xytext=(2.2e14 / 3.6e12, 2.6e11),
        arrowprops=dict(
            facecolor="#CDEB8B",
            shrink=0.05,
            width=LINEWIDTH * 3,
            headwidth=LINEWIDTH * 6 * 1.5,
            headlength=LINEWIDTH * 4 * 1.5,
        ),
    )
    left.text(
        2.25e14 / 3.6e12,
        1.7e11,
        r"\textbf{70GB\\buffer size\\(27\%)}",
        fontdict={"size": ARROW_FONTSIZE},
    )

    right.annotate(
        "",
        xy=(0.7e2, 0.5),
        xytext=(1e2, 0.5),
        arrowprops=dict(
            facecolor="#CDEB8B",
            shrink=0.05,
            width=LINEWIDTH * 3,
            headwidth=LINEWIDTH * 6 * 1.5,
            headlength=LINEWIDTH * 4 * 1.5,
        ),
    )
    right.text(1.45e2, 0.5, r"\textbf{26h\\(24\%)}", fontdict={"size": ARROW_FONTSIZE})

    left.annotate("     ", xy=(-5, 1.5e11), annotation_clip=False)
    plt.subplots_adjust(wspace=0.4)

    if SAVE_TO_DISK:
        path = f"{OUTPUT_DISK_PATH}/metadata-results-{dataset}.{OUTPUT_FORMAT}"
        plt.savefig(path, bbox_inches="tight")
        print(f"Figure saved to disk at: {path}")
#     plt.show()
    plt.close()


if __name__ == "__main__":
    dataset = sys.argv[1]
    agg_n = sys.argv[2] if len(sys.argv) > 2 else 1

    response_times = pd.DataFrame()
    waiting_times = pd.DataFrame()
    total_times = pd.DataFrame()
    storage_buffers = pd.DataFrame()

    for api_activated in EXTENSION_ACTIVATED:
        host = pd.read_parquet(
            f"{INPUT_DISK_PATH}/metadata/{dataset}/host/api={api_activated.lower()}-servers={SERVERS_NUM}-cores={CORES_NUM}/data.parquet",
            columns=["timestamp", "cpu_utilization", "host_id"],
        )
        results = load_times_steady(
            f"{INPUT_DISK_PATH}/metadata/{dataset}/server/api={api_activated}-servers={SERVERS_NUM}-cores={CORES_NUM}/data.parquet",
        )

        # times
        response_times[f"{legend_name_for_api(api_activated)}"] = results[
            "response_time"
        ].apply(lambda x: x.total_seconds() / TIME_UNITS_TO_INT[TIME_UNIT])
        waiting_times[f"{legend_name_for_api(api_activated)}"] = results[
            "waiting_time"
        ].apply(lambda x: x.total_seconds() / TIME_UNITS_TO_INT[TIME_UNIT])

        total_times[f"{legend_name_for_api(api_activated)}"] = (
            waiting_times[f"{legend_name_for_api(api_activated)}"]
            + response_times[f"{legend_name_for_api(api_activated)}"]
        )
        results = None

        # storage
        storage = pd.read_parquet(
            f"{INPUT_DISK_PATH}/metadata/{dataset}/storage/api={api_activated.lower()}-servers={SERVERS_NUM}-cores={CORES_NUM}/data.parquet",
            columns=["timestamp", "storage_server_buffer_size", "storage_server_id"],
        )

        storage = storage[storage["timestamp"] >= storage["timestamp"].quantile(q=0.02)]
        storage = storage[storage["timestamp"] <= storage["timestamp"].quantile(q=0.04)]
        storage_num = len(storage.storage_server_id.unique())

        grouped_storage = storage.groupby("timestamp")[
            ["storage_server_buffer_size"]
        ].sum()

        grouped_storage.storage_server_buffer_size = (
            grouped_storage.storage_server_buffer_size / storage_num
        ) * 0.95
        grouped_storage.rename(
            columns={
                "storage_server_buffer_size": f"{legend_name_for_api(api_activated)}"
            },
            inplace=True,
        )
        storage_buffers = pd.concat([storage_buffers, grouped_storage], axis=1)

    plot_results(storage_buffers, ("total time", total_times), dataset)
