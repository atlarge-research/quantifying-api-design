import pandas as pd
import sys
import matplotlib.pyplot as plt
from datetime import timedelta
from common import *


EXTENSION_ACTIVATED = ["true", "false"]
RATIOS = ["3.0", "4.0", "5.0"]
CLUSTER_TYPES = ["equal"]


plot_colors = [
    "#B0E3E6",
    "#659FA6",
    "#FAD7AC",
    "#C9945C",
    "#AECFA8",
    "#9ABE90",
]


def legend_name_for_api(is_activated: bool):
    if is_activated == "true":
        return "container"
    return "vm"


def normalize_times(servers):
    for i, row in servers.iterrows():
        diff = row["boot_time"] - row["provision_time"]
        if diff <= SCHEDULER_QUANTUM:
            servers.at[i, "boot_time"] = row["provision_time"]
            servers.at[i, "stop_time"] = row["stop_time"] - diff


def normalize_names(servers):
    servers.rename(index=lambda r: r.split("-", 1)[0], inplace=True)


def get_steady_timestamp(dataset):
    meta = pd.read_parquet(
        f"{TRACES_DISK_PATH}/{dataset}/meta.parquet",
        columns=["start_time", "stop_time"],
    )
    steady = (
        meta["start_time"].max()
        + 0.05 * (meta["stop_time"] - meta["start_time"]).mean()
    ) - meta["start_time"].min()
    return steady


def load_times_steady(path, dataset):
    steady = get_steady_timestamp(dataset)
    results = pd.DataFrame()
    print(f"Loading: {path} ...")
    server = pd.read_parquet(
        path,
        columns=["server_id", "provision_time", "boot_time", "stop_time"],
    )
    print(f"Loaded: {path}")

    grouped_servers = server.groupby("server_id")[["provision_time", "boot_time"]].min()

    grouped_servers["stop_time"] = server.groupby("server_id")["stop_time"].max()

    server = None

    grouped_servers = grouped_servers[
        grouped_servers["stop_time"] <= grouped_servers["provision_time"] + steady
    ]

    normalize_times(grouped_servers)
    normalize_names(grouped_servers)

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


def plot_results(
    packing,
    times,
    dataset: str,
):
    plt.rcParams.update(
        {
            "legend.title_fontsize": TEXT_FONTSIZE,
            "font.size": TEXT_FONTSIZE,
            "figure.figsize": (15 * cm, 7.2 * cm),
            "text.usetex": True,
            "font.family": "libertine",
        }
    )

    _, ax = plt.subplots(1, 1)

    packing.index = packing.index.map(lambda x: x.value / 3.6e12)
    i = 0
    for ratio in RATIOS:
        for api_activated in EXTENSION_ACTIVATED:
            col = f"{ratio}/{legend_name_for_api(api_activated)}"
            color = plot_colors[i]
            marker = MARKERS[0]
            marker_size = MARKER_SIZE
            if i % 2 == 1:
                marker = MARKERS[1]
                marker_size *= 1.5
            ax = packing[col].plot.line(
                color=color,
                ax=ax,
                legend=False,
                linewidth=LINEWIDTH,
                marker=marker,
                markevery=MARKER_EVERY,
                markersize=marker_size,
            )
            i += 1

    ax.set_ylabel(f"Normalized cpu utilization")
    ax.xaxis.offsetText.set_visible(False)
    ax.set_xlabel(f"Timestamp [{TIME_UNIT_ACRONYM[TIME_UNIT]}]")
    packing_ax = ax

    if dataset == "google":
        packing_ax.legend(
            title="Ratio/Migration",
            labels=list(packing.columns),
            handles=packing_ax.lines,
            loc="upper center",
            bbox_to_anchor=(1, -0.3),
            fancybox=True,
            ncol=3,
            columnspacing=0.1,
            title_fontsize=TEXT_FONTSIZE,
            fontsize=TEXT_FONTSIZE,
        )
    else:
        if dataset == "azure":
            ax.legend(
                title="Ratio/Migration",
                labels=list(packing.columns),
                handles=packing_ax.lines,
                loc="upper center",
                bbox_to_anchor=(0.5, -0.3),
                fancybox=True,
                ncol=3,
                columnspacing=0.1,
                title_fontsize=TEXT_FONTSIZE,
                fontsize=TEXT_FONTSIZE,
            )
        else:
            ax.legend(
                title="Ratio-Migration",
                labels=list(packing.columns),
                handles=packing_ax.lines,
                title_fontsize=TEXT_FONTSIZE,
                fontsize=TEXT_FONTSIZE,
            )

    if dataset == "azure":
        ax.annotate(
            "",
            xy=(0.9e15 / 3.6e12, 0.78),
            xytext=(0.9e15 / 3.6e12, 0.72),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(
            0.5e15 / 3.6e12, 0.7, r"\textbf{8\%ut.}", fontdict={"size": ARROW_FONTSIZE}
        )

        ax.annotate(
            "",
            xy=(1.1e15 / 3.6e12, 0.91),
            xytext=(1.1e15 / 3.6e12, 0.79),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(
            0.6e15 / 3.6e12, 0.9, r"\textbf{15\%ut.}", fontdict={"size": ARROW_FONTSIZE}
        )

        ax.annotate(
            "",
            xy=(1.35e15 / 3.6e12, 0.68),
            xytext=(1.35e15 / 3.6e12, 0.73),
            arrowprops=dict(
                facecolor="#FF5F59",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(
            1.1e15 / 3.6e12,
            0.65,
            r"\textbf{-6\%ut.}",
            fontdict={"size": ARROW_FONTSIZE},
        )
    elif dataset == "bitbrains":
        ax.annotate(
            "",
            xy=(2.3e15, 0.823),
            xytext=(2.3e15, 0.783),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(1.56e15, 0.81, r"\textbf{4\% ut.}", fontdict={"size": ARROW_FONTSIZE})

        ax.annotate(
            "",
            xy=(2.1e15, 0.7975),
            xytext=(2.1e15, 0.783),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(1.53e15, 0.8, r"\textbf{1\% ut.}", fontdict={"size": ARROW_FONTSIZE})

        ax.annotate(
            "",
            xy=(1.8e15, 0.786),
            xytext=(1.8e15, 0.781),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(1.6e15, 0.773, r"\textbf{0.5\% ut.}", fontdict={"size": ARROW_FONTSIZE})
    ax.annotate(" ", xy=(0.9e15 / 3.6e12, 0.93), annotation_clip=False)

    if SAVE_TO_DISK:
        path = (
            f"{OUTPUT_DISK_PATH}/migrations-results-packing-{dataset}.{OUTPUT_FORMAT}"
        )
        plt.savefig(path, bbox_inches="tight")
        print(f"Figure saved to disk at: {path}")
    plt.show()
    plt.close()

    _, ax = plt.subplots(1, 1)
    _, df_time = times
    i = 0
    ax = df_time.quantile(0.90).plot.bar(
        ax=ax,
        color=plot_colors,
        yerr=[[0, 0, 0, 0, 0, 0], [40, 0, 190, 0, 200, 0]],
        error_kw=dict(lw=1, capsize=5, capthick=1, ecolor="green", marker="_"),
    )
    ax.text(
        0.05, 320, r"\textbf{13\%}", fontdict={"color": "green", "size": ARROW_FONTSIZE}
    )
    ax.text(
        2.05, 260, r"\textbf{63\%}", fontdict={"color": "green", "size": ARROW_FONTSIZE}
    )
    ax.text(
        4.05, 200, r"\textbf{81\%}", fontdict={"color": "green", "size": ARROW_FONTSIZE}
    )
    ax.set_xticklabels(df_time.columns, rotation=45)
    ax.set_ylabel(f"Total time (P90) [{TIME_UNIT_ACRONYM[TIME_UNIT]}]")

    if SAVE_TO_DISK:
        path = (
            f"{OUTPUT_DISK_PATH}/migrations-results-totaltime-{dataset}.{OUTPUT_FORMAT}"
        )
        plt.savefig(path, bbox_inches="tight")
        print(f"Figure saved to disk at: {path}")
    plt.show()
    plt.close()


if __name__ == "__main__":
    dataset = sys.argv[1]
    utilization = sys.argv[2]
    agg_n = int(sys.argv[3]) if len(sys.argv) > 3 else 5

    utilizations = pd.DataFrame()
    response_times = pd.DataFrame()
    waiting_times = pd.DataFrame()
    total_times = pd.DataFrame()
    for cluster_type in CLUSTER_TYPES:
        for ratio in RATIOS:
            for api_activated in EXTENSION_ACTIVATED:

                host = pd.read_parquet(
                    f"{INPUT_DISK_PATH}/migrations/{dataset}/utilization={utilization}/host/api={api_activated}-ratio={ratio}-cluster={cluster_type}/data.parquet",
                    columns=["timestamp", "cpu_utilization", "host_id"],
                )
                results = load_times_steady(
                    f"{INPUT_DISK_PATH}/migrations/{dataset}/utilization={utilization}/server/api={api_activated}-ratio={ratio}-cluster={cluster_type}/data.parquet",
                    dataset,
                )

                # packing
                steady = get_steady_timestamp(dataset)
                host = host[host.timestamp < host.timestamp.min() + steady]
                host_num = len(host.host_id.unique())

                grouped_host = host.groupby("timestamp")[["cpu_utilization"]].sum()
                grouped_host = grouped_host.reset_index()
                agg_host = grouped_host.groupby(grouped_host.index // agg_n)[
                    ["timestamp", "cpu_utilization"]
                ].min()
                agg_host["cpu_utilization"] = grouped_host.groupby(
                    grouped_host.index // agg_n
                )[["cpu_utilization"]].mean()
                agg_host = agg_host.set_index("timestamp")
                grouped_host = agg_host

                grouped_host.cpu_utilization = (
                    grouped_host.cpu_utilization / host_num
                ) * 0.95
                grouped_host.rename(
                    columns={
                        "cpu_utilization": f"{ratio}/{legend_name_for_api(api_activated)}"
                    },
                    inplace=True,
                )
                utilizations = pd.concat([utilizations, grouped_host], axis=1)

                host = None
                grouped_host = None

                # times
                response_times[
                    f"{ratio}/{legend_name_for_api(api_activated)}"
                ] = results["response_time"].apply(
                    lambda x: x.total_seconds() / TIME_UNITS_TO_INT[TIME_UNIT]
                )
                # calculate waiting time
                waiting_times[
                    f"{ratio}/{legend_name_for_api(api_activated)}"
                ] = results["waiting_time"].apply(
                    lambda x: x.total_seconds() / TIME_UNITS_TO_INT[TIME_UNIT]
                )

                total_times[f"{ratio}/{legend_name_for_api(api_activated)}"] = (
                    waiting_times[f"{ratio}/{legend_name_for_api(api_activated)}"]
                    + response_times[f"{ratio}/{legend_name_for_api(api_activated)}"]
                )
                results = None

    plot_results(utilizations, ("total time", total_times), dataset)
