import pandas as pd
import sys
import matplotlib.pyplot as plt
from datetime import timedelta
import seaborn as sns
from common import *


UTILIZATIONS = [0.75, 0.8, 0.85]
RESERVATION_RATIOS = [0.0, 0.5, 1.0]

PLOT_COLORS = [
    "#B0E3E6",
    "#84B5B9",
    "#659FA6",
    "#FAD7AC",
    "#E2B785",
    "#C9945C",
    "#C2DDBF",
    "#AECFA8",
    "#9ABE90",
]


def plot_results(
    waiting_times: pd.DataFrame,
    slowdowns: pd.DataFrame,
    dataset: str,
    prefix: str = "",
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

    fig, axes = plt.subplots(1, 2, sharey=True)
    fig.subplots_adjust(bottom=0.15)

    i = 0
    for utilization in UTILIZATIONS:
        for ratio in RESERVATION_RATIOS:
            col = f"{utilization}/{ratio}"
            color = PLOT_COLORS[i]
            marker = MARKERS[i % 3]
            marker_size = MARKER_SIZE
            if marker == "*":
                marker_size *= 1.5
            ax = sns.ecdfplot(
                waiting_times[[col]],
                log_scale=True,
                ax=axes[0],
                legend=False,
                palette=[color],
                linewidth=LINEWIDTH,
                marker=marker,
                markevery=MARKER_EVERY,
                markersize=marker_size,
            )
            i += 1

    ax.set_xlabel(f"Waiting time [{TIME_UNIT_ACRONYM[TIME_UNIT]}]")
    ax.xaxis.set_ticks([1e0, 1e1, 1e2])
    from matplotlib import ticker as mticker

    ax.xaxis.set_minor_locator(mticker.LogLocator(numticks=999, subs="auto"))

    i = 0
    for utilization in UTILIZATIONS:
        for ratio in RESERVATION_RATIOS:
            col = f"{utilization}/{ratio}"
            color = PLOT_COLORS[i]
            marker = MARKERS[i % 3]
            marker_size = MARKER_SIZE
            if i % 3 == 1:
                marker = MARKERS[1]
                marker_size *= 1.5
            ax = sns.ecdfplot(
                slowdowns[[col]],
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
        title="Utilization/Ratio",
        labels=list(slowdowns.columns),
        handles=ax.lines,
        loc="upper center",
        bbox_to_anchor=(0, -0.3),
        fancybox=True,
        ncol=len(UTILIZATIONS) * len(RESERVATION_RATIOS) / 3,
        columnspacing=0.3,
        title_fontsize=TEXT_FONTSIZE,
        fontsize=TEXT_FONTSIZE,
    )
    ax.xaxis.set_ticks([1e0, 1e1, 1e2, 1e3])
    ax.xaxis.set_minor_locator(mticker.LogLocator(numticks=999, subs="auto"))
    ax.set_xlabel("Slowdown")
    ax.axes.get_yaxis().get_label().set_visible(False)

    left, right = axes

    if dataset == "azure":
        left.annotate(
            "",
            xy=(4.5e1, 0.5),
            xytext=(8e1, 0.5),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        left.text(1e2, 0.5, r"\textbf{35h\\(43\%)}", fontdict={"size": ARROW_FONTSIZE})

        left.annotate(
            "",
            xy=(3e0, 0.5),
            xytext=(0.5e0, 0.5),
            arrowprops=dict(
                facecolor="#FF5F59",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        left.text(
            0.05e0, 0.57, r"\textbf{-2.5h(500\%)}", fontdict={"size": ARROW_FONTSIZE}
        )

        right.annotate(
            "",
            xy=(3.2e1, 0.5),
            xytext=(1e2, 0.5),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        right.text(
            8e1, 0.4, r"\textbf{68 sld.\\(70\%)}", fontdict={"size": ARROW_FONTSIZE}
        )

        right.annotate(
            "",
            xy=(2e1, 0.65),
            xytext=(0.8e1, 0.65),
            arrowprops=dict(
                facecolor="#FF5F59",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        right.text(
            0.08e1,
            0.7,
            r"\textbf{-12 sld.\\(150\%)}",
            fontdict={"size": ARROW_FONTSIZE},
        )
    elif dataset == "google":
        right.annotate(
            "",
            xy=(6.5e0, 0.92),
            xytext=(6.5e0, 0.89),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        right.annotate(
            "",
            xy=(6.5e0, 0.97),
            xytext=(6.5e0, 0.93),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )

        right.annotate(
            "",
            xy=(6.5e0, 0.99),
            xytext=(6.5e0, 0.97),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )

        right.text(6e0, 0.83, r"\textbf{3\% prop.}")

    plt.subplots_adjust(wspace=0, hspace=0)

    if SAVE_TO_DISK:
        path = f"{OUTPUT_DISK_PATH}/reservations-results-{dataset}.{OUTPUT_FORMAT}"
        plt.savefig(path, bbox_inches="tight")
        print(f"Figure saved to disk at: {path}")
    plt.show()
    plt.close()

    fig, axes = plt.subplots(1, 1)
    if dataset == "azure":
        fig.subplots_adjust(bottom=0.25)
    elif dataset == "bitbrains":
        fig.subplots_adjust(bottom=0.35)
    elif dataset == "google":
        fig.subplots_adjust(bottom=0.35)

    ax = slowdowns.mean().plot(
        kind="bar",
        ax=axes,
        color=PLOT_COLORS,
        hatch=[
            "",
            "-",
            "*",
            "",
            "-",
            "*",
            "",
            "-",
            "*",
        ],
    )
    for tick in ax.get_xticklabels():
        tick.set_rotation(45)
    ax.set_xlabel("[Utilization]-[Reservation/Ratio]")
    ax.set_ylabel("Average slowdown")

    if dataset == "azure":
        ax.annotate(
            "",
            xy=(7, 215),
            xytext=(7, 245),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(6.5, 245, r"\textbf{20 \\sld.}")

        ax.annotate(
            "",
            xy=(8, 215),
            xytext=(8, 245),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(8, 245, r"\textbf{20 \\sld.}")
    elif dataset == "google":
        ax.annotate(
            "",
            xy=(2, 1.35),
            xytext=(2, 1.6),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(1.5, 1.7, r"\textbf{0.3 \\sld.}")

        ax.annotate(
            "",
            xy=(5, 2),
            xytext=(5, 2.3),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(4.5, 2.3, r"\textbf{0.3 \\sld.}")

        ax.annotate(
            "",
            xy=(8, 2.5),
            xytext=(8, 2.9),
            arrowprops=dict(
                facecolor="#CDEB8B",
                shrink=0.05,
                width=LINEWIDTH * 3,
                headwidth=LINEWIDTH * 6 * 1.5,
                headlength=LINEWIDTH * 4 * 1.5,
            ),
        )
        ax.text(7.5, 3, r"\textbf{0.4 \\sld.}")

    if SAVE_TO_DISK:
        plt.savefig(
            f"results/reservations/{dataset}/{prefix}{dataset}-slowdown-bars.pdf",
            bbox_inches="tight",
        )
    plt.show()
    plt.close()


if __name__ == "__main__":
    plt.rcParams["text.usetex"] = True
    plt.rcParams["font.family"] = "libertine"

    dataset = sys.argv[1]

    all_waiting_times = pd.DataFrame()
    all_slowdowns = pd.DataFrame()

    for utilization in UTILIZATIONS:
        waiting_times = pd.DataFrame()
        slowdowns = pd.DataFrame()

        for ratio in RESERVATION_RATIOS:
            try:
                server = pd.read_parquet(
                    f"{INPUT_DISK_PATH}/reservations/{dataset}/server/utilization={utilization}-reservation_ratio={ratio}/data.parquet",
                    columns=["server_id", "provision_time", "boot_time", "timestamp"],
                )
            except:
                continue
            server = server.groupby("server_id")[
                ["provision_time", "boot_time", "timestamp"]
            ].max()
            # calculate response_time
            server["response_time"] = server["timestamp"] - server["boot_time"]
            server["response_time"] = server["response_time"].apply(
                lambda x: x.total_seconds()
            )

            # calculate waiting time
            server["waiting_time"] = server["boot_time"] - server["provision_time"]
            server["waiting_time"] = server["waiting_time"].apply(
                lambda x: x.total_seconds() / TIME_UNITS_TO_INT[TIME_UNIT]
            )
            all_waiting_times[f"{utilization}/{ratio}"] = server["waiting_time"]
            waiting_times[f"{utilization}/{ratio}"] = server["waiting_time"]

            # calculate slowdown
            server["runtime"] = (server["timestamp"] - server["provision_time"]).apply(
                lambda x: x.total_seconds()
            )
            server["slowdown"] = server["runtime"] / server["response_time"]
            all_slowdowns[f"{utilization}/{ratio}"] = server["slowdown"]
            slowdowns[f"{utilization}/{ratio}"] = server["slowdown"]

            # print
            print(
                f"utilization<{utilization}> ratio<{ratio}> Response time:",
                timedelta(seconds=server["response_time"].mean()),
            )
            print(
                f"utilization<{utilization}> ratio<{ratio}> Waiting time:",
                timedelta(seconds=server["waiting_time"].mean()),
            )
            print(
                f"utilization<{utilization}> ratio<{ratio}> Slowdown:",
                server["slowdown"].mean(),
            )
            print("============================================================")
            server = None
    plot_results(all_waiting_times, all_slowdowns, dataset, prefix="")
