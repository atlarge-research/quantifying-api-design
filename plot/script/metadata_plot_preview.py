import pandas as pd
import sys
import matplotlib.pyplot as plt
from common import *
from metadata_plot_results import *


def legend_name_for_api(is_activated: bool):
    if is_activated == "true":
        return "metadata"
    return "no-metadata"


def plot_preview(
    times,
    dataset: str,
):
    plt.rcParams.update(
        {
            "legend.title_fontsize": TEXT_FONTSIZE,
            "font.size": TEXT_FONTSIZE,
            "figure.figsize": (15 * cm, 2 * cm),
            "text.usetex": True,
            "font.family": "libertine",
        }
    )

    fig, ax = plt.subplots(1, 1)
    fig.subplots_adjust(bottom=0.15)

    _, df_time = times

    ax = df_time.quantile(0.85).plot.barh(
        ax=ax,
        color=["#B0E3E6", "#FAD7AC"],
        xerr=[[0, 0], [40, 0]],
        error_kw=dict(lw=1, capsize=5, capthick=1, ecolor="green", uplims=True),
    )

    ax.text(100, 0.1, r"\textbf{40\%}", fontdict={"color": "green"})

    ax.set_xlabel(f"Google trace total workflow time [{TIME_UNIT_ACRONYM[TIME_UNIT]}]")

    if SAVE_TO_DISK:
        path = f"{OUTPUT_DISK_PATH}/metadata-preview-{dataset}.{OUTPUT_FORMAT}"
        plt.savefig(path, bbox_inches="tight")
        print(f"Figure saved to disk at: {path}")
    plt.show()
    plt.close()


if __name__ == "__main__":
    dataset = sys.argv[1]
    agg_n = sys.argv[2] if len(sys.argv) > 2 else 1

    response_times = pd.DataFrame()
    waiting_times = pd.DataFrame()
    total_times = pd.DataFrame()

    for api_activated in EXTENSION_ACTIVATED:
        host = pd.read_parquet(
            f"{INPUT_DISK_PATH}/metadata/{dataset}/host/api={api_activated.lower()}-servers={SERVERS_NUM}-cores={CORES_NUM}/data.parquet",
            columns=["timestamp", "cpu_utilization", "host_id"],
        )
        results = load_times_steady(
            f"{INPUT_DISK_PATH}/metadata/{dataset}/server/api={api_activated}-servers={SERVERS_NUM}-cores={CORES_NUM}/data.parquet",
            dataset,
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

    plot_preview(storage_buffers, ("total time", total_times), dataset)
