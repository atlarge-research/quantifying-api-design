from datetime import timedelta

OUTPUT_FORMAT = "pdf"
SAVE_TO_DISK = True
OUTPUT_DISK_PATH = "plot/output"

INPUT_DISK_PATH = "plot/input"
TRACES_DISK_PATH = "plot/trace"

MARKER_SIZE = 6
MARKER_EVERY = 200
LINEWIDTH = 1.0
MARKERS = ["o", "*", "s"]
TEXT_FONTSIZE = 25 * 0.65
ARROW_FONTSIZE = TEXT_FONTSIZE * 0.8
cm = 1 / 2.54

TIME_UNIT = "hours"
TIME_UNIT_ACRONYM = {"hours": "h"}

TIME_UNITS_TO_INT = {"hours": 60 * 60, "days": 60 * 60 * 24}
SCHEDULER_QUANTUM = timedelta(minutes=5)
