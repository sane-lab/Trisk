import getopt
import os
import sys
from math import ceil

import matplotlib
import matplotlib as mpl
from matplotlib.ticker import PercentFormatter, LogLocator
from numpy import double
from numpy.ma import arange

mpl.use('Agg')

import matplotlib.pyplot as plt
import pylab
from matplotlib.font_manager import FontProperties

OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 24
LABEL_FONT_SIZE = 28
LEGEND_FONT_SIZE = 30
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

MARKERS = (['o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "+", "_"])
# you may want to change the color map for different figures
COLOR_MAP = ('#B03A2E', '#2874A6', '#239B56', '#7D3C98', '#F1C40F', '#F5CBA7', '#82E0AA', '#AEB6BF', '#AA4499')
# COLOR_MAP = ('tab:blue', 'tab:orange', 'tab:green', 'tab:pink', 'tab:purple', 'tab:cyan', 'tab:gray', 'tab:brown', 'tab:olive')
# you may want to change the patterns for different figures
PATTERNS = (["", "////", "\\\\", "//", "o", "", "||", "-", "//", "\\", "o", "O", "////", ".", "|||", "o", "---", "+", "\\\\", "*"])
LABEL_WEIGHT = 'bold'
LINE_COLORS = COLOR_MAP
LINE_WIDTH = 3.0
MARKER_SIZE = 10.0
MARKER_FREQUENCY = 1000

mpl.rcParams['ps.useafm'] = True
mpl.rcParams['pdf.use14corefonts'] = True
mpl.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
mpl.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
mpl.rcParams['font.family'] = OPT_FONT_NAME
matplotlib.rcParams['pdf.fonttype'] = 42

FIGURE_FOLDER = '/data/results'

# there are some embedding problems if directly exporting the pdf figure using matplotlib.
# so we generate the eps format first and convert it to pdf.
def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# example for reading csv file
def ReadFile():
    x_axis = []
    y_axis = []

    col = []
    coly = []
    temp_dict = {}
    start_ts = 0
    f = open("/home/myc/samza-hello-samza/test_trisk_10")
    read = f.readlines()
    for r in read:
        if r.find("endToEnd latency: ") != -1:
            if start_ts == 0:
                start_ts = int(int(r.split("ts: ")[1][:13]) / 1000)
            ts = int(int(r.split("ts: ")[1][:13]) / 1000) - start_ts
            latency = int(r.split("endToEnd latency: ")[1])
            if ts not in temp_dict:
                temp_dict[ts] = []
            temp_dict[ts].append(latency)

    for ts in temp_dict:
        coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        col.append(ts)
    # x_axis.append([x -10 for x in col][10:])
    x_axis.append(col[10:])
    y_axis.append(coly[10:])

    col = []
    coly = []
    temp_dict = {}
    start_ts = 0
    f = open("/home/myc/samza-hello-samza/test_trisk_scenarios")
    read = f.readlines()
    for r in read:
        if r.find("endToEnd latency: ") != -1:
            if start_ts == 0:
                start_ts = int(int(r.split("ts: ")[1][:13]) / 1000)
            ts = int(int(r.split("ts: ")[1][:13]) / 1000) - start_ts
            latency = int(r.split("endToEnd latency: ")[1])
            if ts not in temp_dict:
                temp_dict[ts] = []
            temp_dict[ts].append(latency)

    for ts in temp_dict:
        coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        col.append(ts)
    # x_axis.append([x -10 for x in col][10:])
    x_axis.append(col[10:])
    y_axis.append(coly[10:])

    col = []
    coly = []
    temp_dict = {}
    start_ts = 0
    f = open(
        "/home/myc/workspace/flink-related/flink-1.11/flink-dist/target/flink-1.10.0-bin/flink-1.10.0/stock/flink-myc-taskexecutor-0-myc-amd.out")
    read = f.readlines()
    for r in read:
        if r.find("endToEnd latency: ") != -1:
            if start_ts == 0:
                start_ts = int(int(r.split("ts: ")[1][:13]) / 1000)
            ts = int(int(r.split("ts: ")[1][:13]) / 1000) - start_ts
            latency = int(r.split("endToEnd latency: ")[1])
            if ts not in temp_dict:
                temp_dict[ts] = []
            temp_dict[ts].append(latency)

    for ts in temp_dict:
        coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        col.append(ts)
    x_axis.append([x + 10 for x in col])
    y_axis.append(coly)
    # x_axis.append(col[10:])
    # y_axis.append(coly[10:])

    return x_axis, y_axis


# draw a line chart
def DrawFigure(xvalues, yvalues, legend_labels, x_label, y_label, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(10, 5))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    x_values = xvalues
    y_values = yvalues
    lines = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i], \
                               linewidth=LINE_WIDTH, marker=MARKERS[i], \
                               markersize=MARKER_SIZE, label=FIGURE_LABEL[i],
                                markeredgewidth=1, markeredgecolor='k',
                                markevery=11)
    plt.axvline(x = 10, color = 'tab:gray', label = 'axvline - full height')
    plt.axvline(x = 100, color = 'tab:gray', label = 'axvline - full height')
    plt.axvline(x = 200, color = 'tab:gray', label = 'axvline - full height')
    plt.axvline(x = 400, color = 'tab:gray', label = 'axvline - full height')
    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=4,
                   #                     mode='expand',
                   bbox_to_anchor=(0.5, 1.2), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    plt.yscale('log')
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)
    plt.ylim(10, 10000)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')

if __name__ == "__main__":
    x_axis, y_axis = ReadFile()
    legend_labels = ["Baseline", "Trisk", "Flink-reconfig"]
    # legend_labels = ["Flink"]
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "time(s)", "latency(ms)", "latency_comparison", legend)
