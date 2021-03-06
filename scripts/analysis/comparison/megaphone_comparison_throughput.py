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
    f = open("/home/myc/samza-hello-samza/test-megaphone-40960")
    read = f.readlines()
    for r in read:
        if r.find("endToEnd latency: ") != -1:
            arrival_ts = int(int(r.split("ts: ")[1][:13]))
            latency = int(r.split("endToEnd latency: ")[1])
            completion_ts = int((arrival_ts + latency)/1000)
            if start_ts == 0:
                start_ts = completion_ts
            ts = completion_ts - start_ts
            if ts not in temp_dict:
                temp_dict[ts] = 0
            temp_dict[ts] += 1
    #         if start_ts == 0:
    #             start_ts = int(int(r.split("ts: ")[1][:13]) / 1000)
    #         ts = int(int(r.split("ts: ")[1][:13]) / 1000) - start_ts
    #         latency = int(r.split("endToEnd latency: ")[1])
    #         if ts not in temp_dict:
    #             temp_dict[ts] = []
    #         temp_dict[ts].append(latency)

    coly = list(temp_dict.values())
    col = list(temp_dict.keys())
    # for ts in temp_dict:
    #     coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
    #     col.append(ts)
    # x_axis.append([x -10 for x in col][10:])
    x_axis.append([x+1 for x in col][19:119])
    # x_axis.append(col[10:120])
    y_axis.append(coly[19:119])
    print(coly)

    col = []
    coly = []
    temp_dict = {}
    start_ts = 0
    f = open("/home/myc/samza-hello-samza/test_trisk_comparison")
    read = f.readlines()
    for r in read:
        if r.find("endToEnd latency: ") != -1:
            arrival_ts = int(int(r.split("ts: ")[1][:13]))
            latency = int(r.split("endToEnd latency: ")[1])
            completion_ts = int((arrival_ts + latency) / 1000)
            if start_ts == 0:
                start_ts = completion_ts
            ts = completion_ts - start_ts
            if ts not in temp_dict:
                temp_dict[ts] = 0
            temp_dict[ts] += 1
            #         if start_ts == 0:
            #             start_ts = int(int(r.split("ts: ")[1][:13]) / 1000)
            #         ts = int(int(r.split("ts: ")[1][:13]) / 1000) - start_ts
            #         latency = int(r.split("endToEnd latency: ")[1])
            #         if ts not in temp_dict:
            #             temp_dict[ts] = []
            #         temp_dict[ts].append(latency)

    coly = [y/2 for y in list(temp_dict.values())]
    col = list(temp_dict.keys())
    # x_axis.append([x -10 for x in col][10:])
    x_axis.append(col[20:120])
    y_axis.append(coly[20:120])
    print(coly)

    col = []
    coly = []
    temp_dict = {}
    start_ts = 0
    f = open("/home/myc/workspace/flink-related/flink-1.11/build-target/trisk-remap-10000-200-10-5000-1000-40960-2-1-stable/flink-myc-taskexecutor-0-myc-amd.out")
    read = f.readlines()
    for r in read:
        if r.find("endToEnd latency: ") != -1:
            arrival_ts = int(int(r.split("ts: ")[1][:13]))
            latency = int(r.split("endToEnd latency: ")[1])
            completion_ts = int((arrival_ts + latency) / 1000)
            if start_ts == 0:
                start_ts = completion_ts
            ts = completion_ts - start_ts
            if ts not in temp_dict:
                temp_dict[ts] = 0
            temp_dict[ts] += 1

    coly = list(temp_dict.values())
    col = list(temp_dict.keys())

    x_axis.append([x+5 for x in col][15:115])
    y_axis.append(coly[15:115])
    # x_axis.append(col[10:120])
    # y_axis.append(coly[10:120])
    print(coly)

    # col = []
    # coly = []
    # temp_dict = {}
    # for i in range(0,10):
    #     ts = 0
    #     f = open("/data/trisk/Splitter FlatMap-{}.output".format(i))
    #     read = f.readlines()
    #     for r in read:
    #         if r.find("endToEndLantecy: ") != -1:
    #             latency = int(r.split("endToEndLantecy: ")[1][:2])
    #             # col.append(ts)
    #             # coly.append(latency)
    #             if ts not in temp_dict:
    #                 temp_dict[ts] = []
    #             temp_dict[ts].append(latency)
    #             ts += 1
    #
    # for ts in temp_dict:
    #     coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
    #     col.append(ts)
    # x_axis.append(col)
    # y_axis.append(coly)

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
                                markevery=3)
    plt.axvline(x = 50, color = 'b', label = 'axvline - full height')
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

    # plt.yscale('log')
    plt.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)
    plt.ylim(0)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')

if __name__ == "__main__":
    x_axis, y_axis = ReadFile()
    legend_labels = ["Megaphone", "Trisk", "Flink"]
    # legend_labels = ["Flink"]
    legend = False
    DrawFigure(x_axis, y_axis, legend_labels, "time(s)", "throughput(tuples/s)", "comparison_throughput", legend)
