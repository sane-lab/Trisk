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
COLOR_MAP = ('#000000', '#B03A2E', '#2874A6', '#239B56', '#7D3C98', '#000000', '#F1C40F', '#F5CBA7', '#82E0AA', '#AEB6BF', '#AA4499')
# you may want to change the patterns for different figures
PATTERNS = (["", "////", "\\\\", "//", "o", "", "||", "-", "//", "\\", "o", "O", "////", ".", "|||", "o", "---", "+", "\\\\", "*"])
LABEL_WEIGHT = 'bold'
LINE_COLORS = COLOR_MAP
LINE_WIDTH = 3.0
MARKER_SIZE = 4.0
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
    f = open("/home/myc/samza-hello-samza/test")
    read = f.readlines()
    for r in read:
        if r.find("endToEnd latency: ") != -1:
            if start_ts == 0:
                start_ts = int(int(r.split("ts: ")[1][:13])/1000)
            ts = int(int(r.split("ts: ")[1][:13])/1000) - start_ts
            latency = int(r.split("endToEnd latency: ")[1])
            if ts not in temp_dict:
                temp_dict[ts] = []
            temp_dict[ts].append(latency)

    for ts in temp_dict:
        coly.append(sum(temp_dict[ts]) / len(temp_dict[ts]))
        col.append(ts)
    x_axis.append(col)
    y_axis.append(coly)


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
    fig = plt.figure(figsize=(10, 3))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    x_values = xvalues
    y_values = yvalues
    lines = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i], \
                               linewidth=LINE_WIDTH, marker=MARKERS[i], \
                               markersize=MARKER_SIZE, label=FIGURE_LABEL[i],
                                markeredgewidth=2, markeredgecolor='k',
                                markevery=5
                               )

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=2,
                   #                     mode='expand',
                   bbox_to_anchor=(0.55, 1.5), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    plt.yscale('log')
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')

if __name__ == "__main__":
    x_axis, y_axis = ReadFile()
    legend_labels = ["Megaphone", "Trisk", "test"]
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "time(ms)", "latency(ms)", "comparison", legend)
