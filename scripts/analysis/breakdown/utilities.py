import getopt
import os
import sys

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pylab
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator, LogLocator, MaxNLocator
from numpy import double
from matplotlib import gridspec

OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 20
LABEL_FONT_SIZE = 24
LEGEND_FONT_SIZE = 26
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

MARKERS = (['o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "|", "_"])
# you may want to change the color map for different figures
COLOR_MAP = ('#B03A2E', '#2874A6', '#239B56', '#7D3C98', '#F1C40F', '#F5CBA7', '#82E0AA', '#AEB6BF', '#AA4499')
# you may want to change the patterns for different figures
PATTERNS = (["\\", "///", "o", "||", "\\\\", "\\\\", "//////", "//////", ".", "\\\\\\", "\\\\\\"])
LABEL_WEIGHT = 'bold'
LINE_COLORS = COLOR_MAP
LINE_WIDTH = 3.0
MARKER_SIZE = 0.0
MARKER_FREQUENCY = 1000

matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
matplotlib.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['font.family'] = OPT_FONT_NAME

FIGURE_FOLDER = '/data/results'
FILE_FOLER = '/data/raw'

# timers = ["++++++prepare timer", "++++++synchronize timer", "++++++updateKeyMapping timer", "++++++updateState timer"]
timers = ["++++++prepare timer", "++++++synchronize timer", "++++++updateKeyMapping timer", "++++++updateState timer", "++++++updateFunction timer"]
timers_plot = ["++++++prepare timer", "++++++synchronize timer", "++++++update timer"]
# legend_labels = ['pre', 'sync', 'updkey', 'updstat']
legend_labels = ['pre', 'sync', 'upd']

def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# draw a line chart
def DrawFigure(x_values, y_values, legend_labels, x_label, y_label, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(9, 6))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # values in the x_xis
    index = np.arange(len(x_values))
    # the bar width.
    # you may need to tune it to get the best figure.
    width = 0.5
    # draw the bars
    bottom_base = np.zeros(len(y_values[0]))
    bars = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        bars[i] = plt.bar(index + width / 2, y_values[i], width, hatch=PATTERNS[i], color=LINE_COLORS[i],
                          label=FIGURE_LABEL[i], bottom=bottom_base, edgecolor='black', linewidth=3)
        bottom_base = np.array(y_values[i]) + bottom_base

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(bars, FIGURE_LABEL
                   #                     mode='expand',
                   #                     shadow=False,
                   #                     columnspacing=0.25,
                   #                     labelspacing=-2.2,
                   #                     borderpad=5,
                   #                     bbox_transform=ax.transAxes,
                   #                     frameon=False,
                   #                     columnspacing=5.5,
                   #                     handlelength=2,
                   )
        if allow_legend == True:
            handles, labels = figure.get_legend_handles_labels()
        if allow_legend == True:
            print(handles[::-1], labels[::-1])
            leg = plt.legend(handles[::-1], labels[::-1],
                             loc='center',
                             prop=LEGEND_FP,
                             ncol=4,
                             bbox_to_anchor=(0.5, 1.2),
                             handletextpad=0.1,
                             borderaxespad=0.0,
                             handlelength=1.8,
                             labelspacing=0.3,
                             columnspacing=0.3,
                             )
            leg.get_frame().set_linewidth(2)
            leg.get_frame().set_edgecolor("black")

    # plt.ylim(0, 100)

    # # you may need to tune the xticks position to get the best figure.
    # plt.yscale('log')

    # you may need to tune the xticks position to get the best figure.
    plt.xticks(index + 0.5 * width, x_values)
    plt.xticks(rotation=30)

    # plt.xlim(0,)
    # plt.ylim(0,1)

    plt.grid(axis='y', color='gray')
    figure.yaxis.set_major_locator(LinearLocator(6))

    figure.get_xaxis().set_tick_params(direction='in', pad=10)
    figure.get_yaxis().set_tick_params(direction='in', pad=10)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    size = fig.get_size_inches()
    dpi = fig.get_dpi()

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight', format='pdf')

# draw a line chart
def DrawFigureV2(x_values, y_values, legend_labels, x_label, y_label, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(9, 6))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # values in the x_xis
    index = np.arange(len(x_values))
    # the bar width.
    # you may need to tune it to get the best figure.
    width = 0.5
    # draw the bars
    bottom_base = np.zeros(len(y_values[0]))
    bars = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        bars[i] = plt.bar(index + width / 2, y_values[i], width, hatch=PATTERNS[i], color=LINE_COLORS[i],
                          label=FIGURE_LABEL[i], bottom=bottom_base, edgecolor='black', linewidth=3)
        bottom_base = np.array(y_values[i]) + bottom_base

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(bars, FIGURE_LABEL
                   #                     mode='expand',
                   #                     shadow=False,
                   #                     columnspacing=0.25,
                   #                     labelspacing=-2.2,
                   #                     borderpad=5,
                   #                     bbox_transform=ax.transAxes,
                   #                     frameon=False,
                   #                     columnspacing=5.5,
                   #                     handlelength=2,
                   )
        if allow_legend == True:
            handles, labels = figure.get_legend_handles_labels()
        if allow_legend == True:
            print(handles[::-1], labels[::-1])
            leg = plt.legend(handles[::-1], labels[::-1],
                             loc='center',
                             prop=LEGEND_FP,
                             ncol=1,
                             bbox_to_anchor=(1.15, 0.5),
                             handletextpad=0.1,
                             borderaxespad=0.0,
                             handlelength=1.8,
                             labelspacing=0.3,
                             columnspacing=0.3,
                             )
            leg.get_frame().set_linewidth(2)
            leg.get_frame().set_edgecolor("black")

    # plt.ylim(0, 100)

    # # you may need to tune the xticks position to get the best figure.
    # plt.yscale('log')

    # you may need to tune the xticks position to get the best figure.
    plt.xticks(index + 0.5 * width, x_values)
    plt.xticks(rotation=30)

    # plt.xlim(0,)
    # plt.ylim(0,1)

    plt.grid(axis='y', color='gray')
    figure.yaxis.set_major_locator(LinearLocator(6))

    figure.get_xaxis().set_tick_params(direction='in', pad=10)
    figure.get_yaxis().set_tick_params(direction='in', pad=10)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    size = fig.get_size_inches()
    dpi = fig.get_dpi()

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight', format='pdf')


# draw a line chart
def DrawFigureV3(x_values, y_values, legend_labels, x_label, y_label, filename, allow_legend):
    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # If we were to simply plot pts, we'd lose most of the interesting
    # details due to the outliers. So let's 'break' or 'cut-out' the y-axis
    # into two portions - use the top (ax) for the outliers, and the bottom
    # (ax2) for the details of the majority of our data
    # f, (ax, ax2) = plt.subplots(2, 1, sharex=True,figsize=(9, 4))
    fig = plt.figure(figsize=(9, 5))

    gs = gridspec.GridSpec(2, 1, height_ratios=[1, 1])
    ax1 = plt.subplot(gs[0])
    ax2 = plt.subplot(gs[1])


    FIGURE_LABEL = legend_labels
    # values in the x_xis
    index = np.arange(len(x_values))
    # the bar width.
    # you may need to tune it to get the best figure.
    width = 0.5
    # draw the bars
    bottom_base = np.zeros(len(y_values[0]))
    bars = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        # plot the same data on both axes
        if (i != 4):
            bars[i] = ax1.bar(index + width / 2, y_values[i], width, hatch=PATTERNS[i], color=LINE_COLORS[i],
                              label=FIGURE_LABEL[i], bottom=bottom_base, edgecolor='black', linewidth=3)
            ax2.bar(index + width / 2, y_values[i], width, hatch=PATTERNS[i], color=LINE_COLORS[i],
                    label=FIGURE_LABEL[i], bottom=bottom_base, edgecolor='black', linewidth=3)
            bottom_base = np.array(y_values[i]) + bottom_base
        else:
            bars[i] = ax1.bar(index + width / 2, y_values[i], 0, hatch='', linewidth=0, fill=False)
            ax2.bar(index + width / 2, y_values[i], 0, hatch='', linewidth=0, fill=False)

    # zoom-in / limit the view to different portions of the data
    ax1.set_ylim(2000, 30000)  # most of the data
    ax2.set_ylim(0, 300)  # waiting only

    # hide the spines between ax and ax2
    ax1.spines['bottom'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax1.xaxis.tick_top()
    ax1.tick_params(labeltop=False)  # don't put tick labels at the top
    ax2.xaxis.tick_bottom()
    # This looks pretty good, and was fairly painless, but you can get that
    # cut-out diagonal lines look with just a bit more work. The important
    # thing to know here is that in axes coordinates, which are always
    # between 0-1, spine endpoints are at these locations (0,0), (0,1),
    # (1,0), and (1,1).  Thus, we just need to put the diagonals in the
    # appropriate corners of each of our axes, and so long as we use the
    # right transform and disable clipping.

    d = .015  # how big to make the diagonal lines in axes coordinates
    # arguments to pass to plot, just so we don't keep repeating them
    kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
    ax1.plot((-d, +d), (-d, +d), **kwargs)  # top-left diagonal
    ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)  # top-right diagonal

    kwargs.update(transform=ax2.transAxes)  # switch to the bottom axes
    ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
    ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)  # bottom-right diagonal

    # What's cool about this is that now if we vary the distance between
    # ax and ax2 via f.subplots_adjust(hspace=...) or plt.subplot_tool(),
    # the diagonal lines will move accordingly, and stay right at the tips
    # of the spines they are 'breaking'

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(bars, FIGURE_LABEL, prop=LEGEND_FP,
                   loc='upper center', ncol=len(legend_labels), mode='expand', bbox_to_anchor=(0.45, 1.2), shadow=False,
                   frameon=False, borderaxespad=0.0, handlelength=2, labelspacing=0.2)

    # plt.xlabel(x_label, fontproperties=LABEL_FP)
    # ax1.set_ylabel(y_label, fontproperties=LABEL_FP)
    # Set common labels
    fig.text(0.5, 0.015, x_label, ha='center', fontproperties=LABEL_FP)
    fig.text(0.06, 0.5, y_label, va='center', rotation='vertical', fontproperties=LABEL_FP)
    # ax.set_xlabel(x_label, fontproperties=LABEL_FP)
    # ax.set_ylabel(y_label, fontproperties=LABEL_FP)
    # plt.ticklabel_format(axis='y', style='sci', scilimits=(0,0))
    # ax1.tick_params(axis='y', which='major', pad=-40)
    # ax1.tick_params(axis='x', which='major', pad=-20)
    # ax2.tick_params(axis='y', which='major', pad=40)
    # ax2.tick_params(axis='x', which='major', pad=20)
    # plt.subplots_adjust(left=0.1, bottom=None, right=None, top=None, wspace=None, hspace=None)
    # plt.grid(axis='y', color='gray')
    # ax1.yaxis.set_major_locator(pylab.LinearLocator(3))
    # you may need to tune the xticks position to get the best figure.

    ax1.grid(axis='y', color='gray')
    ax2.grid(axis='y', color='gray')
    plt.xticks(index + 0.5 * width, x_values)
    plt.xticks(rotation=30)
    plt.tight_layout(rect=[0.065, 0, 1, 1])
    plt.savefig(FIGURE_FOLDER + '/' + filename + '.pdf')
    # plt.savefig(FIGURE_FOLDER + "/" + filename + ".eps", bbox_inches='tight', format='eps')
    # ConvertEpsToPdf(FIGURE_FOLDER + "/" + filename)


# the average latency
def averageLatency(lines):
    # get all latency of all files, calculate the average
    totalLatency = 0
    count = 0
    for line in lines:
        if line.split(": ")[-1][:-1] != "NaN":
            totalLatency += float(line.split(": ")[-1][:-1])
            count += 1

    if count > 0:
        return totalLatency / count
    else:
        return 0


# the average reconfig time
def averageCompletionTime(lines):
    timers = {}
    counts = {}
    for line in lines:
        key = line.split(" : ")[0]
        if key[0:6] == "++++++":
            if line.split(" : ")[0] not in timers:
                timers[key] = 0
                counts[key] = 0
            timers[key] += int(line.split(" : ")[1][:-3])
            counts[key] += 1

    stats = []
    for key in timers:
        totalTime = timers[key]
        count = counts[key]
        if count > 0:
            stats.append(totalTime / count)
        else:
            stats.append(0)
    # reconfig time breakdown
    # print(stats)
    sum = 0
    for i in stats:
        sum += i
    return sum / 2


# the average reconfig time
def breakdown(lines):
    counter_limit = 1
    start_from = 0
    timers = {}
    counts = {}
    for line in lines:
        key = line.split(" : ")[0]
        if key[0:6] == "++++++":
            if line.split(" : ")[0] not in timers:
                timers[key] = 0
                counts[key] = 0
            if (key == "++++++updateKeyMapping timer"
                or key == "++++++updateState timer"
                or key == "++++++updateFunction timer") and "++++++update timer" not in timers:
                timers["++++++update timer"] = 0
                counts["++++++update timer"] = 0
            # if counts[key] < counter_limit:
            #     if counts[key] >= start_from:
            #         timers[key] += int(line.split(" : ")[1][:-3])
            #     counts[key] += 1
            if counts[key] < counter_limit:
                if counts[key] >= start_from:
                    if key == "++++++updateKeyMapping timer" or key == "++++++updateState timer":
                        timers["++++++update timer"] += int(line.split(" : ")[1][:-3])
                        counts["++++++update timer"] += 0.5
                    if key == "++++++updateFunction timer":
                        timers["++++++update timer"] += int(line.split(" : ")[1][:-3])
                        counts["++++++update timer"] += 1
                    else:
                        timers[key] += int(line.split(" : ")[1][:-3])
                counts[key] += 1

    stats = {}
    for key in timers:
        totalTime = timers[key]
        count = counts[key]
        if count > 0:
            stats[key] = totalTime / (count-start_from)
        else:
            stats[key] = 0

    return stats


def init():
    runtime = 100
    per_task_rate = 6000
    parallelism = 10
    key_set = 1000
    per_key_state_size = 1024  # byte
    # system level
    reconfig_interval = 10000
    reconfig_type = "logic"
    affected_tasks = 2
    repeat_num = 1
    return runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval, reconfig_type, affected_tasks, repeat_num
