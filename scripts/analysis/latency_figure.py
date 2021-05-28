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


def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# draw a bar chart
def DrawFigure(x_values, y_values, legend_labels, x_label, y_label, y_min, y_max, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(10, 3))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # values in the x_xis
    index = np.arange(len(x_values))
    # the bar width.
    # you may need to tune it to get the best figure.
    width = 0.08
    # draw the bars
    bars = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        bars[i] = plt.bar(index + i * width + width / 2,
                          y_values[i], width,
                          hatch=PATTERNS[i],
                          color=LINE_COLORS[i],
                          label=FIGURE_LABEL[i], edgecolor='black', linewidth=3)

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(bars, FIGURE_LABEL,
                   prop=LEGEND_FP,
                   ncol=5,
                   loc='upper center',
                   #                     mode='expand',
                   shadow=False,
                   bbox_to_anchor=(0.45, 1.6),
                   columnspacing=0.1,
                   handletextpad=0.2,
                   #                     bbox_transform=ax.transAxes,
                   #                     frameon=True,
                   #                     columnspacing=5.5,
                   #                     handlelength=2,
                   )

    # you may need to tune the xticks position to get the best figure.
    plt.xticks(index + 2.5 * width, x_values)
    # plt.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    # plt.grid(axis='y', color='gray')
    # figure.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

    # you may need to tune the xticks position to get the best figure.
    plt.yscale('log')
    #
    # plt.grid(axis='y', color='gray')
    # figure.yaxis.set_major_locator(LogLocator(base=10))
    # figure.xaxis.set_major_locator(LinearLocator(5))
    figure.get_xaxis().set_tick_params(direction='in', pad=10)
    figure.get_yaxis().set_tick_params(direction='in', pad=10)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')


# the average latency
def averageLatency(lines):
    # get all latency of all files, calculate the average
    totalLatency = 0
    count = 0
    for line in lines:
        if line.startswith('keygroup: '):
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

    return sum/2

def ReadFile(type):
    w, h = 3, 5
    y = [[] for _ in range(h)]
    col1 = []  # each col has 3 elements
    col2 = []
    col3 = []
    col4 = []

    affected_tasks = 2
    i = 0
    interval = 10000
    runtime = 150
    source_p = 5
    for rate in [1000, 2000, 4000, 8000, 9000]:
        for parallelism in [5, 10, 20]:
            n_tuples = int(rate * parallelism * runtime / source_p)
            print('trisk-{}-{}-{}-{}-{}-{}'.format(type, interval, parallelism, runtime, n_tuples, affected_tasks))
            # ${reconfig_type}-${reconfig_interval}-${parallelism}-${runtime}-${n_tuples}-${affected_tasks}
            exp = FILE_FOLER + '/trisk-{}-{}-{}-{}-{}-{}'.format(type, interval, parallelism, runtime,n_tuples, affected_tasks)
            file_path = os.path.join(exp, "Splitter FlatMap-0.output")
            if os.path.isfile(file_path):
                y[i].append(averageLatency(open(file_path).readlines()))
            else:
                exp = FILE_FOLER + '/trisk-{}-{}-{}-{}-{}-{}'.format(type, interval, parallelism, runtime,n_tuples, 4)
                file_path = os.path.join(exp, "Splitter FlatMap-0.output")
                y[i].append(averageLatency(open(file_path).readlines()))
        i += 1

    return y

if __name__ == '__main__':
    type = 'noop'

    try:
        opts, args = getopt.getopt(sys.argv[1:], '-t:h', ['reconfig type', 'help'])
    except getopt.GetoptError:
        print('breakdown_parallelism.py -t type')
        sys.exit(2)
    for opt, opt_value in opts:
        if opt in ('-h', '--help'):
            print("[*] Help info")
            exit()
        elif opt == '-t':
            print('Reconfig Type:', opt_value)
            type = str(opt_value)

    x_values = ['5', '10', '20']
    y_values = ReadFile(type)

    legend_labels = ['1k', '2k', '4k', '8k', '9k']

    DrawFigure(x_values, y_values, legend_labels,
               'parallelism', 'latency (ms)', 0,
               400, 'latency_{}_sync'.format(type), True)
