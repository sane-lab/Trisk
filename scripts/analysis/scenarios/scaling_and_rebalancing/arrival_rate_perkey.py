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
PATTERNS = (
["", "////", "\\\\", "//", "o", "", "||", "-", "//", "\\", "o", "O", "////", ".", "|||", "o", "---", "+", "\\\\", "*"])
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

FIGURE_FOLDER = './'

Order_No = 0
Tran_Maint_Code = 1
Last_Upd_Date = 2
Last_Upd_Time = 3
Last_Upd_Time_Dec = 4
Order_Price = 8
Order_Exec_Vol = 9
Order_Vol = 10
# Sec_Code = 11
Sec_Code = 6
Trade_Dir = 22

# there are some embedding problems if directly exporting the pdf figure using matplotlib.
# so we generate the eps format first and convert it to pdf.
def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# example for reading csv file
def ReadFile():
    x_axis = []
    y_axis = []

    temp_dict = {}
    start_ts = 0
    # f = open("/home/myc/workspace/datasets/SSE/sb-4hr-50ms.txt")
    f = open("/home/myc/workspace/datasets/SSE/sb-opening-50ms.txt")
    read = f.readlines()
    # for line in read:
    # filter = ["580026", "600111", "600089", "600584", "600175"]
    end_counter = 0
    ts_count = 0
    ts_count_perkey = {}
    temp_dict = {}
    ts = 0
    ts_list_perkey = {}
    for line in read:
        ts_count += 1
        text_arr = line.split("|")
        # if len(text_arr) > 10:
        if len(text_arr) > 7:
            if text_arr[Sec_Code] not in ts_count_perkey:
                ts_count_perkey[text_arr[Sec_Code]] = 0
            if text_arr[Sec_Code] not in ts_list_perkey:
                ts_list_perkey[text_arr[Sec_Code]] = {}
            ts_count_perkey[text_arr[Sec_Code]] += 1
        if line == "end\n":
            end_counter += 1
            if end_counter == 20:
                temp_dict[ts] = ts_count
                for key in ts_count_perkey:
                    ts_list_perkey[key][ts] = ts_count_perkey[key]
                # else:
                # print(count10s)
                ts_count = 0
                end_counter = 0
                ts_count_perkey = {}
                ts += 1
    f.close()

    temp_dict_2 = {}
    datafreq = {}
    col = []
    coly = []
    for stock_id in ts_list_perkey:
        count_dict = ts_list_perkey[stock_id]
        for ts in temp_dict:
            if 10 <= ts < 210:
                if ts in count_dict:
                    coly.append(count_dict[ts])
                    col.append(ts)
                else:
                    coly.append(0)
                    col.append(ts)
        temp_dict_2[stock_id] = [col, coly]
        datafreq[stock_id] = sum(coly)
        col = []
        coly = []

    sorted_datafreq = sorted(datafreq.items(), key=lambda x: x[1], reverse=True)
    print(sorted_datafreq)
    legend_labels = []
    for i in range(0, 5):
        k, v = sorted_datafreq[i]
        legend_labels.append(k)
        sample_ratio = 1
        # for x in range(0, len(temp_dict_2[k])):
        #     if x % sample_ratio == 0:
        #         col.append(x)
        #         coly.append(temp_dict_2[k][x])
        col = temp_dict_2[k][0]
        coly = temp_dict_2[k][1]
        # x_axis.append(col)
        # y_axis.append(coly)

    col = []
    coly = []
    for ts in temp_dict:
        if 10 <= ts < 300:
            col.append(ts)
            coly.append(temp_dict[ts])
    x_axis.append(col)
    y_axis.append(coly)
    legend_labels.append("overall")

    return legend_labels, x_axis, y_axis


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
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i],
                                linewidth=LINE_WIDTH,
                                # marker=MARKERS[i], \
                                # markersize=MARKER_SIZE,
                                label=FIGURE_LABEL[i],
                                markeredgewidth=2, markeredgecolor='k',
                                markevery=5
                                )

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=5,
                   #                     mode='expand',
                   bbox_to_anchor=(0.55, 1.5), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    # plt.yscale('log')
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)
    # plt.ylim(0, 60)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')


if __name__ == "__main__":
    legend_labels, x_axis, y_axis = ReadFile()
    # legend_labels = ["1", "2", "3", "4", "5"]
    legend = False
    DrawFigure(x_axis, y_axis, legend_labels, "time(s)", "arrival rate(e/s)", "test", legend)
