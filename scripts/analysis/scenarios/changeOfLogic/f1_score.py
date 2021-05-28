import pandas as pd
from pandas import DataFrame
import numpy as np


import matplotlib
import matplotlib as mpl

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
MARKER_SIZE = 9.0
MARKER_FREQUENCY = 1000

mpl.rcParams['ps.useafm'] = True
mpl.rcParams['pdf.use14corefonts'] = True
mpl.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
mpl.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
mpl.rcParams['font.family'] = OPT_FONT_NAME
matplotlib.rcParams['pdf.fonttype'] = 42

FIGURE_FOLDER = './'


class F1DataBuilder:

    def __init__(self):
        self.col_index = {"TP": 1, "TN": 2, "FP": 3, "FN": 4, "total": 5}

    def build_data(self, f1: DataFrame):
        res = dict()
        for index, row in f1.iterrows():
            time_stamp = row['timestamp']
            insert_index = self.col_index[row['type']]
            if time_stamp not in res.keys():
                record = [time_stamp, 0, 0, 0, 0, 0]
                res[time_stamp] = record
            else:
                record = res.get(time_stamp)
            record[insert_index] = row['count']
        res_list = sorted(list(res.values()), key=lambda x: x[0])
        total_index = self.col_index['total']
        first_timestamp = res_list[0][0]
        for record in res_list:
            record[total_index] = sum(record[1:total_index])
            record[0] -= first_timestamp
        return np.asarray(res_list)

def get_f1(confusion_matrix):
    precision = confusion_matrix[:, 1] / (confusion_matrix[:, 1] + confusion_matrix[:, 3])
    recall = confusion_matrix[:, 1] / (confusion_matrix[:, 1] + confusion_matrix[:, 4])
    precision2 = confusion_matrix[:, 2] / (confusion_matrix[:, 2] + confusion_matrix[:, 4])
    recall2 = confusion_matrix[:, 2] / (confusion_matrix[:, 2] + confusion_matrix[:, 3])
    f1score_fraud = 2 * precision * recall / (precision + recall)
    f1score_normal = 2 * precision2 * recall2 / (precision2 + recall2)
    return f1score_fraud


def draw(confusion_matrix_update, confusion_matrix_raw):
    labels = ["Precision=TP/(TP+FP)", "Recall=TP/(TP+FN)", "F1 score for fraud",
              "Precision2=TN/(TN+FN)", "Recall2=TN/(TN+FP)", "F1 score for normal"]
    color = ['k', 'r', 'b', 'c', 'm', 'g']

    # fig = plt.figure(figsize=(10, 5))
    # figure = fig.add_subplot(111)

    f1score_fraud_update = get_f1(confusion_matrix_update)
    f1score_fraud_raw = get_f1(confusion_matrix_raw)

    x_axis = []
    y_axis = []
    legend_labels = []

    x_axis.append([x-25 for x in confusion_matrix[125:525, 0]])
    # x_axis.append(confusion_matrix[125:, 0])
    y_axis.append(f1score_fraud_update[125:525])
    legend_labels.append("Trisk")

    x_axis.append([x-25 for x in confusion_matrix_raw[125:525, 0]])
    # x_axis.append(confusion_matrix_raw[125:, 0])
    y_axis.append(f1score_fraud_raw[125:525])
    legend_labels.append("Baseline")

    legend = True

    decisions = []
    for i in [225, 345]:
        if len(confusion_matrix_update) <= i:
            break
        decisions.append(confusion_matrix_update[i, 0])

    # for i in [120, 225, 345]:
    #     if len(confusion_matrix_update) <= i:
    #         break
    #     figure.plot(confusion_matrix_update[i, 0], f1score_fraud_update[i], color = 'tab:gray',
    #                     linewidth = LINE_WIDTH,
    #                     # marker=MARKERS[i], \
    #                     # markersize=MARKER_SIZE,
    #                     label = "Baseline",
    #                     markeredgewidth = 2, markeredgecolor = 'k',
    #                     markevery = 5)
    #
    # figure.plot(confusion_matrix[:, 0], f1score_fraud_update, c=color[2], label=labels[2])
    # plt.plot(confusion_matrix_raw[:, 0], f1score_fraud_raw,
    #                     color = LINE_COLORS[0],
    #                     linewidth = LINE_WIDTH,
    #                     # marker=MARKERS[i], \
    #                     # markersize=MARKER_SIZE,
    #                     label = "Baseline",
    #                     markeredgewidth = 2, markeredgecolor = 'k',
    #                     markevery = 5)
    # plt.xlim(150, 600)
    # plt.ylim(0, 1)
    #
    # plt.xlabel('time /s', fontproperties=LABEL_FP)
    # plt.ylabel('score', fontproperties=LABEL_FP)
    # plt.title('Evaluation Result by applying Model Update (At red point)')
    #
    # plt.legend(loc=0)
    # plt.show()

    DrawFigure(x_axis, y_axis, legend_labels, "time(s)", "F1_score", "fd_f1_score", legend, decisions)



# draw a line chart
def DrawFigure(xvalues, yvalues, legend_labels, x_label, y_label, filename, allow_legend, decisions):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(10, 5))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    x_values = xvalues
    y_values = yvalues
    lines = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i],
                                linewidth=LINE_WIDTH,
                                marker=MARKERS[i], \
                                markersize=MARKER_SIZE,
                                label=FIGURE_LABEL[i],
                                markeredgewidth=2, markeredgecolor='k',
                                markevery=7
                                )

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=1,
                   #                     mode='expand',
                   bbox_to_anchor=(0.8, 0.99), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    for i in decisions:
        plt.axvline(x=i-25, color='tab:gray', label='axvline - full height')

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)
    # plt.yscale('log')
    # plt.ylim(0, 100)
    # plt.xlim(150, 600)
    plt.ylim(0, 1)
    # plt.grid()
    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')


if __name__ == '__main__':
    f1 = pd.read_csv("confusion_matrix_update7.csv")
    f2 = pd.read_csv("confusion_matrix_raw7.csv")
    builder = F1DataBuilder()
    confusion_matrix = builder.build_data(f1)
    confusion_matrix_raw = builder.build_data(f2)
    draw(confusion_matrix, confusion_matrix_raw)
    # code = r""