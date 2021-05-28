# 1. Count the frequency of all stock ids
# 2. Count the per second arrival rate of orders
# 3. count the most frequent top 10 ids' arrvial rate.
import matplotlib.pyplot as plt
import pandas as pd
import csv
import json
import datetime

Order_No            = 0
Tran_Maint_Code     = 1
Last_Upd_Date       = 2
Last_Upd_Time       = 3
Last_Upd_Time_Dec   = 4
Entry_Date          = 5
Entry_Time          = 6
Entry_Time_Dec      = 7
Order_Price         = 8
Order_Exec_Vol      = 9
Order_Vol           = 10
Sec_Code            = 11
PBU_ID              = 12
Acct_ID             = 13
Acct_Attr           = 14
Branch_ID           = 15
PBU_Inter_Order_No  = 16
PBU_Inter_Txt       = 17
Aud_Type            = 18
Order_Type          = 19
Trade_Restr_Type    = 20
Order_Stat          = 21
Trade_Dir           = 22
Order_Restr_Type    = 23
Short_Sell_Flag     = 24
Credit_Type         = 25
Stat_PBU_ID         = 26
Order_Bal           = 27
Trade_Flag          = 28

######### pandas version to much memory overhead ###############

def pandas_version_stock_count():

    data = pd.read_csv('SB_sample.txt', sep="|", header=None)
    # data = pd.read_csv('sortSBAll.txt', sep="|", header=None)

    print(data.head(10))

    df = data.groupby(11).count()[1].reset_index(name='count').sort_values(['count'], ascending=False)

    df.columns = ["Sec_Code", "count"]

    print(df.head(100))

###### normal version: freuqency stock code count#########

def frequency_stock_code_count():
    fp = open('/home/myc/workspace/datasets/SSE/sortSBAll.txt')
    # fp = open('SB_sample.txt')

    sec_code_map = {}

    text = fp.readline()
    while text:
        textArr = text.split("|")
        if textArr[Sec_Code] not in sec_code_map:
            sec_code_map[textArr[Sec_Code]] = 0
        sec_code_map[textArr[Sec_Code]] = sec_code_map[textArr[Sec_Code]] + 1
        text = fp.readline()
    fp.close()

    sorted_sec_code_map = {k: v for k, v in sorted(sec_code_map.items(), key=lambda x: x[1], reverse=True)}

    # with open('stock_code_freqcount.csv', 'w') as csv_file:
    #     writer = csv.writer(csv_file)
    #     for key, value in sorted_sec_code_map.items():
    #         writer.writerow([key, value])

###### count output rate of seconds ######

def stock_input_rate_metrics(filter):
    fp = open('/home/myc/workspace/datasets/SSE/sortSBAll.txt')
    # fp = open('SB_sample.txt')

    # structure of counting map:
    #sec_code_count_map -> {
    #   timestamp1 -> {
    #       total_rate -> total_count,
    #       stock_code : count,
    #       stock_code : count,
    #       ....
    #   }
    #   timestamp2 -> {
    #       total_rate -> total_count,
    #       stock_code : count,
    #       stock_code : count,
    #   }
    #   ....
    #}
    sec_code_count_map = {}

    text = fp.readline()
    while text:
        textArr = text.split("|")
        if len(textArr) < 10:
            continue
        # order_time = datetime.datetime.strptime(textArr[Last_Upd_Date] + " " + textArr[Last_Upd_Time], '%Y%m%d %H:%M:%S')
        # order_time.timestamp()
        # timestamp = order_time.timestamp()
        timestamp = textArr[Last_Upd_Time]
        if timestamp not in sec_code_count_map:
            sec_code_count_map[timestamp] = {}
            sec_code_count_map[timestamp]["total_rate"] = 0
            for stock_id in filter:
                sec_code_count_map[timestamp][stock_id] = 0
        if textArr[Sec_Code] in filter:
            sec_code_count_map[timestamp][textArr[Sec_Code]] = sec_code_count_map[timestamp][textArr[Sec_Code]] + 1
        sec_code_count_map[timestamp]["total_rate"] = sec_code_count_map[timestamp]["total_rate"] + 1
        text = fp.readline()
    fp.close()

    # fp = open('stock_input_metrics.csv', "default_window_1s")
    # json_view = json.dumps(sec_code_count_map, sort_keys=True)
    # fp.write(json_view)


    print(['total_rate'] + filter)
    df = pd.DataFrame.from_dict(sec_code_count_map, orient='index', columns= ['total_rate'] + filter)
    df.to_csv("stock_input_metrics.csv")
    # print(df["580026"].sum)

    # with open('stock_input_metrics.csv','wb') as f:
    #     default_window_1s = csv.writer(f)
    #     row = []
    #     for tm, tmDict in sec_code_count_map:
    #         row.append(tm)
    #         for key, count in tmDict:
    #             row.append(count)
    #         default_window_1s.writerows(sec_code_count_map.items())

if __name__ == "__main__":
    # stock_input_rate_metrics(filter=["580026", "600111", "600089", "600584", "600175"])
    frequency_stock_code_count()