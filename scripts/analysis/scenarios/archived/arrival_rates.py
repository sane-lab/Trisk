import datetime
import time
import matplotlib.pyplot as plt

# file = "/root/SSE-kafka-producer/sb-opening-50ms.txt"
file = "/home/myc/workspace/datasets/SSE/sb-4hr-50ms.txt"
# file = "/home/myc/workspace/SSE-anaysis/data/source/SB_sample.txt"
fp = open(file)

Order_No = 0
Tran_Maint_Code = 1
Last_Upd_Date = 2
Last_Upd_Time = 3
Last_Upd_Time_Dec = 4
Order_Price = 8
Order_Exec_Vol = 9
Order_Vol = 10
Sec_Code = 11
Trade_Dir = 22

lines = fp.readlines()
dists = {}
# keygroups
keygroups = 1

prev = {}

start_ts = {}


def parse_from_lines(lines):
    for line in lines:
        if line == "end":
            continue
        textArr = line.split("|")
        # time = int(textArr[Last_Upd_Time].replace(":", ""))
        if len(textArr) < 26:
            continue
        dt = datetime.datetime.strptime(textArr[Last_Upd_Date] + " " + textArr[Last_Upd_Time], "%Y%m%d %H:%M:%S")
        ts = int(dt.timestamp())

        # ts = int(time.mktime(dt.timetuple()))
        stockid = int(textArr[Sec_Code])
        keygroup = stockid % keygroups

        if keygroup not in start_ts:
            start_ts[keygroup] = ts

        if keygroup not in dists:
            dists[keygroup] = {}
            prev[keygroup] = ts
        # if ts - prev[keygroup] > 1:
        #     for i in range(1, (ts-prev[keygroup])):
        #         dists[keygroup][ts+i] = 0
        #     prev[keygroup] = ts
        cnt = dists[keygroup].get(ts, 0)
        cnt += 1
        dists[keygroup][ts] = cnt

        lists = sorted(dists[0].items())
        x, y = zip(*lists)

        plt.plot(list(dists[0].values()))
        plt.ylim(0, 3000)
        plt.grid()
        plt.xlabel("Timestamp(s)")
        plt.ylabel("Rate(e/s)")
        plt.savefig("rate.png")


def parse_from_end_point(lines):
    filter = ["580026", "600111", "600089", "600584", "600175"]
    end_counter = 0
    ts_count = 0
    ts_count_perkey = {"580026": 0, "600111": 0, "600089": 0, "600584": 0, "600175": 0}
    ts_list = []
    ts_list_perkey = {"580026": [], "600111": [], "600089": [], "600584": [], "600175": []}
    slot = 0
    count10s = 0
    for line in lines:
        ts_count += 1
        textArr = line.split("|")
        if len(textArr) > 10:
            if textArr[Sec_Code] in filter:
                ts_count_perkey[textArr[Sec_Code]] += 1
        if line == "end\n":
            end_counter += 1
            if end_counter == 20:
                # print(ts_count)
                ts_list.append(ts_count)
                for key in ts_list_perkey:
                    ts_list_perkey[key].append(ts_count_perkey[key])
                if slot < 10:
                    slot += 1
                    count10s += ts_count
                # else:
                # print(count10s)
                ts_count = 0
                end_counter = 0
                ts_count_perkey = {"580026": 0, "600111": 0, "600089": 0, "600584": 0, "600175": 0}

    fp.close()

    # print(len(ts_list))
    fig = plt.figure(figsize=(10, 5))
    figure = fig.add_subplot(111)
    figure.plot(ts_list)
    lines = [None] * (len(ts_count_perkey))
    for key in ts_list_perkey:
        print(ts_list_perkey[key])
        figure.plot(ts_list_perkey[key])
    axes = plt.gca()
    # axes.set_ylim([0, 2500])
    axes.set_xlim([0, 500])
    plt.yscale("log")
    # plt.ylim(0,2500)
    plt.xlabel("Timestamp(s)")
    plt.ylabel("Rate(e/s)")
    plt.savefig("rate.png")


# parse_from_lines(lines)
parse_from_end_point(lines)

# fig, axs = plt.subplots(8, 8)
#
# for i in range(8):
#     for j in range(8):
#         key = list(dists.keys())[i*8 + j]
#         # align
#         # dists[key] = dict((skey-start_ts[key], value) for (skey, value) in dists[key].items())
#         lists = sorted(dists[key].items())
#         x, y  = zip(*lists)
#         axs[i,j].plot(x, y, label=key)
#         # axs[i,j].plot(dists[key].keys(), dists[key].values(), label=key)
#         axs[i,j].legend(loc="upper right")
#         axs[i,j].set_ylim(0, 150)
#
# plt.show()

# lists = sorted(dists[0].items())
# x, y  = zip(*lists)
#
# plt.plot(list(dists[0].values()))
# plt.ylim(0,3000)
# plt.xlabel("Timestamp(s)")
# plt.ylabel("Rate(e/s)")
# plt.show()


# start_dt = datetime.datetime.strptime("20100913 09:30:00", "%Y%m%d %H:%M:%S")
# end_dt = datetime.datetime.strptime("20100913 15:20:13", "%Y%m%d %H:%M:%S")
# start_ts = int(start_dt.timestamp())
# end_ts = int(end_dt.timestamp())
# print(15280729/(end_ts-start_ts - 6000))
