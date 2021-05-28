import os


# the average latency
def averageLatency(lines, expName):
    # get all latency of all files, calculate the average
    totalLatency = 0
    count = 0
    for line in lines:
        if line.split(": ")[-1][:-1] != "NaN":
            totalLatency += float(line.split(": ")[-1][:-1])
            count += 1

    if count > 0:
        print("avg latency", ": ", totalLatency / count)
    else:
        print("avg latency", ": ", 0)


# the average reconfig time
def averageCompletionTime(lines, expName):
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
            stats.append("{}: {}".format(key, totalTime / count))
        else:
            stats.append("{}: {}".format(key, 0))
    # reconfig time breakdown
    print(stats)


root = "/data/raw"

for expName in os.listdir(root):
    print("---{}---".format(expName))
    for file in os.listdir(os.path.join(root, expName)):
        file_path = os.path.join(root, expName, file)
        if file == "timer.output":
            averageCompletionTime(open(file_path).readlines(), expName)
        elif file == "Splitter FlatMap-0.output":
            averageLatency(open(file_path).readlines(), expName)
