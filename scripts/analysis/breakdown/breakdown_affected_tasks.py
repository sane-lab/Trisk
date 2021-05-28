import os
import utilities


def ReadFile(runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval, reconfig_type,
             affected_tasks, repeat_num):
    w, h = 5, 3
    y = [[0 for x in range(w)] for y in range(h)]

    for repeat in range(1, repeat_num+1):
        i = 0
        for affected_tasks in [2, 4, 6, 8, 10]:
            # ${reconfig_type}-${reconfig_interval}-${runtime}-${parallelism}-${per_task_rate}-${key_set}-${per_key_state_size}-${affected_tasks}
            exp = utilities.FILE_FOLER + '/trisk-{}-{}-{}-{}-{}-{}-{}-{}-{}'.format(reconfig_type, reconfig_interval,
                                                                                    runtime,
                                                                                    parallelism, per_task_rate, key_set,
                                                                                    per_key_state_size, affected_tasks,
                                                                                    repeat)
            file_path = os.path.join(exp, "timer.output")
            try:
                stats = utilities.breakdown(open(file_path).readlines())
                for j in range(3):
                    if utilities.timers_plot[j] not in stats:
                        y[j][i] = 0
                    else:
                        y[j][i] += stats[utilities.timers_plot[j]]
                i += 1
            except:
                print("Error while processing the file {}".format(exp))

    for j in range(h):
        for i in range(w):
            y[j][i] = y[j][i] / repeat_num

    return y


def draw(val):
    runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval, reconfig_type, affected_tasks, repeat_num = val

    # parallelism
    x_values = [2, 4, 6, 8, 10]
    y_values = ReadFile(runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval,
                        reconfig_type, affected_tasks, repeat_num)

    legend_labels = utilities.legend_labels

    utilities.DrawFigureV2(x_values, y_values, legend_labels,
                         'affected_tasks', 'breakdown (ms)',
                         'breakdown_{}_{}'.format(reconfig_type, "affected_tasks"), True)

# if __name__ == '__main__':
#     runtime, per_task_rate, parallelism, key_set, per_key_state_size, reconfig_interval, reconfig_type, affected_tasks = utilities.init()
#
#     try:
#         opts, args = getopt.getopt(sys.argv[1:], '-t::h', ['reconfig type', 'help'])
#     except getopt.GetoptError:
#         print('breakdown_parallelism.py -t type')
#         sys.exit(2)
#     for opt, opt_value in opts:
#         if opt in ('-h', '--help'):
#             print("[*] Help info")
#             exit()
#         elif opt == '-t':
#             print('Reconfig Type:', opt_value)
#             reconfig_type = str(opt_value)
#
#     draw()
