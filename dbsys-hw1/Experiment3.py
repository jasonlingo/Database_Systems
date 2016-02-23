from Utils.WorkloadGenerator import WorkloadGenerator
from Storage.File            import StorageFile
from Storage.Page            import Page
from Storage.SlottedPage     import SlottedPage
# import matplotlib
# matplotlib.use('Agg')
# import matplotlib.pyplot as plt
import os

# Path to the folder containing csvs (on ugrad cluster)
dataDir = 'datasets/tpch-sf0.01/'

# Pick a page class, page size, scale factor, and workload mode:
# StorageFile.defaultPageClass = Page   # Contiguous Page
# pageSize = 4096                       # 4Kb
# scaleFactor = 0.5                     # Half of the data
# workloadMode = 1                      # Sequential Reads

# Run! Throughput will be printed afterwards.
# Note that the reported throughput ignores the time
# spent loading the dataset.
#
# color_sequence = ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c',
#                   '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5',
#                   '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f',
#                   '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5']

throughputResult = []
diskUsageResult = []
exp = []
wg = WorkloadGenerator()
for pageClass in [Page, SlottedPage]:
    StorageFile.defaultPageClass = pageClass
    for pageSize in [4096, 32768]:
        for workloadMode in [1, 2, 3, 4]:
            exp.append(pageClass.__name__ + ", " + str(pageSize) + ", " + str(workloadMode))
            # throughputResult.append([])
            # diskUsageResult.append([])
            for scaleFactor in [0.2 * i for i in range(1, 6)]:
                print("-------------------------------------")
                print("Experiment:", pageClass.__name__, scaleFactor, pageSize, workloadMode)
                (tuples, throughput, execTime) = wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)

                # calculate disk usage
                folder_size = 0
                for (path, dirs, files) in os.walk("./data"):
                    for file in files:
                        if file not in [".DS_Store", ]:
                            filename = os.path.join(path, file)
                            folder_size += os.path.getsize(filename)
                            #os.remove(filename)
                print("Disk usage:", folder_size)

                # throughputResult[-1].append((scaleFactor, throughput))
                # diskUsageResult[-1].append((scaleFactor, folder_size))

#
# # plot throughput
# plt.title("Throughput")
# fig, ax = plt.subplots(figsize=(20, 15))
# plt.xticks([0.2 * i for i in range(1, 6)])
# fig.suptitle("Throughput Chart\n\nExperiment parameters: page type, page size, workload mode")
# ax.set_xlabel('Scale factor')
# ax.set_ylabel('Throughput (no. of tuple / second)')
# # ax.text(0, 0.1, "Experiment: page type, page size, workload mode")
# for i in range(len(exp)):
#     x = [d[0] for d in throughputResult[i]]
#     y = [d[1] for d in throughputResult[i]]
#     plt.plot(x, y, "-", color=color_sequence[i], lw=2.5)
#     ax.text(x[-1]+0.01, y[-1], exp[i], size=12, color=color_sequence[i])
# plt.savefig('Throughput.png')
#
# plt.clf()
#
# # plot disk usage
# plt.title("Disk Usage")
# fig, ax = plt.subplots(figsize=(20, 15))
# plt.xticks([0.2 * i for i in range(1, 6)])
# fig.suptitle("Disk Usage Chart\n\nExperiment parameters: page type, page size, workload mode")
# ax.set_xlabel('Scale factor')
# ax.set_ylabel('Disk usage (bytes)')
# # ax.text(0, 0.1, "Experiment: page type, page size, workload mode")
# for i in range(len(exp)):
#     x = [d[0] for d in diskUsageResult[i]]
#     y = [d[1] for d in diskUsageResult[i]]
#     plt.plot(x, y, "-", color=color_sequence[i], lw=2.5)
#     ax.text(x[-1]+0.01, y[-1], exp[i], size=12, color=color_sequence[i])
# plt.savefig('DiskUsage.png')