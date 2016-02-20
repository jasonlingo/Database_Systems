from Utils.WorkloadGenerator import WorkloadGenerator
from Storage.File            import StorageFile
from Storage.Page            import Page
from Storage.SlottedPage     import SlottedPage
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
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

color_sequence = ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c',
                  '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5',
                  '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f',
                  '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5']

plt.xlim(0, 1)

throughputResult = []
diskUsageResult = []
exp = []

# for pageClass in [Page, SlottedPage]:
for pageClass in [SlottedPage]:
    StorageFile.defaultPageClass = pageClass
    # for pageSize in [4096, 32768]:
    for pageSize in [4096]:
        # for workloadMode in [1, 2, 3, 4]:
        for workloadMode in [1]:
            throughputResult.append([])
            exp.append([])
            diskUsageResult.append([])
            for scaleFactor in [0.2 * i for i in range(1, 6)]:
                print("Experiment:", pageClass.__name__, scaleFactor, pageSize, workloadMode)
                wg = WorkloadGenerator()
                tuples, throughput, execTime = wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)
                diskUsage = (sum([os.path.getsize(f) for f in os.listdir('./data') if os.path.isfile(f)]))
                print(tuples, throughput, execTime, diskUsage)
                # expTag = (pageClass.__name__, scaleFactor, pageSize, workloadMode)

                exp[-1].append(pageClass.__name__ + ", " + str(scaleFactor) + ", " + str(pageSize) + ", " + str(workloadMode))
                throughputResult[-1].append((scaleFactor, throughput))
                diskUsageResult[-1].append((scaleFactor, diskUsage))

# plot throughput
plt.title("Throughput")
plt.xticks([0.2, 0.4, 0.6, 0.8, 1.0])
fig = plt.figure()
fig.set_size_inches(18.5, 10.5)
ax = fig.add_subplot(111)
fig.suptitle("Disk Usage Chart")
ax.set_xlabel('Scale factor')
ax.set_ylabel('Throughput (no. of tuple / second)')
for i in range(len(exp)):
    x = [d[0] for d in throughputResult[i]]
    y = [d[1] for d in throughputResult[i]]
    plt.plot(x, y, "-")
    ax.text(x[-1], y[-1], exp[i])
plt.savefig('Throughput.png')

plt.clf()

# plot disk usage
plt.title("Disk Usage")
fig = plt.figure()
ax = fig.add_subplot(111)
fig.suptitle("Disk Usage Chart")
ax.set_xlabel('Scale factor')
ax.set_ylabel('Disk usage (bytes)')
for i in range(len(exp)):
    x = [d[0] for d in diskUsageResult[i]]
    y = [d[1] for d in diskUsageResult[i]]
    plt.plot(x, y, "-")
plt.savefig('DiskUsage.png')