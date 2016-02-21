import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pickle


color_sequence = ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c',
                  '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5',
                  '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f',
                  '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5']



with open("exp", 'rb') as f:
    exp = pickle.load(f)
with open("throughput", 'rb') as f:
    throughputResult = pickle.load(f)
with open("diskusage", 'rb') as f:
    diskUsageResult = pickle.load(f)

print(diskUsageResult)

# plot throughput
plt.title("Throughput")
fig, ax = plt.subplots(figsize=(20, 15))
plt.xticks([0.2 * i for i in range(1, 6)])
fig.suptitle("Throughput Chart\n\nExperiment parameters: page type, page size, workload mode")
ax.set_xlabel('Scale factor')
ax.set_ylabel('Throughput (no. of tuple / second)')
for i in range(len(exp)):
    x = [d[0] for d in throughputResult[i]]
    y = [d[1] for d in throughputResult[i]]
    plt.plot(x, y, "-", color=color_sequence[i], lw=2.5)
    ax.text(x[-1]+0.01, y[-1], exp[i], size=12, color=color_sequence[i])
plt.savefig('Throughput.png')

plt.clf()

# plot disk usage
plt.title("Disk Usage")
fig, ax = plt.subplots(figsize=(20, 15))
plt.xticks([0.2 * i for i in range(1, 6)])
fig.suptitle("Disk Usage Chart\n\nExperiment parameters: page type, page size, workload mode")
ax.set_xlabel('Scale factor')
ax.set_ylabel('Disk usage (bytes)')
for i in range(len(exp)):
    x = [d[0] for d in diskUsageResult[i]]
    y = [d[1] for d in diskUsageResult[i]]
    plt.plot(x, y, "-", color=color_sequence[i], lw=2.5)
    ax.text(x[-1]+0.01, y[-1], exp[i], size=12, color=color_sequence[i])
plt.savefig('DiskUsage.png')