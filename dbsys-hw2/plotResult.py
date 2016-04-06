import numpy as np
import matplotlib.pyplot as plt



def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        plt.text(rect.get_x()+rect.get_width()/2., height * 1.02, '%.3f' % float(height),
                ha='center', va='bottom')


# === dataset 0.001 ===============================

n_groups = 2

#(query 1 on 0.001, query 1 on 0.01, query 2, query 3-1, query 3-2)
hash = [82.76287245750427, 12.855971813201904]
bnj = [112.15825939178467, 34.72383761405945]
sqlite = [0.017, 0.021]

hash_order1 = [1368.0099728107452]
hash_order2 = [1963.438952445984]
sqlite3 = [0.047]



fig, ax = plt.subplots()

index = np.arange(n_groups)
bar_width = 0.30

opacity = 0.4
error_config = {'ecolor': '0.3'}

rects1 = plt.bar(index, hash, bar_width,
                 alpha=opacity,
                 color='b',
                 error_kw=error_config,
                 label='Hash Join')

rects2 = plt.bar(index + bar_width, bnj, bar_width,
                 alpha=opacity,
                 color='r',
                 error_kw=error_config,
                 label='Block-Nested Join')

rects3 = plt.bar(index + bar_width * 2, sqlite, bar_width,
                 alpha=opacity,
                 color='g',
                 error_kw=error_config,
                 label='SQLite (less than 1s)')

autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

index = np.arange(1) + bar_width * 6


rects4 = plt.bar(index + bar_width, hash_order1, bar_width,
                 alpha=opacity,
                 color='c',
                 error_kw=error_config,
                 label='Query order 1 using hash join')

rects5 = plt.bar(index + bar_width * 2, hash_order2, bar_width,
                 alpha=opacity,
                 color='y',
                 error_kw=error_config,
                 label='Query order 2 using hash join')

rects6 = plt.bar(index + bar_width * 3, sqlite3, bar_width,
                 alpha=opacity,
                 color='g',
                 error_kw=error_config,
                )

autolabel(rects4)
autolabel(rects5)
autolabel(rects6)

index = np.arange(3)

plt.text(0.1, 1500, "Query order 1: join tables -> group by\nQuery order 2:join tables -> select -> group by", fontsize=8)
plt.xlabel('')
plt.ylabel('Query Time (second)')
plt.ylim(ymax=2200)
plt.title('Query Performance on Dataset-0.001')
plt.xticks(index + bar_width, ('Query 1', 'Query 2', 'Query 3'))
plt.legend(loc=0, prop={'size':12})

plt.tight_layout()
plt.savefig('dataset_0.001.png')






# === dataset 0.001 ===============================

n_groups = 2

#(query 1 on 0.001, query 1 on 0.01, query 2, query 3-1, query 3-2)
hash = [3010.7046897411346, 1004.539781332016]
bnj = [9783.611874341965, 34.72383761405945]
sqlite = [0.017, 0.095]

sqlite3 = [0.282]



fig, ax = plt.subplots()

index = np.arange(2)
bar_width = 0.30

opacity = 0.4
error_config = {'ecolor': '0.3'}

rects1 = plt.bar(index, hash, bar_width,
                 alpha=opacity,
                 color='b',
                 error_kw=error_config,
                 label='Hash Join')

rects2 = plt.bar(index + bar_width, bnj, bar_width,
                 alpha=opacity,
                 color='r',
                 error_kw=error_config,
                 label='Block-Nested Join')

rects3 = plt.bar(index + bar_width * 2, sqlite, bar_width,
                 alpha=opacity,
                 color='g',
                 error_kw=error_config,
                 label='SQLite (less than 1s)')

autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

index = np.arange(1) + bar_width * 6


rects4 = plt.bar(index + bar_width, sqlite3, bar_width,
                 alpha=opacity,
                 color='c',
                 error_kw=error_config,
                 )

autolabel(rects4)

index = np.arange(3)

plt.text(1, 5000, "(We only run SQlite on query 3 for this dataset)", fontsize=8)
plt.xlabel('')
plt.ylim(ymax=11000)
plt.ylabel('Query Time (second)')
plt.title('Query Performance on Dataset-0.01')
plt.xticks(index + bar_width, ('Query 1', 'Query 2', 'Query 3'))
plt.legend(loc=0, prop={'size':12})

plt.tight_layout()
plt.savefig('dataset_0.01.png')
