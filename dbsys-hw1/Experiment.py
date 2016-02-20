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

throughput = []
diskUsage = []

for pageClass in [Page, SlottedPage]:
    StorageFile.defaultPageClass = pageClass
    for pageSize in [4096, 32768]:
        for workloadMode in [1, 2, 3, 4]:
            for scaleFactor in [0.2 * i for i in range(1, 6)]:
                print("Experiment:", pageClass, scaleFactor, pageSize, workloadMode)
                wg = WorkloadGenerator()
                wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)
                diskUsage.append(sum([os.path.getsize(f) for f in os.listdir('.') if os.path.isfile(f)]))


plt.savefig('myfig')