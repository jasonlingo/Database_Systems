from collections import Counter


class StatisticsManager:
    """
    A class that manages the statistics of the database. Currently, it has only histograms for tuples.
    """


    def __init__(self, db, bucketNum=10):
        self.db = db
        self.storage = self.db.storageEngine()
        self.bucketNum = bucketNum
        self.histogram = {}

    def setBucketNum(self, bucketNum):
        """
        Specify the number of buckets for one variable.
        :param bucketNum:
        """
        self.bucketNum = bucketNum

    def constructHist(self):
        """
        Construct equi-width histograms.
        For each column whose type is number in each table, build a histogram for it.
        Step:
          1 Compute boundaries b_i from High(A, R) and Low(A, R).
          2 Scan R once sequentially.
          3 While scanning, maintain B running tuple frequency counters, one for each bucket.
        """

        if not self.db:
            return

        # get all table names
        relations = self.storage.relations()

        # for each table, construct a histogram for each variable that is a number type.
        for rel in relations:
            self.genTableHistogram(rel)

    def genTableHistogram(self, relation):
        if relation not in self.histogram:
            self.histogram[relation] = {}

        # for each attribute in the schema, if the attribute is a number type, add a key in the histogram
        schema = self.db.relationSchema(relation)
        for k, v in schema.schema():
            if isNumType(v):
                if k not in self.histogram[relation]:
                    self.histogram[relation][k] = []

        # scan all tuples for the current relation and assign each value to corresponding histogram collections
        for tup in self.storage.tuple(relation):
            data = schema.unpack(tup)
            for k, v in schema.schema():
                if k in self.histogram[relation]:
                    self.histogram[relation][k].append(v)

        # build buckets for the current relation.
        # the bucket size will be (maxR - minR + 1) / bucket number
        # ref: lecture 9, page 22
        for k in self.histogram[relation]:
            data = self.histogram[relation][k]
            maxR = max(data)
            minR = min(data)
            bucketSize = (maxR - minR + 1) / self.bucketNum
            boundaries = [minR + i * bucketSize for i in range(1, self.bucketNum + 1)]

            # start counting, if one data is less than boundary i+1 and larger than boundary i,
            # then add 1 to bucket i
            self.histogram[relation][k] = {}
            for b in boundaries:
                self.histogram[relation][k][b] = 0

            for d in data:
                for boundary in boundaries:
                    if d <= boundary:
                        self.histogram[relation][k][boundary] += 1

    def getHistogram(self, rel):
        """
        :param rel: relation name
        :return: return the histogram of the relation that has the count for each attributes (number type).
        """
        return self.histogram[rel]


def isNumType(x):
    for t in ['short', 'int', 'float', 'double']:
        if t in x:
            return True
    return False

