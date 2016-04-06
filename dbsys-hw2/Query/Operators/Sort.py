from Catalog.Schema import DBSchema
from Query.Operator import Operator
import sys


# Operator for External Sort
class Sort(Operator):

  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)
    self.subPlan     = subPlan
    self.sortKeyFn   = kwargs.get("sortKeyFn", None)
    self.sortKeyDesc = kwargs.get("sortKeyDesc", None)

    if self.sortKeyFn is None or self.sortKeyDesc is None:
      raise ValueError("No sort key extractor provided to a sort operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.subPlan.schema()

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "Sort"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]


  # Iterator abstraction for external sort operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputIterator = iter(self.subPlan)
    self.inputFinished = False
    self.partitionFiles = []
    self.partFileNo = 0
    self.outputIterator = self.processAllPages()
    return self
    #raise NotImplementedError

  def __next__(self):
    return next(self.outputIterator)

  # Page processing and control methods


  def accessPageBlock(self, bufPool, pageIterator):
    resultPages = []
    maxBlockSize = bufPool.numPages() - 2
    while not (self.inputFinished or bufPool.numFreePages() == 0 or maxBlockSize == 0):
      try:
        pageId, page = next(pageIterator)
        resultPages.append(bufPool.getPage(pageId, True))
        maxBlockSize -= 1
      except StopIteration:
        self.inputFinished = True

    return resultPages

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    schema = self.subPlan.schema()
    if set(locals().keys()).isdisjoint(set(schema.fields)):
      for inputTuple in page:
        # Load tuple fields into the select expression context
        selectExprEnv = self.loadSchema(schema, inputTuple)

        # Execute the predicate.
        if eval(self.selectExpr, globals(), selectExprEnv):
          self.emitOutputTuple(inputTuple)
    else:
      raise ValueError("Overlapping variables detected with operator schema")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if not self.inputIterator:
      self.inputIterator = iter(self.subPlan)

    bufPool = self.storage.bufferPool
    pageBlock = self.accessPageBlock(bufPool, self.inputIterator)

    while pageBlock:
      tuples = []
      for page in pageBlock:
        tuples.extend(self.schema().unpack(tup) for tup in page)
        bufPool.unpinPage(page.pageId)
      tuples.sort(key=self.sortKeyFn, reverse=True)
      self.emitTupleToPartFile(tuples)
      pageBlock = self.accessPageBlock(bufPool, self.inputIterator)

    self.mergeSort()
    self.outputSortedFile()

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())

  def toTuple(self, x):
    return x if isinstance(x, tuple) else (x,)

  def outputSortedFile(self):
    relId = self.partitionFiles.pop(0)
    _, file = self.storage.fileMgr.relationFile(relId)
    for tup in file.tuples():
      self.emitOutputTuple(tup)

  def mergeSort(self):
    while self.partitionFiles:
      if len(self.partitionFiles) == 1:
        # the last one is the final sorted file
        break

      tuples = []
      relId1 = self.partitionFiles.pop(0)
      relId2 = self.partitionFiles.pop(0)
      _, file1 = self.storage.fileMgr.relationFile(relId1)
      _, file2 = self.storage.fileMgr.relationFile(relId2)
      file1TupIter = file1.tuples()
      file2TupIter = file2.tuples()
      tup1 = self.schema().unpack(next(file1TupIter))
      tup2 = self.schema().unpack(next(file2TupIter))
      while tup1 and tup2:
        if list(map(self.sortKeyFn, [tup1])) >= list(map(self.sortKeyFn, [tup2])):
          tuples.append(tup1)
          tup1 = self.schema().unpack(next(file1TupIter))
        else:
          tuples.append(tup2)
          tup2 = self.schema().unpack(next(file2TupIter))
      while tup1:
        tuples.append(tup1)
        tup1 = self.schema().unpack(next(file1TupIter))
      while tup2:
        tuples.append(tup2)
        tup2 = self.schema().unpack(next(file2TupIter))

      self.emitTupleToPartFile(tuples)
      self.deletePartitionFiles([relId1, relId2])

  def deletePartitionFiles(self, deleteFiles):
    for relId in deleteFiles:
      self.storage.removeRelation(relId)

  def createPartitionFile(self):
    relId = self.relationId() + "_tmp_" + str(self.partFileNo)
    self.partFileNo += 1

    if self.storage.hasRelation(relId):
      self.storage.removeRelation(relId)

    self.storage.createRelation(relId, self.schema())
    self.partitionFiles.append(relId)

  def emitTupleToPartFile(self, tuples):
    # for each block of tuples, create a new file to store the sorted result
    self.createPartitionFile()
    relId = self.partitionFiles[-1]

    _, file = self.storage.fileMgr.relationFile(relId)
    # pageId = file.availablePage()
    # page = self.storage.bufferPool.getPage(pageId)
    for tup in tuples:
      file.insertTuple(self.schema().pack(tup))

  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(sortKeyDesc='" + str(self.sortKeyDesc) + "')"
