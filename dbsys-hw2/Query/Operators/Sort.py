from Catalog.Schema import DBSchema
from Query.Operator import Operator

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
    self.partitionFiles = {}

    self.inputFinished = False
    self.outputIterator = self.processAllPages()
    return self
    #raise NotImplementedError

  def __next__(self):
    return next(self.outputIterator)

  # Page processing and control methods

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    pass
    # raise NotImplementedError

  def accessPageBlock(self, bufPool, pageIterator):
    resultPages = []
    maxBlockSize = bufPool.numFreePages()
    self.InputFinished = False
    while not (self.InputFinished or bufPool.numFreePages() == 0 or maxBlockSize == 0):
      try:
        pageId, page = next(pageIterator)
        resultPages.append(bufPool.getPage(pageId, True))
        maxBlockSize -= 1
      except StopIteration:
        self.InputFinished = True

    return resultPages

  # Set-at-a-time operator processing
  def processAllPages(self):
    bufPool = self.storage.bufferPool
    pageIterator = iter(self.subPlan)

    pageBlock = self.accessPageBlock(bufPool, pageIterator)
    counter = 0 # id for partition

    while pageBlock:
      namedtuples = list()
      for page in pageBlock:

        for tuple in page:
          namedtuples.append(self.schema().unpack(tuple))
        bufPool.unpinPage(page.pageId)

      namedtuples.sort(key = self.sortKeyFn, reverse=True)
      for e in namedtuples:
        #print ("e is", e)
        self.emitTupleToGroup(counter, self.schema().pack(e))
      counter += 1

      pageBlock = self.accessPageBlock(bufPool, pageIterator)

    result = list()
    for relId in self.partitionFiles.values():
      file = self.storage.fileMgr.relationFile(relId)[1]
      for _, page in file.pages():
        temp = list()
        for tuple in page:
          temp.append(self.schema().unpack(tuple))
        result = self.merge(list(result), temp)

    for e in result:
      self.emitOutputTuple(self.schema().pack(e))

    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    self.deletePartitionFiles()
    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())

  def merge(self, temp1, temp2):
    result = temp1 + temp2
    result.sort(key = self.sortKeyFn, reverse=True)
    return result


  def deletePartitionFiles(self):
    for relId in self.partitionFiles.values():
      self.storage.removeRelation(relId)
    self.partitionFiles = {}

  def createPartitionFile(self, groupId):
    relId = self.relationId() + "_tmp_" + str(groupId)

    if self.storage.hasRelation(relId):
      self.storage.removeRelation(relId)

    self.storage.createRelation(relId, self.schema())
    self.partitionFiles[groupId] = relId

  def emitTupleToGroup(self, groupId, tupleData):
    relId = self.partitionFiles.get(groupId, None)
    if not relId:
      self.createPartitionFile(groupId)
      relId = self.partitionFiles[groupId]

    _, partitionFile = self.storage.fileMgr.relationFile(relId)
    pageId = partitionFile.availablePage()
    page = self.storage.bufferPool.getPage(pageId)
    page.insertTuple(tupleData)


  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(sortKeyDesc='" + str(self.sortKeyDesc) + "')"
