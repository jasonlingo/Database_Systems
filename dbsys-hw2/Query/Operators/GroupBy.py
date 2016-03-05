from Catalog.Schema import DBSchema
from Query.Operator import Operator

class GroupBy(Operator):
  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined group-by-aggregate operator not supported")

    self.subPlan     = subPlan
    self.subSchema   = subPlan.schema()
    self.groupSchema = kwargs.get("groupSchema", None)
    self.aggSchema   = kwargs.get("aggSchema", None)
    self.groupExpr   = kwargs.get("groupExpr", None)
    self.aggExprs    = kwargs.get("aggExprs", None)
    self.groupHashFn = kwargs.get("groupHashFn", None)

    self.validateGroupBy()
    self.initializeSchema()

  # Perform some basic checking on the group-by operator's parameters.
  def validateGroupBy(self):
    requireAllValid = [self.subPlan, \
                       self.groupSchema, self.aggSchema, \
                       self.groupExpr, self.aggExprs, self.groupHashFn ]

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete group-by specification, missing a required parameter")

    if not self.aggExprs:
      raise ValueError("Group-by needs at least one aggregate expression")

    if len(self.aggExprs) != len(self.aggSchema.fields):
      raise ValueError("Invalid aggregate fields: schema mismatch")

  # Initializes the group-by's schema as a concatenation of the group-by
  # fields and all aggregate fields.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    self.outputSchema = DBSchema(schema, fields)

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "GroupBy"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for selection operator.
  def __iter__(self):
    self.initializeOutput()
    self.partitionFiles = {}
    self.outputIterator = self.processAllPages()
    return self

  def __next__(self):
    return next(self.outputIterator)

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for GroupBy")

  # Set-at-a-time operator processing
  def processAllPages(self):
    # create parition according to the given hashing group key
    # assign tuple to a partition according to the hashed key value
    for pageId, page in iter(self.subPlan):
      for tupleData in page:
        key = self.groupExpr(self.subSchema.unpack(tupleData))
        groupId = self.groupHashFn(self.toTuple(key))
        self.emitTupleToGroup(groupId, tupleData)

    # aggregate data within every group
    aggrData = {}
    for relId in self.partitionFiles.values():
      partitionFile = self.storage.fileMgr.relationFile(relId)[1]
      for _, page in partitionFile.pages():
        for tuple in page:
          tupleData = self.subSchema.unpack(tuple)
          key = self.groupExpr(tupleData)
          if key in aggrData:
            temp = aggrData[key]
            aggrData[key] = [self.aggExprs[i][1](temp, tupleData)\
                             for i in range(len(self.aggExprs))]
          else:
            aggrData[key] = [ self.aggExprs[i][1](self.aggExprs[i][0], tupleData) \
                             for i in range(len(self.aggExprs))]

    for e in aggrData:
      outputTuple = self.outputSchema.instantiate(e, aggrData[e][0], aggrData[e][1])
      self.emitOutputTuple(self.outputSchema.pack(outputTuple))

    return self.storage.pages(self.relationId())

  def toTuple(self, x):
    return x if isinstance(x, tuple) else (x,)

  def createPartitionFile(self, groupId):
    relId = self.relationId() + "_tmp_" + str(groupId)

    if self.storage.hasRelation(relId):
      self.storage.removeRelation(relId)

    self.storage.createRelation(relId, self.subSchema)
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
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"
