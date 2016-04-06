import itertools

from Catalog.Schema import DBSchema
from Query.Operator import Operator
import sys

class Join(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined join operator not supported")

    self.lhsPlan    = lhsPlan
    self.rhsPlan    = rhsPlan
    self.joinExpr   = kwargs.get("expr", None)
    self.joinMethod = kwargs.get("method", None)
    self.lhsSchema  = kwargs.get("lhsSchema", None if lhsPlan is None else lhsPlan.schema())
    self.rhsSchema  = kwargs.get("rhsSchema", None if rhsPlan is None else rhsPlan.schema())

    self.lhsKeySchema   = kwargs.get("lhsKeySchema", None)
    self.rhsKeySchema   = kwargs.get("rhsKeySchema", None)
    self.lhsHashFn      = kwargs.get("lhsHashFn", None)
    self.rhsHashFn      = kwargs.get("rhsHashFn", None)

    self.validateJoin()
    self.initializeSchema()
    self.initializeMethod(**kwargs)

  # Checks the join parameters.
  def validateJoin(self):
    # Valid join methods: "nested-loops", "block-nested-loops", "indexed", "hash"
    if self.joinMethod not in ["nested-loops", "block-nested-loops", "indexed", "hash"]:
      raise ValueError("Invalid join method in join operator")

    # Check all fields are valid.
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      methodParams = [self.joinExpr]

    elif self.joinMethod == "indexed":
      methodParams = [self.lhsKeySchema]

    elif self.joinMethod == "hash":
      methodParams = [self.lhsHashFn, self.lhsKeySchema, \
                      self.rhsHashFn, self.rhsKeySchema]

    requireAllValid = [self.lhsPlan, self.rhsPlan, \
                       self.joinMethod, \
                       self.lhsSchema, self.rhsSchema ] \
                       + methodParams

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete join specification, missing join operator parameter")

    # For now, we assume that the LHS and RHS schema have
    # disjoint attribute names, enforcing this here.
    for lhsAttr in self.lhsSchema.fields:
      if lhsAttr in self.rhsSchema.fields:
        raise ValueError("Invalid join inputs, overlapping schema detected")


  # Initializes the output schema for this join.
  # This is a concatenation of all fields in the lhs and rhs schema.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.lhsSchema.schema() + self.rhsSchema.schema()
    self.joinSchema = DBSchema(schema, fields)

  # Initializes any additional operator parameters based on the join method.
  def initializeMethod(self, **kwargs):
    if self.joinMethod == "indexed":
      self.indexId = kwargs.get("indexId", None)
      if self.indexId is None or self.lhsKeySchema is None:
        raise ValueError("Invalid index for use in join operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.joinSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.lhsSchema, self.rhsSchema]

  # Returns a string describing the operator type
  def operatorType(self):
    readableJoinTypes = { 'nested-loops'       : 'NL'
                        , 'block-nested-loops' : 'BNL'
                        , 'indexed'            : 'Index'
                        , 'hash'               : 'Hash' }
    return readableJoinTypes[self.joinMethod] + "Join"

  # Returns child operators if present
  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for join operator.
  def __iter__(self):
    self.initializeOutput()
    self.lhsInputFinished = False
    self.lhsPartitionFiles = {}
    self.rhsPartitionFiles = {}
    self.outputIterator = self.processAllPages()
    return self

  def __next__(self):
    return next(self.outputIterator)

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.joinMethod == "nested-loops":
      return self.nestedLoops()

    elif self.joinMethod == "block-nested-loops":
      return self.blockNestedLoops()

    elif self.joinMethod == "indexed":
      return self.indexedNestedLoops()

    elif self.joinMethod == "hash":
      return self.hashJoin()

    else:
      raise ValueError("Invalid join method in join operator")


  ##################################
  #
  # Nested loops implementation
  #
  def nestedLoops(self):
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        # Load the lhs once per inner loop.
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

        for (rPageId, rhsPage) in iter(self.rhsPlan):
          for rTuple in rhsPage:
            # Load the RHS tuple fields.
            joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

            # Evaluate the join predicate, and output if we have a match.
            if eval(self.joinExpr, globals(), joinExprEnv):
              outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())

  ##################################
  #
  # Block nested loops implementation
  #
  # This attempts to use all the free pages in the buffer pool
  # for its block of the outer relation.

  # Accesses a block of pages from an iterator.
  # This method pins pages in the buffer pool during its access.
  # We track the page ids in the block to unpin them after processing the block.
  def accessPageBlock(self, bufPool, pageIterator):
    resultPages = []
    maxBlockSize = bufPool.numPages() - 2
    self.lhsInputFinished = False
    while not (self.lhsInputFinished or bufPool.numFreePages() == 0 or maxBlockSize == 0):
      try:
        pageId, page = next(pageIterator)
        resultPages.append(bufPool.getPage(pageId, True))
        maxBlockSize -= 1
      except StopIteration:
        self.lhsInputFinished = True

    return resultPages

  def blockNestedLoops(self, lhsJoinPlan=None, rhsJoinPlan=None):
    if not lhsJoinPlan:
      lhsJoinPlan = self.lhsPlan
    if not rhsJoinPlan:
      rhsJoinPlan = self.rhsPlan

    bufPool = self.storage.bufferPool
    lhsPageIterator = iter(lhsJoinPlan)
    pageBlock = self.accessPageBlock(bufPool, lhsPageIterator)
    rhsPageIterator = list(iter(rhsJoinPlan))

    rhsList = list(iter(rhsJoinPlan))

    while pageBlock:
      for lhsPage in pageBlock:
        for lTuple in lhsPage:
          # Load the lhs once per outer loop.
          joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)


          if self.joinMethod == "hash":
            # hash join
            lhsKey = self.lhsKeySchema.project(self.lhsSchema.unpack(lTuple), self.lhsKeySchema)
          for (_, rhsPage) in rhsPageIterator:
            for rTuple in rhsPage:
              # Load the RHS tuple fields.
              joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

              if self.joinMethod == "block-nested-loops":
                # Evaluate the join predicate, and output if we have a match.
                if eval(self.joinExpr, globals(), joinExprEnv):
                  outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                  self.emitOutputTuple(self.joinSchema.pack(outputTuple))
              else:
                # hash join
                rhsKey = self.rhsKeySchema.project(self.rhsSchema.unpack(rTuple), self.rhsKeySchema)
                if lhsKey == rhsKey:
                  outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                  self.emitOutputTuple(self.joinSchema.pack(outputTuple))

          # No need to track anything but the last output page when in batch mode.
          if self.outputPages:
            self.outputPages = [self.outputPages[-1]]

        bufPool.unpinPage(lhsPage.pageId)

      # get next block of pages
      pageBlock = self.accessPageBlock(bufPool, lhsPageIterator)

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())

  ##################################
  #
  # Indexed nested loops implementation
  #
  # TODO: test
  def indexedNestedLoops(self):
    if self.indexId:
      bufPool = self.storage.bufferPool

      for _, lhsPage in iter(self.lhsPlan):
        for lTuple in lhsPage:
          joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

          lJoinKey = self.lhsSchema.projectBinary(lTuple, self.lhsKeySchema)
          matches = self.storage.fileMgr.lookupByIndex(self.rhsSchema.name, self.indexId, lJoinKey)

          if matches:
            for rTupleId in matches:
              rhsPage = bufPool.getPage(rTupleId.pageId)
              rTuple = rhsPage.getTuple(rTupleId)
              joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))
              if eval(self.joinExpr, globals(), joinExprEnv):
                outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                self.emitOutputTuple(self.joinSchema.pack(outputTuple))
        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

      return self.storage.pages(self.relationId())

    else:
      raise ValueError("No index for index-join")

  ##################################
  #
  # Hash join implementation.
  #
  def hashJoin(self):
    # create partition files for both lhs and rhs
    self.partitionFile(self.lhsPlan, self.lhsSchema, self.lhsKeySchema, self.lhsHashFn, lhs=True)
    self.partitionFile(self.rhsPlan, self.rhsSchema, self.rhsKeySchema, self.rhsHashFn, lhs=False)

    # for each partition pairs (the same group id), join the tuples
    for lhsGroupId in self.lhsPartitionFiles:
      lhsRelId = self.lhsPartitionFiles[lhsGroupId]
      rhsRelId = self.rhsPartitionFiles.get(lhsGroupId, None)
      if rhsRelId:
        lhsPartFile = self.storage.fileMgr.relationFile(lhsRelId)[1]
        rhsPartFile = self.storage.fileMgr.relationFile(rhsRelId)[1]
        self.lhsInputFinished = False
        self.blockNestedLoops(lhsPartFile.pages(), rhsPartFile.pages())

    self.deletePartitionFiles()

    return self.storage.pages(self.relationId())

  def partitionFile(self, plan, planSchema, keySchema, hashFn, lhs):
    for _, page in iter(plan):
      for tupleData in page:
        data = self.loadSchema(planSchema, tupleData)
        groupId = eval(hashFn, globals(), data)
        self.emitTupleToGroup(groupId, tupleData, planSchema, lhs)

  def toTuple(self, x):
    return x if isinstance(x, tuple) else (x,)

  def deletePartitionFiles(self):
    for relId in self.lhsPartitionFiles.values():
      self.storage.removeRelation(relId)
    self.lhsPartitionFiles = {}

    for relId in self.rhsPartitionFiles.values():
      self.storage.removeRelation(relId)
    self.rhsPartitionFiles = {}

  def createPartitionFile(self, groupId, schema, lhs):
    relId = self.relationId() + "_tmp_" + ("lhs" if lhs else "rhs") + str(groupId)

    if self.storage.hasRelation(relId):
      self.storage.removeRelation(relId)

    self.storage.createRelation(relId, schema)
    if lhs:
      self.lhsPartitionFiles[groupId] = relId
    else:
      self.rhsPartitionFiles[groupId] = relId

  def emitTupleToGroup(self, groupId, tupleData, schema, lhs):
    if lhs:
      relId = self.lhsPartitionFiles.get(groupId, None)
    else:
      relId = self.rhsPartitionFiles.get(groupId, None)

    if not relId:
      self.createPartitionFile(groupId, schema, lhs)
      if lhs:
        relId = self.lhsPartitionFiles[groupId]
      else:
        relId = self.rhsPartitionFiles[groupId]

    _, partitionFile = self.storage.fileMgr.relationFile(relId)
    partitionFile.insertTuple(tupleData)

  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      exprs = "(expr='" + str(self.joinExpr) + "')"

    elif self.joinMethod == "indexed":
      exprs =  "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "indexKeySchema=" + self.lhsKeySchema.toString() ]
        ))) + ")"

    elif self.joinMethod == "hash":
      exprs = "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "lhsKeySchema=" + self.lhsKeySchema.toString() ,
            "rhsKeySchema=" + self.rhsKeySchema.toString() ,
            "lhsHashFn='" + self.lhsHashFn + "'" ,
            "rhsHashFn='" + self.rhsHashFn + "'" ]
        ))) + ")"

    return super().explain() + exprs
