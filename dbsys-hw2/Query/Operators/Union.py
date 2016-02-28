from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Union(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)
    self.lhsPlan  = lhsPlan
    self.rhsPlan  = rhsPlan
    self.validateSchema()

  # Unions must have equivalent schemas on their left and right inputs.
  def validateSchema(self):
    if self.lhsPlan.schema().match(self.rhsPlan.schema()):
      schemaName       = self.operatorType() + str(self.id())
      schemaFields     = self.lhsPlan.schema().schema()
      self.unionSchema = DBSchema(schemaName, schemaFields)
    else:
      raise ValueError("Union operator type error, mismatched input schemas")

  def schema(self):
    return self.unionSchema

  def inputSchemas(self):
    return [self.lhsPlan.schema(), self.rhsPlan.schema()]

  def operatorType(self):
    return "UnionAll"

  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for union operator.
  # The iterator must be set up to deal with input iterators and handle both pipelined and
  # non-pipelined cases
  def __iter__(self):
    #raise NotImplementedError
    self.initializeOutput()
    self.lhsIterator = iter(self.lhsPlan)
    self.rhsIterator = iter(self.rhsPlan)
    self.lhsFinished = False
    self.rhsFinished = False

    if not self.pipelined:
      self.outputIterator = self.processAllPages()

    return self

  # Method used for iteration, doing work in the process. Handle pipelined and non-pipelined cases
  def __next__(self):
    #raise NotImplementedError
    if self.pipelined:
      while not(self.lhsFinished or self.rhsFinished or self.isOutputPageReady()):
        try:
          pageId, page = next(self.lhsIterator)
          self.processInputPage(pageId, page)
        except StopIteration:
          self.lhsFinished = True

        try:
          pageId, page = next(self.rhsIterator)
          self.processInputPage(pageId, page)
        except StopIteration:
          self.rhsFinished = True

      return self.outputPage() # a list

    else:
      return next(self.outputIterator)

  # Page processing and control methods

  # Page-at-a-time operator processing
  # For union all, this copies over the input tuple to the output
  def processInputPage(self, pageId, page):
    schema = self.rhsPlan.schema()
    #if set(locals().keys()).isdisjoint(set(schema.fields)):
    for inputTuple in page:
        # Load tuple fields into the select expression context
        #selectExprEnv = self.loadSchema(schema, inputTuple)

        # Execute the predicate.
        #if eval(self.selectExpr, globals(), selectExprEnv):
      self.emitOutputTuple(inputTuple)
    #else:
    #  raise ValueError("Overlapping variables detected with operator schema")



  # Set-at-a-time operator processing
  def processAllPages(self):
    #raise NotImplementedError
    if self.lhsIterator is None:
      self.lhsIterator = iter(self.lhsPlan)

    if self.rhsIterator is None:
      self.rhsIterator = iter(self.rhsPlan)

    # Process all pages from the child operator.
    try:
      for (pageId, page) in self.lhsIterator:
        self.processInputPage(pageId, page)

      for (pageId, page) in self.rhsIterator:
        self.processInputPage(pageId, page)

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

    # To support pipelined operation, processInputPage may raise a
    # StopIteration exception during its work. We catch this and ignore in batch mode.
    except StopIteration:
      pass

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())
