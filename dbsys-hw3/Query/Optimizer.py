import itertools
import sys

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Query.Operators.Union import Union
from Query.Operators.TableScan import TableScan
from Utils.ExpressionInfo import ExpressionInfo
from Catalog.Schema import DBSchema
from Query.StatisticsManager import StatisticsManager


class Optimizer:
  """
  A query optimization class.

  This implements System-R style query optimization, using dynamic programming.
  We only consider left-deep plan trees here.

  We provide doctests for example usage only.
  Implementations and cost heuristics may vary.

  >>> import Database, shutil, Storage
  >>> db = Database.Database()
  >>> try:
  ...   db.createRelation('department', [('did', 'int'), ('eid', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('employee', [('id', 'int'), ('age', 'int'), ('name', 'char(3)')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('salarys', [('sid', 'int'), ('salary', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('work', [('wid', 'int'), ('ewid', 'int')])
  ... except ValueError:
  ...   pass

 # Populate relation
  >>> schema = db.relationSchema('employee')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i, 'e' + str(i))) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  >>> schema = db.relationSchema('department')
  >>> for tup in [schema.pack(schema.instantiate(i, 4*i)) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  >>> schema = db.relationSchema('salarys')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  >>> schema = db.relationSchema('work')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  # >>> query6 = db.query().fromTable('employee').join(\
  #       db.query().fromTable('department').select({'eid':('eid','int')}),\
  #      method='block-nested-loops', expr='id == eid').where('age > 0').join(\
  #      db.query().fromTable('salarys'),\
  #      method='block-nested-loops', expr='sid == id').where('id > 0').select({'id':('id', 'int')})\
  #      .union(\
  #      db.query().fromTable('employee').join(\
  #       db.query().fromTable('department').select({'eid':('eid','int')}),\
  #      method='block-nested-loops', expr='id == eid').where('age > 0').join(\
  #      db.query().fromTable('salarys'),\
  #      method='block-nested-loops', expr='sid == id').where('id > 0').select({'id':('id', 'int')})\
  #      ).finalize()

  # >>> print(query6.explain())
  # >>> q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]
  # >>> print([tup for tup in q6results])
  #
  # >>> query6 = db.optimizer.optimizeQuery(query6)
  #
  # >>> print(query6.explain())
  # >>> q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]
  # >>> print([tup for tup in q6results])

  >>> query7 = db.query().fromTable('employee').join(\
        db.query().fromTable('department').select({'eid':('eid','int')}),\
       method='block-nested-loops', expr='id == eid').join(\
       db.query().fromTable('salarys'),\
       method='block-nested-loops', expr='sid == id').where('sid > 0').select({'age':('age', 'int')}).finalize()

  >>> query7.sample(1.0)
  >>> print(query7.explain())
  >>> q7results = [query7.schema().unpack(tup) for page in db.processQuery(query7) for tup in page[1]]
  >>> print([tup for tup in q7results])

  >>> query8 = db.query().fromTable('employee').join(\
        db.query().fromTable('department').select({'eid':('eid','int')}),\
       method='block-nested-loops', expr='id == eid').join(\
       db.query().fromTable('salarys'),\
       method='block-nested-loops', expr='sid == id').where('sid > 0').select({'name':('name', 'char(3)'), 'age':('age', 'int')}).finalize()

  >>> query8 = db.optimizer.optimizeQuery(query8)
  >>> query8.sample(1.0)
  >>> print(query8.explain())
  >>> q8results = [query8.schema().unpack(tup) for page in db.processQuery(query8) for tup in page[1]]
  >>> print([tup for tup in q8results])

  ### SELECT * FROM employee JOIN department ON id = eid
  # >>> try:
  # ...   db.createRelation('student', [('sid', 'int'), ('year', 'int')])
  # ...   db.createRelation('course', [('cid', 'int'), ('level', 'int')])
  # ... except ValueError:
  # ...   pass
  # >>> query7 = db.query().fromTable('employee').join( \
  #       db.query().fromTable('department'), \
  #       method='block-nested-loops', expr='id == eid').where('age > 5 or eid > 3').join( \
  #       db.query().fromTable('student'), \
  #       method = 'block-nested-loops', expr = 'eid == sid').join( \
  #       db.query().fromTable('course'), \
  #       method = 'block-nested-loops', expr = 'year == level').select({'id': ('id', 'int'), 'age':('age','int')}).finalize()
  #
  # >>> print(query7.explain() )
  #
  # >>> print( db.optimizer.optimizeQuery(query7).explain() )

  ## Clean up the doctest
  >>> shutil.rmtree(Storage.FileManager.FileManager.defaultDataDir)
  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}
    self.costCache = {}
    self.statsMgr = StatisticsManager(db)

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    if isinstance(plan, Plan):
      key = plan.getPlanKey()
      self.costCache[key] = cost
    else:
      self.costCache[plan] = cost

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    key = plan.getPlanKey()
    if key not in self.costCache:
        plan.sample(10.0)
        cost = plan.cost(estimated=True)
        self.addPlanCost(key, cost)
    return self.costCache[key]

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    """
    push down Select and Project operators
    :param plan:
    :return: Plan
    """
    return Plan(root=self.pushdownOperator(plan.root))

  def pushdownOperator(self, op):
    """
    Push down Select and Project
    :param op: current operator
    :return: an operator that its children has been processed by the pushdownOperator()
    """

    if op.operatorType() == "TableScan":
      # base relation
      return op

    elif op.operatorType() == "Select":
      # need to pushdown this operator close to its respective base relation
      return self.pushdownSelect(op)

    elif op.operatorType() == "Project":
      # need to pushdown this operator close to its respective base relation
      return self.pushdownProject(op)

    elif op.operatorType() in ["Sort", "GroupBy"]:
      # no need to push down these operators, but need to check their children
      op.subPlan = self.pushdownOperator(op.subPlan)
      return op

    elif op.operatorType() == "UnionAll" or "Join" in op.operatorType():
      # no need to push down these operators, but need to check their children (left and right children)
      op.lhsPlan = self.pushdownOperator(op.lhsPlan)
      op.rhsPlan = self.pushdownOperator(op.rhsPlan)
      return op

  def pushdownSelect(self, op):
    # pushdown its child first and then push down the current operator
    op.subPlan = self.pushdownOperator(op.subPlan)

    if op.subPlan.operatorType() in ["TableScan", "GroupBy", "Project"]:
      return op

    if op.subPlan.operatorType() == "Select":
      # Combine two consecutive Selects
      # According to the "Equivalence of Expression" on Lecture 8, page 24,
      # we can combine two consecutive Select operators and the result will
      # be the same.
      # i.e. Project_{predicateA ^ predicateB}(E) = Project_predicateA(Project_predicateB(E))
      op.selectExpr = "(%s) and (%s)" % (op.selectExpr, op.subPlan.selectExpr)
      op.subPlan = op.subPlan.subPlan
      return op

    elif op.subPlan.operatorType() == "Sort":
      # Select can be push down to the button of the Sort.
      # After push down the Select, we still have to try to push down it further.
      topOp = op.subPlan
      op.subPlan = op.subPlan.subPlan
      topOp.subPlan = self.pushdownOperator(op)
      return topOp

    elif op.subPlan.operatorType() == "UnionAll":
      # push select to the button of a union to reduce the union complexity
      # because the schemas of lhs and rhs are the same, we can use the original selectExpr for two subPlans
      topOp = op.subPlan
      lsubPlan = Select(op.subPlan.lhsPlan, op.selectExpr)
      rsubPlan = Select(op.subPlan.rhsPlan, op.selectExpr)
      topOp.lhsPlan = self.pushdownOperator(lsubPlan)
      topOp.rhsPlan = self.pushdownOperator(rsubPlan)
      return topOp

    elif "Join" in op.subPlan.operatorType():
      # First decompose the select expression into predicates.
      # Then check each predicate belongs to which subPlan.
      # For those remaining predicates (not assigned to a subPlan), create a new Select operator containing
      # the remaining predicates, and assign the new Select to the top of the Join operator.
      exprs = ExpressionInfo(op.selectExpr).decomposeCNF()

      lhsExprs = []
      rhsExprs = []
      remainingExprs = []

      lhsAttrs = set(op.subPlan.lhsPlan.schema().fields)
      rhsAttrs = set(op.subPlan.rhsPlan.schema().fields)

      # dispatch predicates to subPlans that have the attribute
      for e in exprs:
        attrs = ExpressionInfo(e).getAttributes()
        add = False
        if attrs.issubset(lhsAttrs):
          lhsExprs.append(e)
          add = True
        if attrs.issubset(rhsAttrs):
          rhsExprs.append(e)
          add = True
        if not add:
          remainingExprs.append(e)

      if lhsExprs:
        newLhsExpr = ' and '.join(lhsExprs)
        lhsSelect = Select(op.subPlan.lhsPlan, newLhsExpr)
        op.subPlan.lhsPlan = self.pushdownOperator(lhsSelect)

      if rhsExprs:
        newRhsExpr = ' and '.join(rhsExprs)
        rhsSelect = Select(op.subPlan.rhsPlan, newRhsExpr)
        op.subPlan.rhsPlan = self.pushdownOperator(rhsSelect)

      # deal with the remaining predicates
      if remainingExprs:
        newExpr = ' and '.join(remainingExprs)
        return Select(op.subPlan, newExpr)
      else:
        return op.subPlan

  def pushdownProject(self, op):
    # pushdown op's child first and then push down the current operator
    op.subPlan = self.pushdownOperator(op.subPlan)

    if op.subPlan.operatorType() in ["GroupBy", "TableScan", "Sort"]:
      return op

    if op.subPlan.operatorType() == "Project":
      # Two consecutive Projects, remove duplicate.
      # We cannot directly discard the lower Project because the upper Project might use
      # other new attributes (not original attributes) generated from the lower Project.
      # If the upper Project contains attributes that is the subset of the attributes of
      # the lower Project, then we can discard the lower Project.

      opAttr = set(op.schema().fields)
      subPlanAttr = set(op.subPlan.schema().fields)
      if opAttr.issubset(subPlanAttr):
        op.subPlan = op.subPlan.subPlan
        return self.pushdownProject(op)
      else:
        return op

    elif op.subPlan.operatorType() == "Select":
      # If the attributes of Select is a subset of that of Project, then we can swap
      # the order of Project and Select and pushdown the Project operator.

      projectAttrs = set(op.schema().fields)
      selectAttrs = ExpressionInfo(op.subPlan.selectExpr).getAttributes()
      if selectAttrs.issubset(projectAttrs):
        topOp = op.subPlan
        op.subPlan = op.subPlan.subPlan
        topOp.subPlan = self.pushdownOperator(op)
        return topOp
      else:
        return op

    elif op.subPlan.operatorType() == "UnionAll":
      # Because the attributes of the Project must be a subset of that of the UnionAll,
      # we can directly pushdown the Project to the subPlans of the UnionAll.

      topOp = op.subPlan
      lhsProject = Project(op.subPlan.lhsPlan, op.projectExprs)
      rhsProject = Project(op.subPlan.rhsPlan, op.projectExprs)
      topOp.lhsPlan = self.pushdownOperator(lhsProject)
      topOp.rhsPlan = self.pushdownOperator(rhsProject)
      return topOp

    elif "Join" in op.subPlan.operatorType():
      # First we extract all the attributes of the Project and the two subPlans of the Join.
      # Then, we check whether the attributes belong to which subPlan and assign it to the
      # Project expression.
      # For those attributes that have not been assigned, we keep a new Project containing
      # those remained attributes above the Join.
      lhsAttrs = set(op.subPlan.lhsPlan.schema().fields)
      rhsAttrs = set(op.subPlan.rhsPlan.schema().fields)
      lhsProjectExprs = {}
      rhsProjectExprs = {}
      skipCurrOp = True

      joinAttrs = ExpressionInfo(op.subPlan.joinExpr).getAttributes()

      for attr in op.projectExprs:
        requiredAttrs = ExpressionInfo(op.projectExprs[attr][0]).getAttributes()
        add = False
        if requiredAttrs.issubset(lhsAttrs):
          lhsProjectExprs[attr] = op.projectExprs[attr]
          add = True
        if requiredAttrs.issubset(rhsAttrs):
          rhsProjectExprs[attr] = op.projectExprs[attr]
          add = True
        if not add:
          skipCurrOp = False

      projectAttrs = set(op.projectExprs.keys())

      if joinAttrs.issubset(projectAttrs):
        if lhsProjectExprs:
          lhsProject = Project(op.subPlan.lhsPlan, lhsProjectExprs)
          op.subPlan.lhsPlan = self.pushdownOperator(lhsProject)
        if rhsProjectExprs:
          rhsProject = Project(op.subPlan.rhsPlan, rhsProjectExprs)
          op.subPlan.rhsPlan = self.pushdownOperator(rhsProject)

      # check if the Project's expressions are assigned to appropriate sub-plans.
      # If one exprs is empty or there are remaining exprs, we still need to keep the
      # original Project operator.
      # If all Project's exprs are assign to two sub-plans, then we can discard this
      # Project operator.
      if lhsProjectExprs and rhsProjectExprs or skipCurrOp:
        return op.subPlan
      else:
        return op

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    """
    Use dynamic programming to find a best join order (System-R).
    First extract base relation (stop at UnionAll because two joins above and under UnionAll cannot be exchanged)
    Perform pickJoinOrder for subPlans under each UnionAll.
    Keep top operators before the first Join and connect it back after reordering Joins
    """
    # Extract all base relations, along with any unary operators immediately above.
    # UnionAll will be seen as a base relation
    baseRelations = set(plan.joinSources)

    # Perform pickJoinOrder under UnionAll first.
    for op in baseRelations:
      while op and not isinstance(op, TableScan):
        if isinstance(op, Union):
          op.lhsPlan = self.pickJoinOrder(Plan(root=op.lhsPlan)).root
          op.rhsPlan = self.pickJoinOrder(Plan(root=op.rhsPlan)).root
          break
        op = op.subPlan

    # Keep the top operators before the first Join.
    # After picking the join order, connect the top operators back to the new operation tree.
    end = Select(None, "")
    end.subPlan = plan.root
    while end:
      if isinstance(end, TableScan) or isinstance(end.subPlan, Join):
        break
      elif isinstance(end.subPlan, Union):
        # When encounter Union before Joins, return the original plan because we already
        # perform pickJoinOrder the the subPlans of UnionAll.
        return plan
      end = end.subPlan

    # Extract all joins in original plan, they serve as the set of joins actually necessary.
    # Since we already perform pickJoinOrder for the sub-plans of UnionAlls, here we only
    # perform pickJoinOrder for those join above the UnionAlls.
    joins = set(plan.joinBeforeUnion)

    # For dynamic programming
    dpPlan = {}

    # Establish optimal access paths.
    for relation in baseRelations:
      dpPlan[frozenset((relation,))] = relation

    # Calculate cost using dynamic programming
    for i in range(2, len(baseRelations) + 1):
      for subset in itertools.combinations(baseRelations, i):

        # Build the set of candidate joins.
        candidateJoins = set()
        for candidateRelation in subset:
          candidateJoins.add((dpPlan[frozenset(tupleWithout(subset, candidateRelation))],
                              dpPlan[frozenset((candidateRelation,))]))

        # Find the current best join plan and store it for next iteration
        dpPlan[frozenset(subset)] = self.findBestJoin(candidateJoins, joins)

    # Connect the operators above the first join
    end.subPlan = dpPlan[frozenset(baseRelations)]

    # Reconstruct the best plan, prepare and return.
    bestPlan = Plan(root=plan.root)
    bestPlan.prepare(self.db)
    return bestPlan

  def findBestJoin(self, candidates, joins):
    bestCost = sys.maxsize
    bestPlan = None

    for lhs, rhs in candidates:
      relevantExpr = None

      # Find the joinExpr corresponding to the current join candidate. If there is none, it's a
      # cartesian product.
      for join in joins:
        attrs = ExpressionInfo(join.joinExpr).getAttributes()
        hashJoin = False

        rhsAttr = set(rhs.schema().fields).intersection(attrs)
        lhsAttr = set(lhs.schema().fields).intersection(attrs)
        if lhsAttr and rhsAttr:
          relevantExpr = join.joinExpr

          # construct relevant schema for hash join
          rhsKeySchema = self.buildKeySchema("rhsKey", rhs.schema().fields, rhs.schema().types, rhsAttr, updateAttr=True)
          lhsKeySchema = self.buildKeySchema("lhsKey", lhs.schema().fields, lhs.schema().types, lhsAttr, updateAttr=False)
          rhsFields = ["rhsKey_" + f for f in rhs.schema().fields]
          attrMap = {}
          orgFileds = rhs.schema().fields
          for i in range(len(rhsFields)):
            attrMap[orgFileds[i]] = rhsFields[i]

          # Construct a new schema for rhs to prevent from joining two relations that
          # have the same attribute name.
          rhsNewSchema = rhs.schema().rename("rhsSchema2", attrMap)

          hashJoin = True
          break

        else:
          relevantExpr = 'True'

      # We don't use index-join because we don't necessarily have index for the join.
      # Construct a join plan for the current candidate, for each possible join algorithm.
      for algo in ["nested-loops", "block-nested-loops"]:

        if algo != "hash":
          testPlan = Plan(root=Join(
            lhsPlan=lhs,
            rhsPlan=rhs,
            method=algo,
            expr=relevantExpr
          ))

        elif hashJoin:
          lhsHashFn = "hash(" + lhsKeySchema.fields[0] + ") % 8"
          rhsHashFn = "hash(" + rhsKeySchema.fields[0] + ") % 8"

          joinPlan = Join(
            lhsPlan=lhs,
            rhsPlan=rhs,
            method='hash',
            rhsSchema=rhsNewSchema,
            lhsHashFn=lhsHashFn, lhsKeySchema=lhsKeySchema,
            rhsHashFn=rhsHashFn, rhsKeySchema=rhsKeySchema
          )
          testPlan = Plan(root=joinPlan)

        else:
          # we don't have enough infor for hash join, so skip the hash join test.
          continue

        # Prepare and run the plan in sampling mode, and get the estimated cost.
        testPlan.prepare(self.db)
        cost = self.getPlanCost(testPlan)

        # update best plan
        if cost < bestCost:
          bestCost = cost
          bestPlan = testPlan

    # Need to return the root operator rather than the plan itself, since it's going back into the
    # table.
    return bestPlan.root

  def buildKeySchema(self, name, fields, types, attrs, updateAttr=False):
    keys = []
    for attr in attrs:
      if updateAttr:
        keys.append((name + "_" + attr, types[fields.index(attr)]))
      else:
        keys.append((attr, types[fields.index(attr)]))
    return DBSchema(name, keys)

  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)
    return joinPicked_plan


def tupleWithout(t, x):
  s = list(t)
  s.remove(x)
  return tuple(s)


if __name__ == "__main__":
  import doctest
  doctest.testmod()
