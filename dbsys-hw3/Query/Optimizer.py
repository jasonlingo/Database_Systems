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

# Helper for removing items from a tuple, while preserving order.
def tuple_without(t, x):
  s = list(t)
  s.remove(x)
  return tuple(s)

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
  ...   db.createRelation('employee', [('id', 'int'), ('age', 'int')])
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
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(2000)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  >>> schema = db.relationSchema('department')
  >>> for tup in [schema.pack(schema.instantiate(i, 4*i)) for i in range(2000)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  >>> schema = db.relationSchema('salarys')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(2000)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  >>> schema = db.relationSchema('work')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(2000)]:
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
       method='block-nested-loops', expr='sid == id').where('sid > 0').select({'age':('age', 'int')}).finalize()

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

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    self.costCache[plan] = cost

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    key = plan.getPlanKey()
    if key not in self.costCache:
        plan.sample(1.0)
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

    elif op.operatorType() in ["Sort", "GroupBy"]:
      # no need to push down these operators, but need to check their children
      op.subPlan = self.pushdownOperator(op.subPlan)
      return op

    elif op.operatorType() == "UnionAll" or "Join" in op.operatorType():
      # no need to push down these operators, but need to check their children (left and right children)
      op.lhsPlan = self.pushdownOperator(op.lhsPlan)
      op.rhsPlan = self.pushdownOperator(op.rhsPlan)
      return op

    elif op.operatorType() == "Select":
      # need to pushdown this operator close to its respective base relation
      return self.pushdownSelect(op)

    elif op.operatorType() == "Project":
      # need to pushdown this operator close to its respective base relation
      return self.pushdownProject(op)

    else:
      print("Wrong operator " + op.operatorType(), file=sys.stderr)

  def pushdownSelect(self, op):
    # pushdown its child first and then push down the current operator
    op.subPlan = self.pushdownOperator(op.subPlan)

    if op.subPlan.operatorType() in ["TableScan", "GroupBy", "Project"]:
      return op

    if op.subPlan.operatorType() == "Select":
      # combine two consecutive Selects
      op.selectExpr = "(%s) and (%s)" % (op.selectExpr, op.subPlan.selectExpr)
      op.subPlan = op.subPlan.subPlan
      return op

      # Selects are communtative
      # two consecutive Selects, choose the Select order according to the selectivity rank (descending):
      #    1 - selectivity(predicate) / cost of the predicate
      # opSel = (1 - op.selectivity(True)) / op.tupleCost
      # subSel = (1 - op.subPlan.selectivity(True)) / op.subPlan.tupleCost
      #
      # if opSel < subSel:
      #   topOp = op.subPlan
      #   op.subPlan = op.subPlan.subPlan
      #   topOp.subPlan = self.pushdownOperator(op)
      #   return topOp
      # else:
      #   return op

    elif op.subPlan.operatorType() == "Sort":
      topOp = op.subPlan
      op.subPlan = op.subPlan.subPlan
      topOp.subPlan = self.pushdownOperator(op)
      return topOp

    elif op.subPlan.operatorType() == "UnionAll":
      # push select to the button of a union to reduce the union complexity
      # because the schemas of lhs and rhs are the same, we can use the original selectExpr for two subPlans
      topOp = op.subPlan
      topOp.lhsPlan = self.pushdownOperator(Select(op.subPlan.lhsPlan, op.selectExpr))
      topOp.rhsPlan = self.pushdownOperator(Select(op.subPlan.rhsPlan, op.selectExpr))
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

      # deal with the remaining predicates  #TODO: check if we need this
      if remainingExprs:
        print("has remaining attributes")
        newExpr = ' and '.join(remainingExprs)
        result = Select(op.subPlan, newExpr)
      else:
        result = op.subPlan

      return result

    else:
      print("wrong operator " + op.subPlan.operatorType(), file=sys.stderr)


  def pushdownProject(self, op):
    # pushdown its child first and then push down the current operator
    op.subPlan = self.pushdownOperator(op.subPlan)

    if op.subPlan.operatorType() in ["GroupBy", "TableScan", "Sort"]:
      return op

    if op.subPlan.operatorType() == "Project":
      # Two consecutive Projects, remove duplicate.
      # We cannot directly discard the lower Project because the upper Project might use
      # other new attributes (not original attributes) generated from the lower Project.
      # If the upper Project contains attributes that is the subset of the attributes of
      # the lower Project, then we can discard the lower Project.

      opAttr = set([v[0] for v in op.projectExprs.values()])
      subPlanAttr = set([v[0] for v in op.subPlan.projectExprs.values()])
      if opAttr.issubset(subPlanAttr):
        op.subPlan = op.subPlan.subPlan
        return self.pushdownProject(op)
      else:
        return op

    elif op.subPlan.operatorType() == "Select":
      # If the attributes of Select is a subset of that of Project, then we can swap
      # the order of Project and Select and pushdown the Project operator.

      projectAttrs = set([v[0] for v in op.projectExprs.values()])
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
      topOp.lhsPlan = self.pushdownOperator(Project(op.subPlan.lhsPlan, op.projectExprs))
      topOp.rhsPlan = self.pushdownOperator(Project(op.subPlan.rhsPlan, op.projectExprs))
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
      remainingProjectExprs = False

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
          remainingProjectExprs = True

      projectAttrs = set(op.projectExprs.keys())

      if joinAttrs.issubset(projectAttrs):
        if lhsProjectExprs:
          op.subPlan.lhsPlan = self.pushdownOperator(Project(op.subPlan.lhsPlan, lhsProjectExprs))
        if rhsProjectExprs:
          op.subPlan.rhsPlan = self.pushdownOperator(Project(op.subPlan.rhsPlan, rhsProjectExprs))

      # todo: check whether we need this
      # if op.subPlan.lhsPlan.operatorType() == "UnionAll":
      #   op.subPlan.lhsPlan.validateSchema()
      #
      # op.subPlan.lhsSchema = op.subPlan.lhsPlan.schema()
      #
      # if op.subPlan.rhsPlan.operatorType() == "UnionAll":
      #   op.subPlan.rhsPlan.validateSchema()

      # op.subPlan.rhsSchema = op.subPlan.rhsPlan.schema()

      # op.subPlan.initializeSchema()

      result = op
      # Remove op from the tree if there are no remaining project expressions, and each side of the join recieved a projection
      if not remainingProjectExprs and lhsProjectExprs and rhsProjectExprs:
        result = op.subPlan
      return result

    else:
      print("wrong operator " + op.subPlan.operatorType(), file=sys.stderr)


  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    """
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
    dummy = Select(None, "")
    dummy.subPlan = plan.root
    end = dummy
    while end:
      if isinstance(end.subPlan, Join):
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

    # Define the dynamic programming table.
    optimal_plans = {}

    # Establish optimal access paths.
    for relation in baseRelations:
      optimal_plans[frozenset((relation,))] = relation
      print ('relation type', type(relation), frozenset((relation,)))

    # Calculate cost using dynamic programming
    for i in range(2, len(baseRelations) + 1):
      for subset in itertools.combinations(baseRelations, i):

        # Build the set of candidate joins.
        candidate_joins = set()
        for candidate_relation in subset:
          candidate_joins.add((
            optimal_plans[frozenset(tuple_without(subset, candidate_relation))],
            optimal_plans[frozenset((candidate_relation,))]
          ))

        # Find the best of the candidate joins.
        optimal_plans[frozenset(subset)] = self.get_best_join(candidate_joins, joins)

    # Connect the operators above the first join
    # FIXME: still will lose some operators between joins
    end.subPlan = optimal_plans[frozenset(baseRelations)]

    # Reconstruct the best plan, prepare and return.
    newPlan = Plan(root=plan.root)
    newPlan.prepare(self.db)
    return newPlan

  def get_best_join(self, candidates, required_joins):
    best_plan_cost = sys.maxsize
    best_plan = None

    for left, right in candidates:
      relevant_expr = None

      # Find the joinExpr corresponding to the current join candidate. If there is none, it's a
      # cartesian product.
      for join in required_joins:
        names = ExpressionInfo(join.joinExpr).getAttributes()
        # hashJoin = False
        if set(right.schema().fields).intersection(names) and set(left.schema().fields).intersection(names):
          relevant_expr = join.joinExpr

          # # for hash join
          # rhsKeySchema = self.buildKeySchema("rhsKey", right.schema().fields, right.schema().types, set(right.schema().fields).intersection(names), updateAttr=True)
          # lhsKeySchema = self.buildKeySchema("lhsKey", left.schema().fields, left.schema().types, set(left.schema().fields).intersection(names), updateAttr=False)
          # rhsFields = ["rhsKey_" + f for f in right.schema().fields]
          # attrMap = {}
          # orgFileds = right.schema().fields
          # for i in range(len(rhsFields)):
          #   attrMap[orgFileds[i]] = rhsFields[i]
          # rhsNewSchema = right.schema().rename("rhsSchema2", attrMap)
          # # print("-----")
          # # print(rhsKeySchema.toString())
          # # print(lhsKeySchema.toString())
          # # print(rhsNewSchema.toString())
          # # print(right.schema().toString())
          # hashJoin = True
          break

        else:
          relevant_expr = 'True'

      # We don't use index-join because we don't necessarily have index for the join.
      # Construct a join plan for the current candidate, for each possible join algorithm.
      # TODO: Evaluate more than just nested loop joins, and determine feasibility of those methods.
      for algo in ["nested-loops", "block-nested-loops"]:
        # if algo != "hash":
        test_plan = Plan(root = Join(
          lhsPlan = left,
          rhsPlan = right,
          method = algo,
          expr = relevant_expr
        ))

        # elif hashJoin:
        #   lhsHashFn = "hash(" + lhsKeySchema.fields[0] + ") % 8"
        #   rhsHashFn = "hash(" + rhsKeySchema.fields[0] + ") % 8"
        #
        #   joinPlan = Join(
        #     lhsPlan=left,
        #     rhsPlan=right,
        #   method='hash',
        #   rhsSchema=rhsNewSchema,
        #   lhsHashFn=lhsHashFn, lhsKeySchema=lhsKeySchema,
        #   rhsHashFn=rhsHashFn, rhsKeySchema=rhsKeySchema
        #   )
        #   print(joinPlan.conciseExplain())
        #   test_plan = Plan(root=joinPlan)
        #
        # else:
        #   continue

        # Prepare and run the plan in sampling mode, and get the estimated cost.
        test_plan.prepare(self.db)
        cost = self.getPlanCost(test_plan)

        # Update running best.
        if cost < best_plan_cost:
          best_plan_cost = cost
          best_plan = test_plan

    # Need to return the root operator rather than the plan itself, since it's going back into the
    # table.
    return best_plan.root

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

if __name__ == "__main__":
  import doctest
  doctest.testmod()