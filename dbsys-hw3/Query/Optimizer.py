import itertools

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo

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
  >>> import Database, shutil, Storage
  >>> import Database
  >>> db = Database.Database()
  # >>> try:
  # ...   db.createRelation('department', [('did', 'int'), ('eid', 'int')])
  # ...   db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  # ...   db.createRelation('salarys', [('id', 'int'), ('salary', 'int')])
  # ... except ValueError:
  # ...   pass

  >>> db.createRelation('department', [('did', 'int'), ('eid', 'int')])
  >>> db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  >>> db.createRelation('salarys', [('sid', 'int'), ('salary', 'int')])
  >>> db.createRelation('work', [('wid', 'int'), ('ewid', 'int')])


 # Populate relation
  >>> schema = db.relationSchema('employee')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(20)]:
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

  # >>> query4 = db.query().fromTable('employee').join( \
  #       db.query().fromTable('department'), \
  #       method='block-nested-loops', expr='id == eid') \
  #       .where('eid > 0')\
  #       .where('id < 10') \
  #       .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()



  # >>> query4 = db.query().fromTable('employee')\
  #       .where('id < 10 and id > 0') \
  #       .select({'id': ('id', 'int')}).finalize()
  #
  #
  # >>> print (query4.explain())
  #
  # >>> q4results = [query4.schema().unpack(tup) for page in db.processQuery(query4) for tup in page[1]]
  # >>> [(tup.id) for tup in q4results] #doctest:+ELLIPSIS
  # ...
  #
  # # >>> db.optimizer.pickJoinOrder(query4)
  # >>> query4 = db.optimizer.pushdownOperators(query4)
  #
  # >>> print (query4.explain())
  #
  # >>> q4results = [query4.schema().unpack(tup) for page in db.processQuery(query4) for tup in page[1]]
  # >>> [(tup.id) for tup in q4results] #doctest:+ELLIPSIS
  # ...


# SELECT id, eid FROM employee, department WHERE employee.eid = department.id
#   >>> query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
#         db.query().fromTable('department'), \
#         method='block-nested-loops', expr='id == eid')\
#         .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()

 # >>> query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
 #        db.query().fromTable('department'), \
 #        method='block-nested-loops', expr='id == eid')\
 #        .where('eid > 0 and id > 0')\
 #        .select({'id': ('id', 'int')}).finalize()

 >>> query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='eid == id')\
        .where('eid > 0 and id > 0')\
        .select({'eid':('eid','int')}).finalize()


  # >>> query5 = db.query().fromTable('employee').select({'id': ('id', 'int')}).where('id > 0').union(db.query().fromTable('employee').select({'id': ('id', 'int')}).where('id > 0')).join( \
  #       db.query().fromTable('department').select({'eid':('eid','int')}).where('eid > 0'), \
  #       method='block-nested-loops', expr='eid == id')\
  #       .finalize()


  # >>> print (query5.explain())
  #
  # >>> q5results = [query5.schema().unpack(tup) for page in db.processQuery(query5) for tup in page[1]]
  # >>> [(tup.eid) for tup in q5results]
  #
  # >>> query5 = db.optimizer.pushdownOperators(query5)
  # # >>> query5 = db.optimizer.pickJoinOrder(query5)
  #
  # >>> print (query5.explain())
  #
  # >>> q5results2 = [query5.schema().unpack(tup) for page in db.processQuery(query5) for tup in page[1]]
  # >>> [(tup.eid) for tup in q5results2]

  # >>> query6 = db.query().fromTable('employee').join(\
  #       db.query().fromTable('department').select({'eid':('eid','int')}),\
  #      method='block-nested-loops', expr='id == eid').join(\
  #      db.query().fromTable('salarys'),\
  #      method='block-nested-loops', expr='sid == id').where('sid > 9').select({'age':('age', 'int')}).finalize()
  #
  # # >>> query6 = db.query().fromTable('employee').join(\
  # #       db.query().fromTable('department'),\
  # #      method='block-nested-loops', expr='id == eid').select({'fuck':('id', 'int')}).finalize()
  #
  # >>> q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]
  # >>> [(tup) for tup in q6results]
  #
  # >>> query6.sample(1.0)
  #
  # >>> q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]
  # >>> [(tup) for tup in q6results]
  #
  # >>> print (query6.explain())

  >>> query6 = db.query().fromTable('employee').join(\
        db.query().fromTable('department').select({'eid':('eid','int')}),\
       method='block-nested-loops', expr='id == eid').join(\
       db.query().fromTable('salarys'),\
       method='block-nested-loops', expr='sid == id').where('sid > 9').select({'age':('age', 'int')}).finalize()


  >>> query6 = db.optimizer.optimizeQuery(query6)

  >>> print (query6.explain())

  >>> q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]
  >>> [(tup) for tup in q6results]

###
  #
  # >>> schema = db.relationSchema('work')
  # >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(20)]:
  # ...   _ = db.insertTuple(schema.name, tup)
  # ...
  #
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
  #
  #
  # >>> query7 = db.query().fromTable('employee').join(\
  #       db.query().fromTable('department').select({'eid':('eid','int')}),\
  #      method='block-nested-loops', expr='id == eid').join(\
  #      db.query().fromTable('salarys'),\
  #      method='block-nested-loops', expr='sid == id').where('sid > 0').select({'age':('age', 'int')}).finalize()
  #
  # >>> query7.sample(1.0)
  # >>> print(query7.explain())
  # >>> q7results = [query6.schema().unpack(tup) for page in db.processQuery(query7) for tup in page[1]]
  # >>> print([tup for tup in q7results])
  #
  # >>> query8 = db.query().fromTable('employee').join(\
  #       db.query().fromTable('department').select({'eid':('eid','int')}),\
  #      method='block-nested-loops', expr='id == eid').join(\
  #      db.query().fromTable('salarys'),\
  #      method='block-nested-loops', expr='sid == id').where('sid > 0').select({'age':('age', 'int')}).finalize()
  #
  # >>> query8 = db.optimizer.optimizeQuery(query8)
  # # >>> query8 = db.optimizer.pickJoinOrder2(query8)
  #
  # >>> print (query8.explain())
  #
  # >>> query8.sample(1.0)
  #
  # >>> print(query8.explain())
  # >>> q8results = [query8.schema().unpack(tup) for page in db.processQuery(query8) for tup in page[1]]
  # >>> print([tup for tup in q8results])
  #


  >>> shutil.rmtree(Storage.FileManager.FileManager.defaultDataDir)
  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    raise NotImplementedError

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    raise NotImplementedError

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    return Plan(root=self.pushdownOperator(plan.root))

  def pushdownOperator(self, op):
    if op.operatorType() == "TableScan":
      return op
    elif op.operatorType() in ["GroupBy", "Sort"]:
      op.subPlan = self.pushdownOperator(op.subPlan)
      return op
    elif op.operatorType() == "UnionAll" or "Join" in op.operatorType():
      op.lhsPlan = self.pushdownOperator(op.lhsPlan)
      op.rhsPlan = self.pushdownOperator(op.rhsPlan)
      return op
    elif op.operatorType() == "Project":
      return self.pushdownProject(op)
    elif op.operatorType() == "Select":
      return self.pushdownSelect(op)
    else:
      print("Unmatched operatorType in pushdownOperator(): " + op.operatorType())
      # raise NotImplementedError

  def pushdownProject(self, op):
    # First pushdown operators below:
    op.subPlan = self.pushdownOperator(op.subPlan)

    if op.subPlan.operatorType() in ["GroupBy", "TableScan"]:
      return op

    elif op.subPlan.operatorType() == "Project":
      # Attempt to remove redundant projections:

      bools = [ExpressionInfo(op.subPlan.projectExprs[key][0]).isAttribute()
               for key in op.projectExprs]
      if False not in bools:
        op.subPlan = op.subPlan.subPlan
      return self.pushdownOperator(op)

    elif op.subPlan.operatorType() == "Select":
      # Move op below its subplan if op provides all attributes needed for the selectExpr
      selectAttrs = ExpressionInfo(op.subPlan.selectExpr).getAttributes()
      outputAttrs = set(op.projectExprs.keys())
      result = op
      if selectAttrs.issubset(outputAttrs):
        result = op.subPlan
        op.subPlan = result.subPlan
        result.subPlan = self.pushdownOperator(op)
      return result

    elif op.subPlan.operatorType() == "Sort":
      return op

    elif op.subPlan.operatorType() == "UnionAll":
      # Place a copy of op on each side of the union
      result = op.subPlan
      result.lhsPlan = self.pushdownOperator(Project(result.lhsPlan, op.projectExprs))
      result.rhsPlan = self.pushdownOperator(Project(result.rhsPlan, op.projectExprs))
      return result

    elif "Join" in op.subPlan.operatorType():
      # Partition the projections among the input relations, as much as possible
      lhsAttrs = set(op.subPlan.lhsPlan.schema().fields)
      rhsAttrs = set(op.subPlan.rhsPlan.schema().fields)

      lhsProjectExprs = {}
      rhsProjectExprs = {}
      remainingProjectExprs = False

      joinAttrs = ExpressionInfo( op.subPlan.joinExpr ).getAttributes()

      for attr in op.projectExprs:
        requiredAttrs = ExpressionInfo(op.projectExprs[attr][0]).getAttributes()
        if requiredAttrs.issubset(lhsAttrs):
          lhsProjectExprs[attr] = op.projectExprs[attr]
        elif requiredAttrs.issubset(rhsAttrs):
          rhsProjectExprs[attr] = op.projectExprs[attr]
        else:
          remainingProjectExprs = True

      projectAttrs = set(op.projectExprs.keys())

      if joinAttrs.issubset(projectAttrs):
        if lhsProjectExprs:
          op.subPlan.lhsPlan = self.pushdownOperator(Project(op.subPlan.lhsPlan, lhsProjectExprs))
        if rhsProjectExprs:
          op.subPlan.rhsPlan = self.pushdownOperator(Project(op.subPlan.rhsPlan, rhsProjectExprs))
      else:
        return op

      if op.subPlan.lhsPlan.operatorType() == "UnionAll":
        op.subPlan.lhsPlan.validateSchema()

      if op.subPlan.rhsPlan.operatorType() == "UnionAll":
        op.subPlan.rhsPlan.validateSchema()

      op.subPlan.lhsSchema = op.subPlan.lhsPlan.schema()
      op.subPlan.rhsSchema = op.subPlan.rhsPlan.schema()

      op.subPlan.initializeSchema()

      result = op
      # Remove op from the tree if there are no remaining project expressions, and each side of the join recieved a projection
      if not remainingProjectExprs and lhsProjectExprs and rhsProjectExprs:
        result = op.subPlan
      return result
    else:
      print("Unmatched operatorType in pushdownOperator(): " + op.operatorType())
      # raise NotImplementedError

  def pushdownSelect(self, op):
    # First pushdown operators below:
    op.subPlan = self.pushdownOperator(op.subPlan)

    if op.subPlan.operatorType() in ["GroupBy", "TableScan", "Project"]:
      return op

    elif op.subPlan.operatorType() == "Select":
      # Reorder two selects based on 'score'
      useEstimated = True
      opScore = (1 - op.selectivity(useEstimated)) / op.tupleCost
      childScore = (1 - op.subPlan.selectivity(useEstimated)) / op.tupleCost

      result = op
      if childScore > opScore:
        result = op.subPlan
        op.subPlan = result.subPlan
        result.subPlan = self.pushdownOperator(op)
      return result

    elif op.subPlan.operatorType() == "Sort":
      # Always move a select below a sort
      result = op.subPlan
      op.subPlan = result.subPlan
      result.subPlan = self.pushdownOperator(op)
      return result

    elif op.subPlan.operatorType() == "UnionAll":
      # Place a copy of op on each side of the union
      result = op.subPlan
      result.lhsPlan = self.pushdownOperator(Select(result.lhsPlan, op.selectExpr))
      result.rhsPlan = self.pushdownOperator(Select(result.rhsPlan, op.selectExpr))
      return result

    elif "Join" in op.subPlan.operatorType():
      # Partition the select expr as much as possible
      exprs = ExpressionInfo(op.selectExpr).decomposeCNF()
      lhsExprs = []
      rhsExprs = []
      remainingExprs = []

      lhsAttrs = set(op.subPlan.lhsPlan.schema().fields)
      rhsAttrs = set(op.subPlan.rhsPlan.schema().fields)

      for e in exprs:
        attrs = ExpressionInfo(e).getAttributes()
        if attrs.issubset(lhsAttrs):
          lhsExprs.append(e)
        elif attrs.issubset(rhsAttrs):
          rhsExprs.append(e)
        else:
          remainingExprs.append(e)

      if lhsExprs:
        newLhsExpr = ' and '.join(lhsExprs)
        lhsSelect = Select(op.subPlan.lhsPlan, newLhsExpr)
        op.subPlan.lhsPlan = self.pushdownOperator(lhsSelect)

      if rhsExprs:
        newRhsExpr = ' and '.join(rhsExprs)
        rhsSelect = Select(op.subPlan.rhsPlan, newRhsExpr)
        op.subPlan.rhsPlan = self.pushdownOperator(rhsSelect)

      result = None
      if remainingExprs:
        newExpr = ' and '.join(remainingExprs)
        result = Select(op.subPlan, newExpr)
      else:
        result = op.subPlan

      return result
    else:
      print("Unmatched operatorType in pushdownOperator(): " + op.operatorType())
      # raise NotImplementedError

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.

  # This method is going to make a number of assumptions about the structure of the canonicial input
  # plan, since it needs to deconstruct it.
  #
  # - Unary operators in the query plan exist either at the root, or at the leaves, nowhere else.
  #   This is consistent with a plan obtained from `pushdownOperators`.
  # - There are no joins whose expressions reference attributes from more than two relations. That
  #   is, only binary joins.
  # - As stated somewhere else, all attribute names must be globally unique.

  def pickJoinOrder(self, plan):
    root = plan.root
    # Extract all base relations, along with any unary operators immediately above.
    base_relations = set(plan.sources)

    # Extract all joins in original plan, they serve as the set of joins actually necessary.
    joins = set(plan.joins)
    # joins = set()
    # for (_, plan) in plan.flatten():
    #   if ("Join" in plan.operatorType()):
    #     joins.add(plan)

    # Define the dynamic programming table.
    optimal_plans = {}

    # Establish optimal access paths.
    for relation in base_relations:
      optimal_plans[frozenset((relation,))] = relation

    # Fill in the table.
    for i in range(2, len(base_relations) + 1):
      for subset in itertools.combinations(base_relations, i):

        # Build the set of candidate joins.
        candidate_joins = set()
        for candidate_relation in subset:
          candidate_joins.add((
            optimal_plans[frozenset(tuple_without(subset, candidate_relation))],
            optimal_plans[frozenset((candidate_relation,))]
          ))

        # Find the best of the candidate joins.
        optimal_plans[frozenset(subset)] = self.get_best_join(candidate_joins, joins)

    # Reconstruct the best plan, prepare and return.

    while not "Join" in root.subPlan.operatorType():
      root = root.subPlan

    # final_plan = Plan(root = optimal_plans[frozenset(base_relations)])
    # final_plan.prepare(self.db)
    root.subPlan = optimal_plans[frozenset(base_relations)]
    return plan

  def get_best_join(self, candidates, required_joins):
    best_plan_cost = None
    best_plan = None
    for left, right in candidates:

      relevant_expr = None

      # Find the joinExpr corresponding to the current join candidate. If there is none, it's a
      # cartesian product.
      for join in required_joins:
        names = ExpressionInfo(join.joinExpr).getAttributes()
        if set(right.schema().fields).intersection(names) and set(left.schema().fields).intersection(names):
          relevant_expr = join.joinExpr
          break
        else:
          relevant_expr = 'True'

      # Construct a join plan for the current candidate, for each possible join algorithm.
      for algorithm in ["nested-loops","block-nested-loops","hash","indexed"]:
        if algorithm in ["nested-loops","block-nested-loops"]:
          test_plan = Plan(root = Join(
            lhsPlan = left,
            rhsPlan = right,
            method = algorithm,
            expr = relevant_expr
          ))
        else:
          continue
        # elif algorithm == "hash":
        #   test_plan = Plan(root = Join(
        #     lhsPlan=left,
        #     rhsPlan=right,
        #     method=algorithm,
        #     # lhsHashFn='hash(id) % 4',  lhsKeySchema=keySchema, \
        #     # rhsHashFn='hash(id2) % 4', rhsKeySchema=keySchema2, \
        #   ))
        #   continue

        # Prepare and run the plan in sampling mode, and get the estimated cost.
        test_plan.prepare(self.db)
        test_plan.sample(1.0)
        cost = test_plan.cost(estimated = True)

        # Update running best.
        if best_plan_cost is None or cost < best_plan_cost:
          best_plan_cost = cost
          best_plan = test_plan

    # Need to return the root operator rather than the plan itself, since it's going back into the
    # table.
    return best_plan.root

  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan

if __name__ == "__main__":
  import doctest
  doctest.testmod()
