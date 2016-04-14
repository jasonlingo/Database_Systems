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
from Query.Optimizer import Optimizer


def tuple_without(t, x):
  s = list(t)
  for i in list(x):
    s.remove(i)
  return tuple(s)


class BushyOptimizer(Optimizer):
  """
  >>> import Database, shutil, Storage
  >>> db = Database.Database()
  >>> try:
  ...   db.createRelation('A', [('a1', 'int'), ('a2', 'int'), ('a3', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('B', [('b1', 'int'), ('b2', 'int'), ('b3', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('C', [('c1', 'int'), ('c2', 'int'), ('c3', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('D', [('d1', 'int'), ('d2', 'int'), ('d3', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('E', [('e1', 'int'), ('e2', 'int'), ('e3', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('F', [('f1', 'int'), ('f2', 'int'), ('f3', 'int')])
  ... except ValueError:
  ...   pass
  >>> schema = db.relationSchema('A')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i, 3*i)) for i in range(2000)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...
  >>> schema = db.relationSchema('B')
  >>> for tup in [schema.pack(schema.instantiate(i, 4*i, 3*i)) for i in range(2000)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...
  >>> schema = db.relationSchema('C')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i, 3*i)) for i in range(2000)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...
  >>> schema = db.relationSchema('D')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i, 3*i)) for i in range(2000)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...
  >>> schema = db.relationSchema('E')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i, 3*i)) for i in range(2000)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...
  >>> schema = db.relationSchema('E')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i, 3*i)) for i in range(2000)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...
  >>> query7 = db.query().fromTable('salarys').join(\
       db.query().fromTable('employee'),\
       method='block-nested-loops', expr='id == s_eid').join(\
       db.query().fromTable('department'),\
       method='block-nested-loops', expr='depId == did').join(\
       db.query().fromTable('tax'),\
       method='block-nested-loops', expr='EIN == d_EIN')\
       .finalize()
  # >>> query7.sample(1.0)
  >>> print(query7.explain())
  >>> q7results = [query7.schema().unpack(tup) for page in db.processQuery(query7) for tup in page[1]]
  # >>> print([tup for tup in q7results])
  >>> print(len(q7results))
  >>> db.setOptimizer(BushyOptimizer)
  >>> query7 = db.optimizer.optimizeQuery(query7)
  # >>> query7.sample(1.0)
  >>> print(query7.explain())
  # >>> query8 = db.query().fromTable('employee').join(\
  #       db.query().fromTable('department').select({'eid':('eid','int')}),\
  #      method='block-nested-loops', expr='id == eid').join(\
  #      db.query().fromTable('salarys'),\
  #      method='block-nested-loops', expr='sid == id').where('sid > 0').select({'age':('age', 'int')}).finalize()
  #
  # >>> query8 = db.optimizer.optimizeQuery(query8)
  # >>> query8.sample(1.0)
  # >>> print(query8.explain())
  # >>> q8results = [query8.schema().unpack(tup) for page in db.processQuery(query8) for tup in page[1]]
  # >>> print([tup for tup in q8results])
  #
  # >>> query9 = db.query().fromTable('employee').join(\
  #       db.query().fromTable('department').select({'eid':('eid','int')}),\
  #      method='block-nested-loops', expr='id == eid').join(\
  #      db.query().fromTable('salarys'),\
  #      method='block-nested-loops', expr='sid == id').where('sid > 0').select({'age':('age', 'int')}).finalize()
  #
  # >>> db.setOptimizer(BushyOptimizer)
  # >>> query9 = db.optimizer.optimizeQuery(query9)
  # >>> query9.sample(1.0)
  # >>> print(query9.explain())
  # >>> q9results = [query9.schema().unpack(tup) for page in db.processQuery(query9) for tup in page[1]]
  # >>> print([tup for tup in q9results])
  >>> shutil.rmtree(Storage.FileManager.FileManager.defaultDataDir)
############################################################
  Join size 2
############################################################
#   >>> query6 = db.query().fromTable('A').join(\
#         db.query().fromTable('B').select({'b1':('b1','int')}),\
#        method='block-nested-loops', expr='b1 == a1').where('a1 > 0').finalize()
#
#
#   >>> query6.sample(1.0)
#   >>> print("Original query")
#   >>> print(query6.explain())
#
#   >>> print("Optimizer")
#   >>> query6 = db.optimizer.optimizeQuery(query6)
#   >>> print (query6.explain())
#
#   >>> print("Optimizer")
#   >>> query6 = db.optimizer.optimizeQuery(query6)
#   >>> print (query6.explain())
#   # >>> q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]
#   # >>> print([tup for tup in q6results])
#
# ############################################################
#   Join size 4
# ############################################################
#   >>> query6 = db.query().fromTable('A').join(\
#         db.query().fromTable('B').select({'b1':('b1','int')}),\
#        method='block-nested-loops', expr='b1 == a1').join(\
#        db.query().fromTable('C'),\
#        method='block-nested-loops', expr='c1 == b1').join(\
#        db.query().fromTable('D'),\
#        method='block-nested-loops', expr='c1==d1').where('a1 > 0').finalize()
#
#
#   >>> query6.sample(1.0)
#   >>> print(query6.explain())
#
#   >>> query6 = db.optimizer.optimizeQuery(query6)
#   >>> print (query6.explain())
############################################################
  Join size 6
############################################################
  >>> query6 = db.query().fromTable('A').join(\
        db.query().fromTable('B').select({'b1':('b1','int')}),\
       method='block-nested-loops', expr='b1 == a1').join(\
       db.query().fromTable('C'),\
       method='block-nested-loops', expr='c1 == b1').join(\
       db.query().fromTable('D'),\
       method='block-nested-loops', expr='d1 == c1').join(\
       db.query().fromTable('E'),\
       method='block-nested-loops', expr='d1 == e1').join(\
       db.query().fromTable('F'),\
       method='block-nested-loops', expr='e1 == f1').where('a1 > 0').finalize()
  >>> query6.sample(5.0)
  >>> print(query6.explain())
  >>> query6 = db.optimizer.optimizeQuery(query6)
  >>> print (query6.explain())
  ## Clean up the doctest
  # >>> shutil.rmtree(Storage.FileManager.FileManager.defaultDataDir)
  """
  def __init__(self, db):
    super().__init__(db)

  def pickJoinOrder(self, plan):
    """
    First extract base relation (stop at UnionAll because two joins above and under UnionAll cannot be exchanged)
    Perform pickJoinOrder for subPlans under each UnionAll.
    Keep top operators before the first Join and connect it back after reordering Joins
    """
    # Extract all base relations. Here we see UnionAll as a base relation.
    baseRelations = set(plan.joinSources)

    # perform pickJoinOeder under UnionAll
    for op in baseRelations:
      while op and not isinstance(op, TableScan):
        if isinstance(op, Union):
          op.lhsPlan = self.pickJoinOrder(Plan(root=op.lhsPlan)).root
          op.rhsPlan = self.pickJoinOrder(Plan(root=op.rhsPlan)).root
          break
        op = op.subPlan

    # Keep the top operators before the first Join operator.
    # After pick the join order, connect the top operators back to the new operation tree.
    end = Select(None, "")
    end.subPlan = plan.root
    while end:
      if isinstance(end.subPlan, Join):
        break
      elif isinstance(end.subPlan, Union):
        # When encounter Union before Joins, return the original plan because we already
        # perform pickJoinOrder the the subPlans of UnionAll.
        return plan
      end = end.subPlan

    # Extract all joins in original plan, they serve as the set of joins actually necessary.
    joins = set(plan.joinBeforeUnion)

    # Define the dynamic programming table.
    optimal_plans = {}

    # Establish optimal access paths.
    for relation in baseRelations:
      optimal_plans[frozenset((relation,))] = relation

    # Calculate cost using dynamic programming
    for i in range(2, len(baseRelations) + 1):
      for subset in itertools.combinations(baseRelations, i):

        # Build the set of candidate joins.
        candidate_joins = set()
        for j in range(1, len(subset)):
          for subset2 in itertools.combinations(subset, j):
            candidate_joins.add((
              optimal_plans[frozenset(tuple_without(subset, subset2))],
              optimal_plans[frozenset(subset2)]
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
        attrs = ExpressionInfo(join.joinExpr).getAttributes()

        rhsInterAttrs = set(right.schema().fields).intersection(attrs)
        lhsInterAttrs =  set(left.schema().fields).intersection(attrs)
        if rhsInterAttrs and lhsInterAttrs:
          relevant_expr = join.joinExpr
          break

        else:
          relevant_expr = 'True'

      #According to the performance result in assignment 2, we use hash join here.
      #We don't use index-join because we don't necessarily have index for the join.
      # Construct a join plan for the current candidate, for each possible join algorithm.
      # TODO: Evaluate more than just nested loop joins, and determine feasibility of those methods.
      for algo in ["nested-loops", "block-nested-loops"]:
        test_plan = Plan(root = Join(
          lhsPlan = left,
          rhsPlan = right,
          method = algo,
          expr = relevant_expr
        ))

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

if __name__ == "__main__":
  import doctest
  doctest.testmod()