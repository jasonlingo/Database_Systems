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
<<<<<<< HEAD
from Query.GreedyOptimizer import GreedyOptimizer


# Helper for removing items from a tuple, while preserving order.
def tuple_without(t, x):
  s = list(t)
  for item in x:
    s.remove(item)
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
=======


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
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
  ... except ValueError:
  ...   pass

 # Populate relation
<<<<<<< HEAD
  >>> schema = db.relationSchema('A')
=======
  >>> schema = db.relationSchema('employee')
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

<<<<<<< HEAD
  >>> schema = db.relationSchema('B')
=======
  >>> schema = db.relationSchema('department')
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
  >>> for tup in [schema.pack(schema.instantiate(i, 4*i)) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

<<<<<<< HEAD
  >>> schema = db.relationSchema('C')
=======
  >>> schema = db.relationSchema('salarys')
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

<<<<<<< HEAD
  >>> schema = db.relationSchema('D')
=======
  >>> schema = db.relationSchema('work')
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

<<<<<<< HEAD
  >>> schema = db.relationSchema('E')
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i)) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...


  >>> query6 = db.query().fromTable('A').join(\
        db.query().fromTable('B').select({'b1':('b1','int')}),\
       method='block-nested-loops', expr='b1 == a1').join(\
       db.query().fromTable('C'),\
       method='block-nested-loops', expr='c1 == b1').where('id > 0').select({'id':('id', 'int')})\
       .union(\
       db.query().fromTable('employee').join(\
        db.query().fromTable('department').select({'eid':('eid','int')}),\
       method='block-nested-loops', expr='id == eid').where('age > 0').join(\
       db.query().fromTable('salarys'),\
       method='block-nested-loops', expr='sid == id').where('id > 0').select({'id':('id', 'int')})\
       ).finalize()


  >>> query7 = db.query().fromTable('employee').join(\
=======
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

  >>> query9 = db.query().fromTable('employee').join(\
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
        db.query().fromTable('department').select({'eid':('eid','int')}),\
       method='block-nested-loops', expr='id == eid').join(\
       db.query().fromTable('salarys'),\
       method='block-nested-loops', expr='sid == id').where('sid > 0').select({'age':('age', 'int')}).finalize()

<<<<<<< HEAD
  >>> query7.sample(1.0)
  >>> print(query7.explain())
  >>> q7results = [query7.schema().unpack(tup) for page in db.processQuery(query7) for tup in page[1]]
  >>> print([tup for tup in q7results])

  ## Clean up the doctest
  >>> shutil.rmtree(Storage.FileManager.FileManager.defaultDataDir)
  """

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
=======
  >>> db.setOptimizer(BushyOptimizer)
  >>> query9 = db.optimizer.optimizeQuery(query9)
  >>> query9.sample(1.0)
  >>> print(query9.explain())
  >>> q9results = [query9.schema().unpack(tup) for page in db.processQuery(query9) for tup in page[1]]
  >>> print([tup for tup in q9results])


  >>> shutil.rmtree(Storage.FileManager.FileManager.defaultDataDir)

  """

  def __init__(self, db):
    super().__init__(db)

>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
  def pickJoinOrder(self, plan):
    """
    First extract base relation (stop at UnionAll because two joins above and under UnionAll cannot be exchanged)
    Perform pickJoinOrder for subPlans under each UnionAll.
    Keep top operators before the first Join and connect it back after reordering Joins
    """
<<<<<<< HEAD
    # Extract all base relations, along with any unary operators immediately above.
=======
    # Extract all base relations. Here we see UnionAll as a base relation.
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
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
<<<<<<< HEAD

    dummy = Select(None, "")
    dummy.subPlan = plan.root
    dummy.prepare(self.db)
    end = dummy

    # end = plan.root
=======
    end = Select(None, "")
    end.subPlan = plan.root
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
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
<<<<<<< HEAD
      # print ('relation type', type(relation), frozenset((relation,)))
=======
      print ('relation type', type(relation), frozenset((relation,)))
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e

    # Calculate cost using dynamic programming
    for i in range(2, len(baseRelations) + 1):
      for subset in itertools.combinations(baseRelations, i):

        # Build the set of candidate joins.
        candidate_joins = set()
        for j in range(1, len(subset)):
<<<<<<< HEAD
          for candidate_relations in itertools.combinations(subset, j):
            candidate_joins.add((
              optimal_plans[frozenset(tuple_without(subset, candidate_relations))],
              optimal_plans[frozenset(candidate_relations)]
            ))
            # candidate_joins.add((
            #   optimal_plans[frozenset(candidate_relations)],
            #   optimal_plans[frozenset(tuple_without(subset, candidate_relations))]
            # ))
=======
          for subset2 in itertools.combinations(subset, j):
            candidate_joins.add((
              optimal_plans[frozenset(tuple_without(subset, subset2))],
              optimal_plans[frozenset(subset2)]
            ))
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e

        # Find the best of the candidate joins.
        optimal_plans[frozenset(subset)] = self.get_best_join(candidate_joins, joins)

    # Connect the operators above the first join
<<<<<<< HEAD
=======
    # FIXME: still will lose some operators between joins
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
    end.subPlan = optimal_plans[frozenset(baseRelations)]

    # Reconstruct the best plan, prepare and return.
    newPlan = Plan(root=plan.root)
    newPlan.prepare(self.db)
    return newPlan

  def get_best_join(self, candidates, required_joins):
<<<<<<< HEAD
    best_plan_cost = None
    best_plan = None
    for left, right in candidates:

=======
    best_plan_cost = sys.maxsize
    best_plan = None

    for left, right in candidates:
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
      relevant_expr = None

      # Find the joinExpr corresponding to the current join candidate. If there is none, it's a
      # cartesian product.
      for join in required_joins:
<<<<<<< HEAD
        names = ExpressionInfo(join.joinExpr).getAttributes()
        # if set(join.rhsSchema.fields).intersection(names) and set(join.lhsSchema.fields).intersection(names):
        if set(right.schema().fields).intersection(names) and set(left.schema().fields).intersection(names):
          relevant_expr = join.joinExpr
          break
=======
        attrs = ExpressionInfo(join.joinExpr).getAttributes()

        rhsInterAttrs = set(right.schema().fields).intersection(attrs)
        lhsInterAttrs =  set(left.schema().fields).intersection(attrs)
        if rhsInterAttrs and lhsInterAttrs:
          relevant_expr = join.joinExpr
          break

>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
        else:
          relevant_expr = 'True'

      #According to the performance result in assignment 2, we use hash join here.
      #We don't use index-join because we don't necessarily have index for the join.
      # Construct a join plan for the current candidate, for each possible join algorithm.
<<<<<<< HEAD
      for algorithm in ["nested-loops", "block-nested-loops"]:
        if algorithm == "hash":
          # Hash join cannot handle cartesian product?
          if relevant_expr == 'True':
            continue

          names = ExpressionInfo(relevant_expr).getAttributes()
          key1 = set(left.schema().fields).intersection(names).pop()
          key2 = set(right.schema().fields).intersection(names).pop()

          for (field, type) in left.schema().schema():
            if field == key1:
              keySchema = DBSchema('key1',[(field, type)])
              break

          for (field, type) in right.schema().schema():
            if field == key2:
              keySchema2 = DBSchema('key2',[(field, type)])
              break

          if (keySchema is None or keySchema2 is None):
            print ("Key Schema Error\n")
            exit(0)

          test_plan = Plan(root = Join(
            lhsPlan = left,
            rhsPlan = right,
            method = 'hash',
            lhsHashFn= 'hash(' + key1 + ') % 4', lhsKeySchema=keySchema,
            rhsHashFn= 'hash(' + key2 + ') % 4', rhsKeySchema=keySchema2
          ))
        else:
          test_plan = Plan(root = Join(
            lhsPlan = left,
            rhsPlan = right,
            method = algorithm,
            expr = relevant_expr
          ))

        # Prepare and run the plan in sampling mode, and get the estimated cost.
        test_plan.prepare(self.db)
        test_plan.sample(1.0)
        cost = test_plan.cost(estimated = True)

        # Update running best.
        if best_plan_cost is None or cost < best_plan_cost:
=======
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
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
          best_plan_cost = cost
          best_plan = test_plan

    # Need to return the root operator rather than the plan itself, since it's going back into the
    # table.
    return best_plan.root

<<<<<<< HEAD
  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan

if __name__ == "__main__":
  import doctest
  doctest.testmod()
=======
  def buildKeySchema(self, name, fields, types, attrs, updateAttr=False):
    keys = []
    for attr in attrs:
      if updateAttr:
        keys.append((name + "_" + attr, types[fields.index(attr)]))
      else:
        keys.append((attr, types[fields.index(attr)]))
    return DBSchema(name, keys)


if __name__ == "__main__":
  import doctest
  doctest.testmod()
>>>>>>> 26f1519acb21752e14ebc70682f58c53e5b2690e
