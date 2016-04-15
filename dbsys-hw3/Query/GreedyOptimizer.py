import itertools
import sys

from Query.Optimizer import  Optimizer
from Query.BushyOptimizer import BushyOptimizer
from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Query.Operators.Union import Union
from Query.Operators.TableScan import TableScan
from Utils.ExpressionInfo import ExpressionInfo
from Catalog.Schema import DBSchema



class GreedyOptimizer(Optimizer):
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

  >>> query6 = db.query().fromTable('employee').join(\
        db.query().fromTable('department').select({'eid':('eid','int')}),\
       method='block-nested-loops', expr='id == eid').where('age > 0').join(\
       db.query().fromTable('salarys'),\
       method='block-nested-loops', expr='sid == id').where('id > 0').select({'id':('id', 'int')})\
       .union(\
       db.query().fromTable('employee').join(\
        db.query().fromTable('department').select({'eid':('eid','int')}),\
       method='block-nested-loops', expr='id == eid').where('age > 0').join(\
       db.query().fromTable('salarys'),\
       method='block-nested-loops', expr='sid == id').where('id > 0').select({'id':('id', 'int')})\
       ).finalize()


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


  >>> db.setOptimizer(BushyOptimizer(db=db))
  >>> query8 = db.optimizer.optimizeQuery(query8)

  >>> query8.sample(1.0)
  >>> print(query8.explain())


  >>> query8 = db.query().fromTable('employee').join(\
        db.query().fromTable('department').select({'eid':('eid','int')}),\
       method='block-nested-loops', expr='id == eid').join(\
       db.query().fromTable('salarys'),\
       method='block-nested-loops', expr='sid == id').where('sid > 0').select({'age':('age', 'int')}).finalize()

  >>> db.setOptimizer(GreedyOptimizer(db=db))
  >>> query8 = db.optimizer.optimizeQuery(query8)

  >>> query8.sample(1.0)
  >>> print(query8.explain())


  >>> query8 = db.query().fromTable('employee').join(\
        db.query().fromTable('department').select({'eid':('eid','int')}),\
       method='block-nested-loops', expr='id == eid').join(\
       db.query().fromTable('salarys'),\
       method='block-nested-loops', expr='sid == id').where('sid > 0').select({'age':('age', 'int')}).finalize()


  >>> db.setOptimizer(Optimizer(db=db))
  >>> query8 = db.optimizer.optimizeQuery(query8)

  >>> query8.sample(1.0)
  >>> print(query8.explain())


  ## Clean up the doctest
  >>> shutil.rmtree(Storage.FileManager.FileManager.defaultDataDir)
  """

  def pickJoinOrder(self, plan):
    """
    First extract base relation (stop at UnionAll because two joins above and under UnionAll cannot be exchanged)
    Perform pickJoinOrder for subPlans under each UnionAll.
    Keep top operators before the first Join and connect it back after reordering Joins
    """
    # Extract all base relations, along with any unary operators immediately above.
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

    end = plan.root
    while end:
      if isinstance(end, TableScan) or isinstance(end.subPlan, Join):
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

    baseRelations = list(baseRelations)
    while len(baseRelations) > 1:
      candidate_joins = set()
      for i in range(0, len(baseRelations) - 1):
        candidate_joins.add((baseRelations[i], baseRelations[i+1]))
        candidate_joins.add((baseRelations[i+1], baseRelations[i]))
      optimal_plan = self.get_best_join(candidate_joins, joins)
      baseRelations.append(optimal_plan)
      baseRelations.remove(optimal_plan.rhsPlan)
      baseRelations.remove(optimal_plan.lhsPlan)

    end.subPlan = baseRelations[0]

    newPlan = Plan(root=plan.root)
    newPlan.prepare(self.db)
    return newPlan


  def get_best_join(self, candidates, required_joins):
    best_plan_cost = None
    best_plan = None
    for left, right in candidates:

      relevant_expr = None

      # Find the joinExpr corresponding to the current join candidate. If there is none, it's a
      # cartesian product.
      for join in required_joins:
        names = ExpressionInfo(join.joinExpr).getAttributes()
        # if set(join.rhsSchema.fields).intersection(names) and set(join.lhsSchema.fields).intersection(names):
        if set(right.schema().fields).intersection(names) and set(left.schema().fields).intersection(names):
          relevant_expr = join.joinExpr
          break
        else:
          relevant_expr = 'True'

      #According to the performance result in assignment 2, we use hash join here.
      #We don't use index-join because we don't necessarily have index for the join.
      # Construct a join plan for the current candidate, for each possible join algorithm.
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
          best_plan_cost = cost
          best_plan = test_plan

    # Need to return the root operator rather than the plan itself, since it's going back into the
    # table.
    return best_plan.root

if __name__ == "__main__":
  import  doctest
  doctest.testmod()