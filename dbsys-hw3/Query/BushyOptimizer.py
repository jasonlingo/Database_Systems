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


class BushyOptimizer(Optimizer):
  """
  >>> import Database, shutil, Storage
  >>> db = Database.Database()
  >>> try:
  ...   db.createRelation('department', [('did', 'int'), ('EIN', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('employee', [('id', 'int'), ('age', 'int'), ('depId', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('salarys', [('s_eid', 'int'), ('salary', 'int')])
  ... except ValueError:
  ...   pass
  >>> try:
  ...   db.createRelation('tax', [('d_EIN', 'int'), ('tax', 'int')])
  ... except ValueError:
  ...   pass

 # Populate relation
  >>> schema = db.relationSchema('department')
  >>> for tup in [schema.pack(schema.instantiate(i, i * 100)) for i in range(10)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  >>> schema = db.relationSchema('employee')
  >>> for tup in [schema.pack(schema.instantiate(i, 2 * i, i % 5)) for i in range(10)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  >>> schema = db.relationSchema('salarys')
  >>> for tup in [schema.pack(schema.instantiate(i, 1000*i)) for i in range(10)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  >>> schema = db.relationSchema('tax')
  >>> for tup in [schema.pack(schema.instantiate(100 * i, 2*i)) for i in range(10)]:
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

  >>> query7.sample(1.0)
  >>> print(query7.explain())
  >>> q7results = [query7.schema().unpack(tup) for page in db.processQuery(query7) for tup in page[1]]
  # >>> print([tup for tup in q7results])
  >>> print(len(q7results))

  >>> db.setOptimizer(BushyOptimizer)
  >>> query7 = db.optimizer.optimizeQuery(query7)
  >>> query7.sample(1.0)
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
      if isinstance(end, TableScan) or isinstance(end.subPlan, Join):
        break
      elif isinstance(end.subPlan, Union):
        # When encounter Union before Joins, return the original plan because we already
        # perform pickJoinOrder the the subPlans of UnionAll.
        return plan
      end = end.subPlan

    # Extract all joins in original plan, they serve as the set of joins actually necessary.
    joins = set(plan.joinBeforeUnion)

    # for dynamic programming
    dpPlan = {}

    for relation in baseRelations:
      dpPlan[frozenset((relation,))] = relation

    # Calculate cost using dynamic programming
    for i in range(2, len(baseRelations) + 1):
      for subset in itertools.combinations(baseRelations, i):

        # Build the set of candidate joins.
        candidate_joins = set()
        for j in range(1, len(subset)):
          for subset2 in itertools.combinations(subset, j):
            candidate_joins.add((dpPlan[frozenset(tupleWithout(subset, subset2))], dpPlan[frozenset(subset2)]))

        # Find the best of the candidate joins.
        dpPlan[frozenset(subset)] = self.findBestJoin(candidate_joins, joins)

    # Connect the operators above the first join
    end.subPlan = dpPlan[frozenset(baseRelations)]

    # Reconstruct the best plan, prepare and return.
    newPlan = Plan(root=plan.root)
    newPlan.prepare(self.db)
    return newPlan


def tupleWithout(t, x):
  s = list(t)
  for i in list(x):
    s.remove(i)
  return tuple(s)

if __name__ == "__main__":
  import doctest
  doctest.testmod()