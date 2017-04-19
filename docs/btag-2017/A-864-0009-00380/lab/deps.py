#!/usr/bin/env python

"""
Dump dependeny tree.

$ python deps.py
 \_ Export()
    \_ ApplyLicensing()
       \_ CombinedIntermediateSchema()
      \_ DOAJIntermediateSchema()
         \_ DOAJInput()
      \_ CrossrefIntermediateSchema()
         \_ CrossrefItems()
        \_ CrossrefInput()
       \_ CreateConfiguration()
      \_ HoldingFile()
"""

import collections

# the top-level task, whose dependency are to be visualized
from merge import MergedSources

# node -> [deps] dictionary
g = collections.defaultdict(set)


def dump(root=None, indent=0):
    """ dump to stdout """
    print('%s \_ %s' % ('   ' * indent, root))
    for dep in g[root]:
        dump(root=dep, indent=indent + 1)

if __name__ == '__main__':
    # a basic BFS to populate g
    queue = [MergedSources()]
    while len(queue) > 0:
        task = queue.pop()
        for dep in task.deps():
            g[task].add(dep)
            queue.append(dep)

    dump(root=MergedSources())

    # # for a visualization with dot/graphviz
    # print('digraph g {')
    # for task, deps in g.iteritems():
    #     for dep in deps:
    #         print(' "%s" -> "%s";' % (task, dep))
    # print('}')
