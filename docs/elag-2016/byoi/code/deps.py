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
from part6_export import Export

g = collections.defaultdict(set)

def dump(root=None, indent=0):
    print('%s \_ %s' % ('   ' * indent, root))
    for dep in g[root]:
        dump(root=dep, indent=indent + 1)

if __name__ == '__main__':    
    queue = [Export()]
    while len(queue) > 0:
        task = queue.pop()
        for dep in task.deps():
            g[task].add(dep)
            queue.append(dep)

    dump(root=Export())

    # print('digraph g {')
    # for task, deps in g.iteritems():
    #     for dep in deps:
    #         print(' "%s" -> "%s";' % (task, dep))
    # print('}')
