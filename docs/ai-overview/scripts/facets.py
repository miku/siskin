# coding: utf-8

import itertools
import json

import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)


with open("facets.json") as handle:
    doc = json.load(handle)

Y = [size / 1000000.0 for _, size in grouper(doc, 2)]
X = range(len(Y))

fig, (ax1, ax2) = plt.subplots(2)
ax1.yaxis.set_major_formatter(FormatStrFormatter("%.0f"))

ax1.plot(X, Y)
ax1.set_title("Crossref collection size distribution (2017)")
ax1.set_xlabel("number of collections")
ax1.set_ylabel("records (millions)")

ax2.plot(X[:10], Y[:10])
ax2.set_xlabel("number of collections")
ax2.set_ylabel("records (millions)")

plt.tight_layout()
plt.savefig("facets.png")
