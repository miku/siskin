# coding: utf-8

"""
Sources and sizes.
"""
import base64
import json

import matplotlib.pyplot as plt
import numpy as np
import requests

addr = base64.b64decode("""aHR0cDovLzE3Mi4xOC4xMTMuNzo4MDg1L3NvbHIvYmlibGlv""")


def total():
    """ Return the total number of docs. """
    r = requests.get('%s/select?wt=json&q=*:*' % (addr, label))
    if r.status_code >= 300:
        raise RuntimeError("got HTTP %s on %s" % (r.status_code, r.url))
    doc = json.loads(r.text)
    return doc['response']['numFound']

sources = (
    ('28', 'DOAJ'),
    ('48', 'WISO'),
    ('49', 'Crossref'),
    ('50', 'De Gruyter'),
    ('55', 'JSTOR'),
    ('60', 'Thieme'),
    ('85', 'Elsevier'),
    ('89', 'IEEE'),
    ('105', 'Springer'),
    ('121', 'Arxiv'),
)

labels, names, sizes = [s[0] for s in sources], [s[1] for s in sources], []

for label in labels:
    r = requests.get('%s/select?wt=json&q=source_id:%s' % (addr, label))
    if r.status_code >= 300:
        raise RuntimeError("got HTTP %s on %s" % (r.status_code, r.url))
    doc = json.loads(r.text)
    found = doc['response']['numFound']
    sizes.append(found)

explode = [0 for _ in range(len(labels))]
explode[2] = 0.1

fig1, ax1 = plt.subplots()

cmap = plt.get_cmap('Set1')
colors = [cmap(i) for i in np.linspace(0, 1, len(labels))]

patches, texts = plt.pie(sizes, startangle=90, colors=colors, shadow=False, explode=explode)
plt.legend(patches, names, loc="lower left")
ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.title('Article Metadata Index Sources (2017)')
plt.savefig('pie.png')
