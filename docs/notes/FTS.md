---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernelspec:
    display_name: siskin
    language: python
    name: siskin
---

```python
import pandas as pd
import matplotlib.pyplot as plt
```

```python
df = pd.read_csv("83f8a712c4cf05ce7492cfade8170169cca301a5", compression="gzip")
```

```python
df.head()
```

```python
len(df)
```

```python
df["Fulltext Searchable"] = df["Fulltext Searchable"].str.strip()
df.loc[df["Fulltext Searchable"] == "0", "Fulltext Searchable"] = "No"
df.loc[df["Fulltext Searchable"] == "no", "Fulltext Searchable"] = "No"
```

```python
df.groupby("Fulltext Searchable").size()
```

```python
ax = df.groupby("Fulltext Searchable").size().plot(kind="pie",
                                                   title="Fulltext Searchable (Collection, N=4328)",
                                                   figsize=(10, 6))
ax.set_ylabel(None);
```

```python
df.sort_values(by="Record Count", ascending=False).loc[:, ["Collection Name", "Record Count"]].head(20)
```

```python
df["Fulltext Searchable"] = df["Fulltext Searchable"].str.strip()
```

```python
df.groupby("Fulltext Searchable").size()
```

```python
df[df["Fulltext Searchable"] == "Yes"].sort_values(by="Record Count", ascending=False).loc[:,
    ["Collection Name", "Provider Name", "Fulltext Searchable", "Record Count"]].head(30)
```

```python
df[df["Fulltext Searchable"] == "No"].sort_values(by="Record Count", ascending=False).loc[:,
    ["Collection Name", "Provider Name", "Fulltext Searchable", "Record Count"]].head(30)
```

```python

```
