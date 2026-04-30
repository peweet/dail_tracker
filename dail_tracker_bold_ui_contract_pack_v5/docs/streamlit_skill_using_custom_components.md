---
name: using-streamlit-custom-components
description: Using third-party Streamlit custom components. Use when extending Streamlit with community packages. Covers installation, popular custom components, and when to use them.
license: Apache-2.0
source: https://github.com/streamlit/agent-skills/tree/main/developing-with-streamlit/skills/using-streamlit-custom-components
---

# Streamlit custom components

Extend Streamlit with third-party custom components from the community.

## What are custom components?

Custom components are standalone Python libraries that add features not in Streamlit's core API. They're built by the community and can be installed like any Python package.

## Installation

Install using the PyPI package name (not the repo name—they can differ):

```bash
uv add <pypi-package-name>
```

Then import according to the component's documentation. The import name often differs from the package name too.

## Use with caution

Components are not maintained by Streamlit. Before adopting:

- **Check maintenance** — Is it actively maintained? Recent commits?
- **Check compatibility** — Does it work with your Streamlit version?
- **Check popularity** — GitHub stars, downloads, community usage
- **Consider alternatives** — Can you achieve this with core Streamlit?

Custom components can break when Streamlit updates, so prefer core features when possible.

## Popular custom components

### streamlit-keyup

Text input that fires on every keystroke instead of waiting for enter/blur. Useful for live search.

- **Repo:** https://github.com/blackary/streamlit-keyup

```bash
uv add streamlit-keyup
```

```python
from st_keyup import st_keyup

query = st_keyup("Search", debounce=300)  # 300ms debounce
filtered = df[df["name"].str.contains(query, case=False)]
st.dataframe(filtered)
```

### streamlit-bokeh

Official replacement for `st.bokeh_chart` (removed from Streamlit API). Maintained by Streamlit.

- **Repo:** https://github.com/streamlit/streamlit-bokeh

```bash
uv add streamlit-bokeh
```

```python
from bokeh.plotting import figure
from streamlit_bokeh import streamlit_bokeh

p = figure(title="Simple Line", x_axis_label="x", y_axis_label="y")
p.line([1, 2, 3, 4, 5], [6, 7, 2, 4, 5], line_width=2)
streamlit_bokeh(p)
```

### streamlit-aggrid

Interactive dataframes with sorting, filtering, cell editing, grouping, and pivoting.

- **Repo:** https://github.com/PablocFonseca/streamlit-aggrid

```bash
uv add streamlit-aggrid
```

```python
from st_aggrid import AgGrid

AgGrid(df, editable=True, filter=True)
```

**When to use aggrid over st.dataframe:**
- Interactive row grouping and pivoting
- Advanced filtering and sorting UI
- Complex cell editing workflows
- Custom cell renderers

### streamlit-folium

Interactive maps powered by Folium.

- **Repo:** https://github.com/randyzwitch/streamlit-folium

```bash
uv add streamlit-folium
```

```python
import folium
from streamlit_folium import st_folium

m = folium.Map(location=[37.7749, -122.4194], zoom_start=12)
st_folium(m, width=700)
```

### pygwalker

Tableau-like drag-and-drop data exploration.

- **Repo:** https://github.com/Kanaries/pygwalker

```bash
uv add pygwalker
```

```python
import pygwalker as pyg

pyg.walk(df, env="Streamlit")
```

### streamlit-extras

A collection of community utilities. Cherry-pick what you need.

- **Repo:** https://github.com/arnaudmiribel/streamlit-extras

```bash
uv add streamlit-extras
```

```python
from streamlit_extras.image_selector import image_selector

selection = image_selector(image, selections=["Region A", "Region B"])
```

## Discover more

Browse the custom component gallery: https://streamlit.io/components

## References

- [Components Gallery](https://streamlit.io/components)
- [Build a custom component](https://docs.streamlit.io/develop/concepts/custom-components)
