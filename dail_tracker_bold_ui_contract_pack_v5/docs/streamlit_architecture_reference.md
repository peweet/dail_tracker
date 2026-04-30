# Streamlit Architecture Reference

Consolidated from official Streamlit docs. Covers caching, session state, multipage apps, widgets, multithreading, and custom classes.

---

## Caching

### st.cache_data — for serializable data
Use for anything that returns DataFrames, arrays, dicts, strings, numbers.
- Creates a **copy** on each call — mutations don't affect the cache
- Safe for concurrent access
- Uses pickle under the hood

```python
@st.cache_data(ttl=3600, show_spinner=False)
def load_data(year: int) -> pd.DataFrame:
    return pd.read_parquet(f"data/{year}.parquet")
```

### st.cache_resource — for shared global objects
Use for database connections, ML models, DuckDB connections, file handles.
- Returns the **same object instance** every time (singleton)
- Mutations affect ALL sessions simultaneously — thread-safety is your responsibility
- Not serialized, so no copy overhead

```python
@st.cache_resource
def get_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute(sql)
    return conn
```

### Key parameters
```python
@st.cache_data(
    ttl=3600,          # seconds until cache expires (or datetime.timedelta)
    max_entries=1000,  # cap cache size to avoid memory growth
    show_spinner=False,
    hash_funcs={MyClass: lambda x: x.id},  # custom hashing for unhashable params
)
```

### Excluding params from hashing
Prefix with underscore — the param is passed but not used as a cache key:
```python
@st.cache_data
def fetch(_conn, num_rows):  # _conn not hashed — safe for connection objects
    return _conn.execute("SELECT * FROM t LIMIT ?", [num_rows]).df()
```

### Decision matrix
| Use case | Decorator |
|----------|-----------|
| CSV / parquet reads | `st.cache_data` |
| API calls | `st.cache_data` |
| DuckDB query results | `st.cache_data` |
| DuckDB connection object | `st.cache_resource` |
| ML model weights | `st.cache_resource` |
| Data transformation | `st.cache_data` |

---

## Session State

Streamlit reruns the entire script on every interaction. Session state persists values across reruns for a given browser session.

### Initialization pattern
Always check before setting — re-running the script would otherwise reset:
```python
if "selected_td" not in st.session_state:
    st.session_state["selected_td"] = None
```

Or use `setdefault`:
```python
st.session_state.setdefault("selected_td", None)
```

### Widget state
Widgets with a `key` are automatically synced to session state:
```python
st.selectbox("Year", options, key="selected_year")
# st.session_state["selected_year"] is now the selected value
```

**Restriction:** Cannot programmatically set `st.button` or `st.file_uploader` state — raises `StreamlitAPIException`.

### Callbacks
Triggered before the next script run — use for state transitions:
```python
def on_select():
    st.session_state["selected_td"] = st.session_state["td_picker"]

st.selectbox("Member", members, key="td_picker", on_change=on_select)
```

### Scope
- Persists within one browser tab session
- Persists across page navigations in multipage apps
- Lost on server restart or tab close

---

## Multipage Apps

### st.navigation + st.Page (preferred approach)

> **Note:** Name your pages directory `app_pages/` (not `pages/`). Using `pages/` conflicts with
> Streamlit's old file-based auto-discovery API and can cause unexpected behaviour.

```python
# streamlit_app.py (entrypoint)
import streamlit as st

pages = [
    st.Page("app_pages/attendance.py", title="Attendance", icon=":material/calendar_today:"),
    st.Page("app_pages/votes.py",      title="Votes",      icon=":material/how_to_vote:"),
]
pg = st.navigation(pages)
pg.run()
```

- `st.navigation` must be called exactly once per run
- Returns the active page object — call `.run()` to execute it
- Pages can be grouped using a dict: `{"Analytics": [page1, page2], "Admin": [page3]}`
- `position="hidden"` disables the auto sidebar menu (use `st.page_link` for custom nav)

### Dynamic pages (role-based access)
```python
if st.session_state.get("is_admin"):
    pg = st.navigation([public_pages + admin_pages])
else:
    pg = st.navigation(public_pages)
```

---

## Widget State Across Pages

Widgets defined in different pages have different IDs — their state resets when navigating away and back.

### Option 1: Define widget in the entrypoint file
Widgets in `streamlit_app.py` (sidebar filters, navigation) persist because the entrypoint runs on every rerun.

### Option 2: Dummy key pattern
```python
# Store real value under a permanent key
if "my_value" not in st.session_state:
    st.session_state["my_value"] = default

def _sync():
    st.session_state["my_value"] = st.session_state["_my_widget"]

st.selectbox("Choose", options, key="_my_widget", on_change=_sync,
             index=options.index(st.session_state["my_value"]))
```

### Option 3: Prevent cleanup (simplest)
At the top of any page that uses persistent keys:
```python
# Prevent Streamlit from cleaning up these keys on page switch
for key in ("selected_td", "selected_year"):
    if key in st.session_state:
        st.session_state[key] = st.session_state[key]
```

---

## Multithreading

### Architecture
Streamlit runs one **server thread** (Tornado) plus one **script thread** per session. Each interaction creates a new script thread.

### The NoSessionContext problem
Custom threads don't have a `ScriptRunContext` — calling `st.session_state`, `st.write`, etc. from them raises `streamlit.errors.NoSessionContext`.

### Safe pattern: do work in threads, display in script thread
```python
import threading

result = {}

def worker():
    result["data"] = expensive_io_call()

t = threading.Thread(target=worker)
t.start()
t.join()
st.write(result["data"])  # display from script thread, not worker
```

### Unsafe pattern: exposing ScriptRunContext
```python
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

add_script_run_ctx(thread, get_script_run_ctx())
```
⚠️ Not officially supported. May break across versions. Security risk if threads outlive the session. Avoid unless you have no alternative.

### When to use threading vs alternatives
| Need | Approach |
|------|----------|
| Cache expensive queries | `@st.cache_data` or `@st.cache_resource` |
| Non-blocking UI updates | `@st.fragment` |
| Parallel IO | threading (results-only pattern) |
| CPU-heavy computation | `multiprocessing` (threads don't bypass GIL) |

---

## Custom Classes

Streamlit reruns the script on every interaction, redefining classes each time. This breaks equality checks using `is` or `==` when instances come from different runs.

### Pattern 1: Define classes in a separate module (preferred)
```python
# models.py
class FilterState:
    def __init__(self, year, member):
        self.year = year
        self.member = member
```
Streamlit doesn't re-import external modules, so the class identity stays stable.

### Pattern 2: Override __eq__
```python
class FilterState:
    def __eq__(self, other):
        return isinstance(other, FilterState) and self.__dict__ == other.__dict__
```

### Pattern 3: st.cache_resource for singleton instances
```python
class AppConfig:
    @staticmethod
    @st.cache_resource
    def get():
        return AppConfig()
```

### Enum coercion (Streamlit ≥ 1.29)
`enumCoercion` is on by default — Enum members in selectbox/multiselect are automatically coerced to the latest class definition, so enum comparisons usually just work.

---

## st.image Reference

```python
st.image(
    image,                    # URL str, file path, SVG str, bytes, or list
    caption=None,             # str or list[str], supports Markdown
    width="content",          # "content" | "stretch" | int (pixels)
    clamp=False,              # clamp byte arrays to 0-255
    channels="RGB",           # "RGB" | "BGR" for numpy arrays
    output_format="auto",     # "JPEG" | "PNG" | "auto"
    link=None,                # URL to wrap image in a hyperlink (single image only)
)
```

**Accepted image types:**
- Remote URLs: `"https://example.com/image.png"`
- Local static assets: `"/app/static/logo.png"`
- SVG strings: `"<svg xmlns=...>...</svg>"`
- NumPy arrays, PIL Images, byte arrays
- List of any of the above (displays as a horizontal row)

```python
st.image("logo.png", width=120)
st.image("https://example.com/photo.jpg", caption="Caption text")
st.image(["img1.png", "img2.png"], caption=["First", "Second"])
```
