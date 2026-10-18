# Planning workspace

Start here to find planning code, documentation and local working files.

| I want to… | Open | What is there |
| --- | --- | --- |
| Work with public planning data | [Civic planning guide](civic/README.md) | Applications, appeals, decision profiles and public-data extractors |
| Understand the private SpecPlan product | [Product overview](product/README.md) | Product scope, setup and verification |
| Find private resources or understand the loose files here | [Resource map](product/RESOURCES.md) | Rules, spatial data, harvested documents, CSVs, logs and checkpoints |
| Find a SpecPlan document | [Documentation guide](product/doc/README.md) | Architecture, procedures, plans, research, evidence and historical records |

`civic/` belongs to the public Dáil Tracker repository. `product/` is a separate,
private Git repository; its links are available only in a checkout containing that repository.
Keep their ownership and verification separate.

## Understand the folders

```text
planning/
  README.md            Start here
  civic/               Public planning data code; see its README
  product/             Private SpecPlan repository; see RESOURCES.md
  _files*/             Local downloaded planning documents and retained text
  _open_register/      Local register acquisition cache
  _probe_*/            Local investigation output
  *.csv                Local harvest inputs, observations and derived exports
  *.lock               Coordination files used by writers
  _*done*.txt           Harvest checkpoints and completion records
  *.log                Run diagnostics
```

The loose files and underscore folders are a **working area**. A filename alone does
not establish which export is current, whether a run completed, or whether a source
has been reviewed. Use the [private resource map](product/RESOURCES.md) to find their
owning tools and instructions.

## Keep it understandable

Put public source code under `civic/` and private product material under `product/`.
Add a link from the relevant guide when introducing a new resource family.
Use the [private documentation folders](product/doc/README.md) for new product notes.

Several tools still read and write the loose files at these exact paths. Relocation
must update those readers, writers and checkpoint conventions together; do not move
or delete them just because they look temporary.

For agent instructions, start at [the repository guide](../AGENTS.md), then
[the private guide](product/AGENTS.md) or [the civic extractor guide](civic/extractors/AGENTS.md).
