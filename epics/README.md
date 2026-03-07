# Epics

This directory contains all planned and completed work for this repository,
managed as **Epic-level tasks** using local files instead of an external
ticketing system.

Exactly **one Epic may be active at a time**.

Claude Code is expected to treat the filesystem structure here as the source
of truth for scope and status.

---

## Directory Structure

epics/
├── active/ # The single Epic currently being worked on
├── backlog/ # Future Epics; not yet started
└── done/ # Completed Epics (historical record)

### `active/`

- Must contain **exactly one** Epic file
- Claude may only work on the Epic in this directory
- If more than one Epic is present, Claude should stop and ask for clarification

### `backlog/`

- Epics that are defined but not yet active
- Claude must not work on these
- Files may be edited only to clarify scope, not to implement work

### `done/`

- Completed Epics
- Treated as immutable historical records
- Changes here should be rare and explicit

---

## Epic Files

Each Epic is represented by a single Markdown file.

Epic files define:

- the objective
- the scope and non-goals
- a checklist of tasks
- constraints and invariants
- the definition of done

Epic files are intended to be **operational**, not narrative.

---

## Workflow Rules

- Work proceeds **one Epic at a time**
- Tasks are completed in order unless explicitly stated otherwise
- Claude should check off tasks only when their acceptance criteria are met
- If required work is outside the stated scope, propose changes to the Epic
  file before implementing anything

If no Epic is present in `active/`, no work should be performed.

---

## Source of Truth

The contents of this directory are authoritative.

External issue trackers, TODO comments, or ad-hoc task lists should not be
treated as substitutes for Epic files.

Progress is tracked by:

- task checkboxes
- file movement between directories
- git history

---

## Philosophy

This system favors:

- explicit scope
- serialized focus
- local, inspectable state
- minimal process overhead

It is intentionally simple and intentionally manual.
