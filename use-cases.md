# DBTF — UI Use Cases

How the Digital Building Twin app is actually used, screen by screen. Written for UX/UI
developers: what the user sees, what they click, in what order, and what the app does back.

No API details here. Where a screen's behaviour is driven by something invisible (a page
reload, a background provisioning job), it is called out — those are the parts that shape the
UX.

Open the app at **<http://localhost:4200>**.

---

## 0. The app at a glance

### It is one page

There is **no routing**. `app.routes.ts` is empty. The whole application is a single screen
made of a toolbar, a sidenav and nested tab groups. Consequences you need to design around:

- **No deep links.** You cannot link to a twin, a building, a room or an asset. Refreshing
  always lands on the first tab.
- **No browser back.** Back leaves the app.
- **State is in memory**, except the selected twin (localStorage) and the detail-panel width.

### Two modes, one toggle

The toolbar carries a two-option toggle that swaps the entire body of the app:

| Mode | Toggle label | What it is for |
|---|---|---|
| `dbt` | **Building Twin** | Working *inside* one building: structure, assets, telemetry |
| `dbtf` | **Control Plane** | Administering the platform: twins and templates |

### Screen map

```
Toolbar  [Digital Building Twin]  ( Building Twin | Control Plane )   … context actions
─────────────────────────────────────────────────────────────────────────────────────
BUILDING TWIN mode                        CONTROL PLANE mode
┌───────────┬───────────────────────┐     ┌─────────────────────────────────────────┐
│ Buildings │ Structure │ Assets    │     │ DBTs │ Templates                        │
│ (sidenav) │ Groups    │ Telemetry │     │                                         │
│           │ Dashboard             │     │  master list  │  detail pane            │
│  · Site A │ ┌──────┬────────────┐ │     │               │                         │
│  · Site B │ │ tree │ detail     │ │     │               │                         │
│           │ └──────┴────────────┘ │     │               │                         │
└───────────┴───────────────────────┘     └─────────────────────────────────────────┘
```

Toolbar context actions change with the active tab:

| Mode / tab | Buttons on the right of the toolbar |
|---|---|
| Building Twin → Structure | *DBT picker* · **Import .ttl** · **Export .ttl** · **New Building** |
| Building Twin → Assets | *DBT picker* · **Place Asset** · **New Asset** |
| Building Twin → other tabs | *DBT picker* only |
| Control Plane → DBTs | **New DBT** |
| Control Plane → Templates | *(none — the + button lives in the list header)* |

### The DBT picker — the most important control in the app

A dropdown in the toolbar (only in Building Twin mode) showing the current twin. It decides
which building's data every other screen shows.

- The first entry is always **"Default building"** — the demo twin.
- Then one entry per twin, each with its state next to the name.
- A divider, then **"New DBT…"**.
- Retired twins are hidden. So is the `default` twin's own entry (it is already offered as
  "Default building").

**Selecting a different twin reloads the whole page.** This is deliberate — every feature
loads its data eagerly when it is constructed, so a reload is the only reliable way to
re-scope all of it. For the user it reads as a hard context switch, which is roughly honest,
but it does throw away everything they had open. Worth knowing before you design anything that
depends on surviving a twin switch.

### Terms that look alike but are not

| Term | Where | What it is |
|---|---|---|
| **Structure template** | Control Plane → Templates | A platform-level, versioned vocabulary. Reusable across twins. |
| **Structure Rules** | (per-twin, API only today) | The *live copy* of a vocabulary inside one twin. What space creation is actually validated against. |
| **Assign** vs **Apply** | Control Plane → DBTs | *Assign* records which template a twin should follow. *Apply* pushes it into the twin's live rules. Two buttons, two different effects. |

---

## Part A — Control Plane mode

Switch the toolbar toggle to **Control Plane**. Two tabs: **DBTs** and **Templates**.

Both tabs use the same master/detail layout: a list on the left, a detail pane on the right,
and an empty hint in the detail pane until something is selected.

---

### UC-01 — Create a digital building twin

**Where:** Control Plane → DBTs · **Actor:** platform engineer

The starting point. A twin is one customer building with its own isolated storage.

**Steps**

1. Click **New DBT** in the toolbar. A dialog opens: *"New Digital Building Twin"*.
2. It explains what will happen — *"Creates and provisions an isolated twin (own graph
   repository and data schemas)."* — and asks for one thing: a **Name**
   (placeholder: `e.g. Alpha HQ`).
3. Click **Create & provision**. The button switches to a spinner and the label
   **"Provisioning…"**; the Name field goes read-only and Cancel is disabled.
4. When it finishes, the dialog closes, a snackbar confirms *"DBT «name» provisioned"*, and
   **the page reloads** with the new twin as the active scope.

**What the user sees afterwards:** the new twin appears in the DBTs list with an **ACTIVE**
badge and a **current scope** badge.

**UX notes**
- This single button does three things behind the scenes (creates the ownership records,
  registers the twin, then provisions its storage). There is no progress detail — just a
  spinner. Provisioning is real work across five data stores, so this wait is not instant.
- If it fails, the error appears **inside the dialog**, in red, and the dialog stays open so
  the user can retry. A twin that failed provisioning is still created — it shows up in the
  list as **FAILED**, and can be retried with the **Activate** button (UC-06).
- The picker's **"New DBT…"** entry opens the same dialog.

**Empty state:** with no twins, the DBTs list reads *"No DBTs registered yet. Click «New DBT»
in the toolbar to create one."*

---

### UC-02 — Create a structure template

**Where:** Control Plane → Templates · **Actor:** solution architect

A structure template defines what shape a building may have: which space types exist, what may
contain what, and which fields each type carries. Build it once, reuse it across every twin of
that kind.

**Steps**

1. Go to the **Templates** tab. Click the **+** button in the *Structure Templates* list header.
2. The *"New structure template"* dialog asks for:
   - **Name** — warns inline *"A template with this name already exists"* if taken.
   - **Description**
   - **Start from** — either **Empty**, or **Copy of «name» (vN)** to fork an existing template.
3. Click **Create**. The template appears in the list marked **unsaved draft**, and the editor
   opens on the right.
4. **Add space types.** In the *Space types* panel, type a name in **New type**
   (placeholder `e.g. ServerRoom`) and click **Add** (or press Enter).
5. **Define each type.** Expand a type and click **Edit rules**:
   - *Can contain* — a checkbox grid of every other type.
   - *Fields* — rows of **Name** / **Type** / **Required**, with **Add field** and a delete
     button per row.
   - **Apply** or **Cancel**.
   A type that nothing else lists as a child is tagged **root**.
6. **Add relationship types.** In the *Relationship types* panel, both a code
   (`e.g. controls`) and a **What it means** description are required before **Add** enables.
7. Click **Publish v1**.

**What the user sees afterwards:** the template gains a version chip (**v1**) and an **ACTIVE**
badge.

**UX notes**
- Everything before Publish is an **in-memory draft**. There is no autosave, no dirty-state
  warning, and no confirmation on navigating away. Switching to another template or leaving
  the tab loses the work silently. (Summarised under *Unsaved work* in the closing section.)
- Collapsed types show a summary — *"3 child type(s) · 5 field(s)"* — which is a good pattern
  worth keeping.
- Space types can be renamed inline (pencil → inline input with ✓ / ✕) and removed. Relationship
  types have edit and remove icon buttons per row, and show *"No description yet — edit to add
  one."* when one is missing.

**Empty states:** *"No templates published yet. Create one with the + button above."* ·
*"No space types yet — add the first one above."* · *"No relationship types yet."*

---

### UC-03 — Publish a new version of a template

**Where:** Control Plane → Templates · **Actor:** solution architect

Templates are versioned so a change can be rolled out to twins one at a time rather than all at
once.

**Steps**

1. Select the template. Use the **Version** dropdown (`v2 — ACTIVE`, `v1 — ACTIVE`, …) to pick
   the version to start from.
2. Edit the draft as in UC-02.
3. Click **Publish new version (v3)** — the button label always shows the number you are about
   to create.

**What the user sees afterwards:** the list row shows the new version chip and a
*"N versions"* count.

**UX notes**
- Versions are immutable once published. The button tooltip says so: *"Save the draft as a new
  immutable version — DBTs keep the version they reference until re-assigned."*
- **Retire** soft-retires the selected version, which removes it from the assign dropdown in
  UC-04. Twins already on it are unaffected. The button is disabled when the version is already
  retired.
- Editing a published version and forgetting to publish loses the changes — same gap as UC-02.

---

### UC-04 — Assign a template to a twin, then apply it

**Where:** Control Plane → DBTs · **Actor:** platform engineer

This is the two-step that trips people up. **Assign** says which template the twin follows.
**Apply** actually writes that vocabulary into the twin. Both buttons sit on the same row.

**Steps**

1. Select an **ACTIVE** twin in the DBTs list. The header shows its name and state badge; the
   meta grid shows Slug / Tenant / **Template** / Created / Updated. (Step 4 needs ACTIVE —
   **Apply now** is disabled in every other state, see the button table below.)
2. Pick a template from the **Structure template** dropdown — options read
   `standard-building (v2)`.
3. Click **Assign template**. The *Template* row in the meta grid updates to the template name
   and version.
4. Click **Apply now**. A spinner runs; a snackbar reports the applied template.

**Button states — worth memorising, because nothing on screen explains them**

| Button | Disabled when |
|---|---|
| **Assign template** | twin is RETIRED · no template picked · the picked one is already assigned |
| **Apply now** | twin is **not ACTIVE** · no template assigned yet · already applying |

**UX notes**
- The only explanation of the difference is in tooltips: *"The building import applies this
  template's vocabulary to the twin before importing"* on Assign, and *"Overwrite this twin's
  live Structure Rules with the assigned template's vocabulary"* on Apply now. A user who does
  not hover gets no hint that Assign alone did nothing visible.
- **Apply now overwrites.** There is no confirmation dialog and no preview of what changes.
- Apply can be rejected — most commonly when the new vocabulary would remove a space type the
  twin still uses. The error surfaces as a snackbar; the twin keeps its current rules.
- If no templates exist, the dropdown is disabled and shows the hint *"No templates published —
  run seed-control-plane.ps1"*. That is developer-facing text sitting in a user-facing screen.

---

### UC-05 — Seed a twin from a building file

**Where:** Control Plane → DBTs · **Actor:** platform engineer

The fast way to fill an empty twin: one `.ttl` file describing spaces, assets and asset groups.

**Steps**

1. Select an **ACTIVE** twin.
2. Click **Import building (.ttl)** — tooltip: *"Seed this twin from one .ttl: assigned template
   → spaces → assets → groups"*. A native file picker opens (`.ttl` / `text/turtle`).
3. Pick the file. A spinner runs in the actions row.
4. A snackbar reports the result: how many spaces, assets and groups were created.

**What the user sees afterwards:** switching to Building Twin mode on this twin shows the
imported building in the Buildings sidenav.

**UX notes**
- The button only appears when the twin is ACTIVE.
- The import applies the assigned template first, so UC-04 step 3 is enough — you do not need
  to click Apply now before importing.
- Import is partly forgiving: spaces are all-or-nothing, but a bad asset or group becomes a
  warning rather than aborting. Those warnings are returned but the UI currently only surfaces
  the counts, so partial failures are easy to miss.
- Sample files live in `building-structure-service/samples/ttl/`.

---

### UC-06 — Manage a twin's lifecycle and health

**Where:** Control Plane → DBTs · **Actor:** platform operator

**Steps**

1. **Rename** — edit the **Name** field, click **Rename**. (The slug never changes, so the twin
   keeps its storage identity. The UI does not say this; the slug is shown read-only in the
   meta grid.)
2. **Assign building** — a free-text **Building reference** field
   (placeholder: `address, cadastral id, …`), then the **Assign building** button.
3. **Lifecycle** — the actions row shows only the buttons that are legal right now:

   | Button | Appears when | Note |
   |---|---|---|
   | **Pause** | twin is ACTIVE | |
   | **Resume** | twin is PAUSED | |
   | **Activate** | twin is DRAFT or FAILED | tooltip: *"Runs the provisioning saga (GraphDB repo + per-store schemas)"* — this is the retry path after a failed provision |
   | **Retire** | any non-retired state | opens a confirmation dialog |

4. **Retire** opens *"Retire DBT"*: *"Retire «name»? The twin becomes read-only history —
   RETIRED is terminal and cannot be undone (data is preserved, never deleted)."*
   Confirm with the red **Retire** button.
5. **Health** — a section at the bottom with a refresh icon (*"Re-run the observe probe"*). It
   shows an **Overall** badge (UP / DEGRADED), a *(not provisioned)* marker when relevant, and a
   table of stores with per-store status and detail.

**UX notes**
- A retired twin's detail pane replaces every action with *"This twin is retired — read-only
  history, no further actions."* All the edit fields go disabled.
- Retire is confirmed by a dialog, as most destructive actions in the app are. **Apply now** is
  the exception: it overwrites the twin's live rules with no confirmation at all, even though it
  is the most consequential action on this screen. See *Destructive actions* in the closing
  section for the full inventory.
- Health is a manual probe, not live — it loads when a twin is selected and otherwise only on
  the refresh button.

---

### UC-07 — Switch into a twin to work in it

**Where:** Control Plane → DBTs → **Open this DBT**

**Steps**

1. Select an ACTIVE twin.
2. Click **Open this DBT** (tooltip: *"Scope the UI to this twin and open its working view"*).
3. The app scopes to that twin and switches to Building Twin mode.

Equivalent: pick the twin from the toolbar **DBT picker**. Either way the page reloads.

---

## Part B — Building Twin mode

Toggle to **Building Twin**. A **Buildings** sidenav appears on the left, and the body becomes
five tabs: **Structure · Assets · Groups · Telemetry · Dashboard**.

Almost everything here depends on a building being selected in the sidenav. With none, each tab
shows its own empty hint.

**Empty state:** *"No buildings yet. Click «New Building» to create one."*

---

### UC-08 — Build the spatial structure

**Where:** Structure tab · **Actor:** building modeller

**Steps**

1. Click **New Building** in the toolbar to create the root space. The *element form* dialog
   asks for **Name**, **Type** (a dropdown of the types the vocabulary allows at this position)
   and then one field per property the type declares — required ones marked with a red `*`.
2. Select the building in the **Buildings** sidenav. The tree renders on the left.
3. Hover any node to reveal its row of icon actions:

   | Icon | Action | Shown when |
   |---|---|---|
   | ➕ | **Add child** | always (disabled unless the node is ACTIVE) |
   | 📄→ | **Move** | always |
   | ⤚ | **Merge** | the node has a parent |
   | ⤙ | **Split** | the node has a parent **and** no children |
   | ✏️ | **Edit** | always |
   | 🗑 | **Delete** | always |

4. **Add child** opens the same element form, with the Type dropdown already filtered to what
   this parent may contain. If the type is a leaf, the dialog says *"This space cannot have
   children."* and the submit button stays disabled.
5. Click a node's **name** to open the detail panel on the right.

**UX notes**
- The tree toolbar has one control: a **Show deleted** toggle. Deleted nodes then appear greyed
  with a **deleted** badge; merged ones with a **merged** badge (*"Merged into a newer space"*).
- Every action is disabled on non-ACTIVE nodes, with no explanation beyond the badge.
- The detail panel is **resizable** by dragging its left edge, and the width persists across
  sessions.
- All six actions are icon-only with tooltips. On a wide tree row this is a lot of unlabelled
  affordance.

---

### UC-09 — Reorganise spaces: move, merge, split, delete

**Where:** Structure tab, node actions · **Actor:** building modeller

**Move**

1. Click the **Move** icon. The *"Move «name»"* dialog asks for a new parent.
2. Confirm.

**Merge** — combine sibling spaces into one

1. Click **Merge**. The dialog *"Merge spaces into one"* shows the **Primary** (the node you
   clicked) and its type.
2. **Also merge** — a multi-select of active same-type siblings.
3. **Merged space name**, and an optional **Reason**.
4. The dialog explains the outcome: *"The primary and selected spaces will be superseded by one
   new «Type». History is kept and their relationships move to the merged space."*
5. Click **Merge**.

If there is nothing to merge with, the dialog says *"No active «Type» siblings to merge with."*

**Split** — divide one space into several

1. Click **Split**. The dialog opens with **two** name rows by default.
2. Add rows with **Add «Type»**; remove with the ✕ (disabled at two rows — the minimum).
   Optional **Reason**.
3. The header warns: *"The room is superseded by the new «Type»s below. History is kept; its
   relationships (sensors, heating, adjacency) are retired and listed for you to re-attach."*
4. Click **Split into N** — the button label counts the rows.
5. Afterwards a *"Re-attach connections"* dialog lists the relationships that were retired, so
   the user can wire them to the right new space.

**Delete** — a *"Delete Space"* confirmation. It is a soft delete: the node stays visible under
**Show deleted**.

**UX notes**
- Merge and Split both explain their consequences in the dialog body, which is the right call —
  they are the two operations users are least likely to predict.
- Split's follow-up re-attach step is the only multi-stage flow in the app.

---

### UC-10 — Inspect and enrich a space

**Where:** Structure tab → detail panel · **Actor:** building modeller / analyst

Clicking a node opens a detail panel with a fixed header (name + edit pencil), five always-on
fields (**ID**, **Type**, **Status**, **Parent**, **Created**) and a long accordion. Panels open
lazily — several only fetch their data when first expanded.

| Panel | What it holds | Open by default |
|---|---|---|
| **Properties** | the type's declared fields, with units | ✅ (when non-empty) |
| **Contents** | chips counting sub-spaces by type | |
| **Geometry** | the 2D footprint editor — see UC-11 | |
| **Relationships** | typed edges, a type filter, **Add relationship** — see UC-12 | ✅ |
| **Assets** | assets placed in this space | ✅ |
| **Twin snapshot** | cross-domain aggregate: structure + assets + latest readings + geometry | |
| **Heating chain** | room → radiators → heating circuit → members | |
| **Comfort over time** | comfort compliance trend | |
| **Heating diagnosis** | is heating underperforming here | |
| **Thermal response** | how fast the room responds | |
| **Tenant comfort** | match against occupant preference | |
| **Predictive maintenance** | drift toward failure | |
| **Monthly comfort (ESG)** | monthly rollup | |
| **Traverse** | walk the relationship graph from here | |
| **External Identifiers** | BMS / CMMS / ERP aliases — see UC-13 | |
| **Validation** | structural validation report for the subtree | |

**UX notes**
- **Sixteen accordion panels in one column.** There is no grouping, no search, and no
  distinction between "core facts" (Properties, Contents, Assets) and "analytics" (seven of the
  panels). For a user who just wants a room's area, this is a lot of scrolling. This screen is
  also worth reconsidering — grouping the analytics behind a sub-tab would halve the list.
- The panel order is fixed, and the edit pencil in the header opens the same element form as
  the tree's Edit action.

---

### UC-11 — Draw a room footprint

**Where:** Structure tab → detail panel → **Geometry** · **Actor:** building modeller

A 2D polygon editor on an SVG canvas with a 1 m grid.

**Steps — new footprint**

1. Expand **Geometry** (*"2D footprint — draw & edit"*).
2. Either click **Start with a rectangle**, or follow the hint *"…or click on the canvas to
   place vertices."*
3. While drawing, the toolbar shows **Undo point** and a live count:
   *"4 points — need at least 3."*
4. Set **Elevation (m)** and **Height (m)** in the fields below the canvas. **Area** and
   **Volume** update live.
5. Click **Save**.

**Steps — editing an existing footprint**

The toolbar hint becomes: *"Drag a vertex to move it · click a small dot to split an edge ·
double-click a vertex to remove it."* **Reset** discards changes; **Save** commits.

**UX notes**
- Other footprints on the same floor render as read-only grey context polygons with their
  names — genuinely helpful for placing a room correctly.
- **Fit view** re-frames the canvas.
- Overlapping another room on the same floor is rejected on save. The user gets no warning
  while dragging, so the failure arrives late.
- Save is disabled until the polygon is valid and something has changed.

---

### UC-12 — Connect spaces and assets

**Where:** Structure tab → detail panel → **Relationships** · **Actor:** building modeller

**Steps**

1. Expand **Relationships**. Existing edges are listed with a direction arrow (→ outgoing,
   ← incoming), the relationship type, and the other end — a space (with its type) or an asset
   (with a sensor icon and the label `· asset`).
2. Filter with **Filter by type** (default *All types*; each option carries the type's
   description as a tooltip).
3. Click **Add relationship** to open the *"Add Relationship"* dialog.
4. Delete an edge with the trash icon — only available on ACTIVE relationships.

**UX notes**
- The empty state adapts to the filter: *"No «serves» relationships."* vs *"No relationships."*
- Inactive relationships stay listed, greyed, without a delete button.

---

### UC-13 — Map external system identifiers

**Where:** Structure tab → detail panel → **External Identifiers**

Expand the panel and register an alias via the *"Register External ID"* dialog — the system
(BMS, CMMS, ERP, IFC…) and its identifier for this space. Existing aliases are listed.

---

### UC-14 — Import and export a structure file

**Where:** Structure tab toolbar

- **Import .ttl** opens a file picker and imports spaces into the current twin.
- **Export .ttl** downloads the selected building's structure. Disabled until a building is
  selected in the sidenav.

Unlike UC-05, this handles **spaces only** — no assets, no groups.

---

### UC-15 — Register an asset

**Where:** Assets tab · **Actor:** building modeller

**Steps**

1. Click **New Asset** in the toolbar. The *"New Asset"* dialog asks for:
   - **Asset Type** — grouped by category from the equipment catalog. While loading it shows
     *"Loading catalog…"*; if the catalog is empty, *"No asset types available"*.
   - **Name**
   - **Serial number (optional)** · **Manufacturer (optional)** · **Model (optional)**
   - **Place in space (optional)** — a space dropdown whose first entry is
     *"— Leave unplaced —"*.
2. Click **Register**.

**What the user sees afterwards:** a new row in the assets table with columns
**Name · Type · Space · Placement · Status**, placement and status rendered as coloured chips.

**UX notes**
- The header count updates live: *"Assets (165)"*.
- Selecting a row opens a **resizable** detail pane on the right with a close ✕.
- The list has a refresh icon but **no filters and no search** — with the demo dataset's ~165
  assets, that is a real problem. Filtering exists in the API; it is simply not surfaced.

---

### UC-16 — Place an asset in a space

**Where:** Assets tab · **Actor:** building modeller

**Steps**

1. Click **Place Asset** in the toolbar.
2. The dialog offers three dropdowns: **Asset** (only unplaced ones, labelled
   `name (Type)`), **Space** (grouped by space type), and **Placement Status**
   (`PLACED` / `PARTIAL` / `MOBILE`).
3. Click **Place**.

If everything is already placed, the dialog shows *"No unplaced assets available."* and the
submit button is disabled.

Assets can also be placed from their own detail pane, via the **Update Location** dialog under
the *Location* section.

---

### UC-17 — Work with one asset

**Where:** Assets tab → detail pane · **Actor:** facility manager

The pane is a flat list of labelled sections, in this order:

| Section | What the user can do |
|---|---|
| **Metadata** | edit name, manufacturer, model, serial, installation date, lifetime, cost |
| **Design Intent** | record what was *specified*, and compare it against what was installed |
| **Location** | current space + placement, with **Update Location** |
| **Documents** | list and **Attach Document** (manuals, datasheets, certificates) |
| **Groups** | which asset groups this asset belongs to |
| **History** | the lifecycle event log, plus **Register Event** (planned / maintenance / failure) |
| **Commissioning** | activities, **Register Commissioning**, and closing one with an outcome |
| **Actions** | **Decommission** and **Void**, both in warn red |

When an asset is not ACTIVE, the Actions section reads *"No actions available."*

**UX notes**
- **Decommission** and **Void** sit side by side in identical red styling, but mean very
  different things — decommission retires equipment that really existed; void erases a
  registration mistake. Neither label explains this, and both are irreversible.
- Sections are always expanded — this is a long scroll compared with the accordion used for
  spaces. Two different disclosure patterns for two comparable detail views.

---

### UC-18 — Manage asset groups (heating circuits)

**Where:** Groups tab · **Actor:** building modeller

Systems that span rooms — heating circuits, ventilation — are modelled as groups.

**Steps**

1. Click **+** in the *Asset Groups* header → the *"Create Asset Group"* dialog.
2. Narrow the list with **Filter by type** (*All types* by default).
3. Select a group. The detail pane shows its name, type, description, and a validation chip:
   **Composition complete** (green ✓) or **Composition incomplete** (red !). If the type has no
   defined composition, it reads *"No catalog composition defined for this type"*.
4. Findings are listed underneath with severity icons — errors and warnings, e.g. a missing pump.
5. **Add member** opens a dialog to attach an asset with a role. **Served Spaces** manages which
   spaces the group serves. The red ⊖ button dissolves the group (tooltip: *"Dissolve group"*).

**Live snapshot:** for heating circuits the pane also shows an operating-point strip —
**Supply** / **Return** / **ΔT** / **Pump** — plus detected faults.

**Empty states:** *"No groups found."* · *"Select a group to see its members."*

---

### UC-19 — Watch telemetry

**Where:** Telemetry tab · **Actor:** analyst

Four sub-tabs:

| Sub-tab | Shows |
|---|---|
| **Live** | current readings |
| **Readings** | historical series per asset and metric |
| **Events** | alarms, anomalies and state changes |
| **Coverage** | how much data is correctly linked to building entities |

**UX note that will bite you:** the live data feeder only writes to the **default** twin. A twin
created through the UI (UC-01) has **no live telemetry at all** — every chart is empty, and
nothing on the screen explains why. Users will read this as a broken app, so it is worth
explicit messaging.

---

### UC-20 — Read the building dashboard

**Where:** Dashboard tab · **Actor:** building manager

A read-only rollup for the building selected in the sidenav.

1. **Four KPI cards** across the top: *Building comfort* (mean compliance across rooms),
   *Rooms in violation* (turns warning-coloured above zero), *Worst circuit* (energy intensity),
   *Best forecast accuracy* (next-day heating demand). Missing data shows as **n/a**.
2. **Retrofit ranking — energy intensity (worst first)**: a numbered list of circuits by
   kWh/m².
3. **Heating circuits**: one card per circuit with *Intensity*, *vs OAT* (a Pearson correlation,
   explained only in a tooltip), *Next-day demand* and *Pred. accuracy*.
4. **Rooms needing attention — lowest comfort compliance**: worst rooms, each with a warning
   icon when the violation is persistent.

**Empty states:** *"Select a building from the list to see its dashboard."* and
*"No analytics available for this building yet."*

**UX notes**
- This is the only screen in the app designed as a dashboard rather than a form, and the only
  one that is purely read-only.
- `r = 0.83` under a label reading *"vs OAT"* is unexplained outside its tooltip.

---

## Part C — UX notes and known gaps

Collected so you do not have to rediscover them.

### Navigation

| Gap | Effect |
|---|---|
| No routing | No deep links, no shareable URLs, no browser back, no bookmarking a room or asset |
| Twin switch reloads the page | Everything the user had open is lost, every time |
| Tabs keep their content mounted | Switching tabs is instant, but stale data can persist — the Templates tab explicitly triggers a refresh when you return to DBTs, because otherwise a newly published template would not show up |
| Selection is not shared between tabs | The Assets tab does not know which space is selected in Structure |

### Feedback and errors

- Success and failure are both reported by **snackbar**. There is no persistent error surface,
  so a message missed is a message gone.
- Long operations (provisioning, import, apply) show a spinner but **no progress and no
  cancel**.
- Backend errors come in two different shapes and are normalised by an HTTP interceptor before
  display — so messages are readable, but their tone and length vary a lot by which service
  produced them.
- Several errors are developer-facing in a user-facing place. The clearest example is
  *"No templates published — run seed-control-plane.ps1"*.

### Destructive actions

| Action | Confirmed? | Reversible? |
|---|---|---|
| Retire DBT | ✅ dialog | ❌ terminal |
| Apply template ("Apply now") | ❌ | ❌ overwrites the twin's live rules |
| Delete space | ✅ dialog | soft — recoverable via *Show deleted* |
| Merge / Split | ✅ explained in-dialog | history kept, but relationships are affected |
| Decommission asset | ✅ dialog | ❌ |
| Void asset | ✅ dialog | ❌ |
| Dissolve group | tooltip only | ❌ |
| Retire template version | ❌ | soft |
| Remove a space type from a template draft | ❌ | draft only — but the draft itself is unsaved |

The inconsistency is the issue: the single most consequential action in Part A (**Apply now**)
has no confirmation, while deleting one soft-deletable space has one.

### Unsaved work

The **template editor** holds everything in memory until Publish. No autosave, no dirty
indicator, no navigation guard. Switching templates, switching tabs or switching twins discards
it silently. This is the highest-value fix in the app.

### Inconsistencies worth reconciling

- **Two disclosure patterns for detail views** — spaces use a 16-panel accordion, assets use
  eight always-open sections.
- **Two creation patterns** — templates edit inline in the detail pane; almost everything else
  uses a modal dialog.
- **Icon-only actions** in the structure tree (six per row) versus labelled buttons everywhere
  else.
- **No list filtering** in Assets, where the demo dataset alone has ~165 rows, while Groups and
  Relationships both have filters.

### Present in the codebase but unreachable from the UI

- **Structure Rules** (`SchemaAdminComponent`) — the per-twin live vocabulary editor. Referenced
  by name in the Templates tab's own empty hint (*"the per-twin «Structure Rules» tab edits the
  live copy"*), but there is no such tab. Users are pointed at a screen that does not exist.
- The entire **device catalog** feature (register device, scan bus, ingestion status).

### Terminology to fix or explain

| Shown | Means |
|---|---|
| **DBT** | Digital Building Twin. The abbreviation appears in the toolbar, dialogs and list headers, and is never expanded except in the create dialog's title. |
| **Slug** | The twin's immutable storage identity. Shown as a read-only monospace value with no explanation. |
| **Assign** vs **Apply now** | See UC-04 — the distinction is only in tooltips. |
| **Provisioning** | Creating the twin's isolated storage. Appears as a button label mid-operation. |
| **Composition complete / incomplete** | Whether a group has all the member roles its type expects. |
| **vs OAT** | Correlation against outside air temperature. |
| **New Building** | Creates a **root space**, whatever the vocabulary allows at the top — in the standard template that is a `Site`, not a `Building`. The sidenav it feeds is also labelled *Buildings*. Both labels are wrong whenever the root type is not literally a building. |

### Screen inventory

| Screen | Component | Path under `building-twin-ui/src/app/` |
|---|---|---|
| Shell, toolbar, tabs | `App` | `app.ts` / `app.html` |
| Twin scope picker | `DbtPickerComponent` | `shared/dbt-picker/` |
| New twin dialog | `DbtCreateDialogComponent` | `shared/dbt-picker/dbt-create-dialog.component.ts` |
| DBTs admin | `DbtAdminComponent` | `features/dbt-admin/` |
| Templates list + detail | `StructureTemplatesComponent` | `features/structure-templates/` |
| Template vocabulary editor | `TemplateEditorComponent` | `features/structure-templates/template-editor/` |
| Structure tree | `BuildingTreeComponent` | `features/building-structure/components/building-tree/` |
| Space create/edit dialog | `ElementFormComponent` | `.../components/element-form/` |
| Space detail accordion | `ElementDetailComponent` | `.../components/element-detail/` |
| Footprint editor | `GeometryEditorComponent` | `.../components/geometry-editor/` |
| Live vocabulary editor *(unreachable)* | `SchemaAdminComponent` | `.../components/schema-admin/` |
| Assets list + dialogs | `AllAssetsComponent` | `features/asset-management/components/all-assets/` |
| Asset detail | `AssetDetailComponent` | `.../components/asset-detail/` |
| Assets within a space | `SpaceAssetsComponent` | `.../components/space-assets/` |
| Asset groups | `AssetGroupsComponent` | `.../components/asset-groups/` |
| Telemetry tabs | `TelemetryDashboardComponent` | `features/telemetry/components/telemetry-dashboard/` |
| Building dashboard | `BuildingDashboardComponent` | `features/dashboard/components/building-dashboard/` |
| Device catalog *(unreachable)* | `DeviceCatalogComponent` | `features/device-catalog/` |

---

## Appendix — the happy path, end to end

For someone seeing the app for the first time:

1. **Control Plane → Templates** → **+** → name it, add space types and relationship types →
   **Publish v1**. *(UC-02)*
2. **Control Plane → DBTs** → **New DBT** → name → **Create & provision**. *(UC-01)*
3. Still on **DBTs**: pick the template → **Assign template** → **Apply now**. *(UC-04)*
4. Optionally **Import building (.ttl)** to fill it in one shot. *(UC-05)*
5. **Open this DBT** → the app switches to Building Twin mode on that twin. *(UC-07)*
6. **Structure**: **New Building**, then build the tree with **Add child**. *(UC-08)*
7. Select a room → **Geometry** → draw its footprint. *(UC-11)*
8. **Assets**: **New Asset**, then **Place Asset**. *(UC-15, UC-16)*
9. **Groups**: create a heating circuit, add members, set served spaces. *(UC-18)*
10. **Telemetry** and **Dashboard** — remembering that a hand-made twin has no live data.
    *(UC-19, UC-20)*
