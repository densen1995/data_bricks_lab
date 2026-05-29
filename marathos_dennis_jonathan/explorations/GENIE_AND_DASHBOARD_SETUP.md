# Tasks 6 & 7 — Dashboard + Marathos Genie setup

Both are UI-driven in Databricks. Everything they need (gold views, validation
notebook, widget SQL) is already in the repo — the steps below are just the
clicks left to do in the workspace.

## Task 6 — Lakeview dashboard

1. **Dashboards → Create dashboard**, name it `marathos_overview`.
2. Set the data source to the Serverless Starter Warehouse.
3. Open `explorations/03_dashboard_queries.sql`. For each cell:
   - Copy the SQL into a new **Dataset** in the dashboard.
   - Add a **Widget** of the type called out in the table at the top of that file (counter, bar chart, etc.).
4. Add a **Markdown widget** at the top right with:

   ```markdown
   **🤖 Ask Marathos Genie**

   [Open the Genie space →](<paste Genie space URL after Task 7>)
   ```

5. Publish the dashboard.

## Task 7 — Marathos Genie space

1. **Genie → New space**, name `Marathos Genie`.
2. **Tables**: link the views (Genie performs better on enriched views than raw facts):
   - `marathos.gold.vw_results_enriched`
   - `marathos.gold.vw_distance_top_events`
   - `marathos.gold.vw_distance_country_leaderboard`
   - `marathos.gold.vw_time_top_events`
   - `marathos.gold.vw_time_country_leaderboard`
   - `marathos.gold.dim_country` (for full country names)
3. **Instructions** — paste:

   > Marathos is an ultramarathon hosting company. Events are either `distance`
   > (athletes race a fixed km/mi distance and finish time is measured) or
   > `time` (athletes run for a fixed number of hours and distance covered is
   > measured). `performance_time_seconds` is filled for distance events;
   > `performance_distance_km` is filled for time events. Athlete country is
   > a 3-letter IOC code — join `dim_country.country_code` for the full name.
   > Prefer `vw_results_enriched` for general questions.

4. **Sample questions** — add (these match the validation notebook):
   - "How many race finishes are recorded per year?"
   - "Which 10 events have the most finishers all-time?"
   - "What is the fastest 100km finish in the dataset?"
   - "Average finish time per distance bucket?"
   - "Top countries by number of finishes and average pace"
   - "Gender split of finishers per year"
   - "Athlete age distribution overall"
   - "For 24h events, average distance covered, by year"

5. **Validate** by running `explorations/02_genie_answer_validation` and
   comparing each cell's table to what Genie returns for the matching
   question. If anything diverges, add column comments (Catalog → table →
   columns → edit comment) so Genie understands the semantics, e.g.:

   ```sql
   ALTER TABLE marathos.gold.fct_results
     ALTER COLUMN performance_time_seconds
     COMMENT 'Total finish time in seconds — only populated for distance (km/mi) events. NULL for time (h) events.';
   ```

6. Copy the Genie space URL into the dashboard markdown widget from step 4 above.
