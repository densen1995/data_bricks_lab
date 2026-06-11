# Simulated marathon generator (run as a notebook, manually or on a schedule)
#
# Invents a fictional ultramarathon (using a small LLM call as the creative seed)
# and drops the finisher rows as a CSV under
# /Volumes/marathos/default/raw/simulated/. The Lakeflow pipeline's bronze
# streaming table reads the raw volume incrementally, so the next pipeline run
# picks up the new CSV automatically - this is the streaming source of the
# medallion. Re-run any time to push another fictional race through the pipeline.

import random
import datetime
import json
import re

SIMULATED_DIR = "/Volumes/marathos/default/raw/simulated"
dbutils.fs.mkdirs(SIMULATED_DIR)

FALLBACK_EVENT = {
    "event_name": "Northern Lights Ultra (NOR)",
    "country": "NOR",
    "distance_km": 100,
    "n_finishers": 80,
}


def _sanitize_event(raw: str) -> dict:
    """Parse an LLM response into a safe event dict - raises ValueError on bad input."""
    # Strip markdown fences / leading prose the model sometimes adds.
    text = raw.strip()
    fence = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not fence:
        raise ValueError("no JSON object found in LLM response")
    obj = json.loads(fence.group(0))

    required = {"event_name", "country", "distance_km", "n_finishers"}
    missing = required - obj.keys()
    if missing:
        raise ValueError(f"missing keys: {missing}")

    # event_name - strip CSV-breaking chars (comma, quote, newline, CR, tab) + collapse whitespace.
    name = str(obj["event_name"])
    name = re.sub(r"[,\"\r\n\t]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    if not name:
        raise ValueError("event_name empty after sanitization")

    # country - must be exactly 3 uppercase letters (IOC).
    country = str(obj["country"]).strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", country):
        raise ValueError(f"bad country code: {obj['country']!r}")

    # distance_km - int in [50, 250].
    try:
        distance_km = int(float(obj["distance_km"]))
    except (TypeError, ValueError):
        raise ValueError(f"non-numeric distance_km: {obj['distance_km']!r}")
    if not 50 <= distance_km <= 250:
        raise ValueError(f"distance_km out of range: {distance_km}")

    # n_finishers - int in [30, 300].
    try:
        n_finishers = int(float(obj["n_finishers"]))
    except (TypeError, ValueError):
        raise ValueError(f"non-numeric n_finishers: {obj['n_finishers']!r}")
    if not 30 <= n_finishers <= 300:
        raise ValueError(f"n_finishers out of range: {n_finishers}")

    # Ensure the name ends with the IOC code in parens (the spec wants this).
    if not name.endswith(f"({country})"):
        name = re.sub(r"\s*\([A-Z]{3}\)\s*$", "", name).strip()
        name = f"{name} ({country})"

    return {
        "event_name": name,
        "country": country,
        "distance_km": distance_km,
        "n_finishers": n_finishers,
    }


# 1. Ask the workspace foundation model for an interesting fictional marathon.
#    Uses Databricks Mosaic AI Model Serving; falls back to a static dict if unavailable.
LLM_PROMPT = """
Invent a fictional ultra-marathon for Marathos to host this year.
Return ONLY a JSON object with keys:
- event_name   (string, must end with the country IOC code in parens, e.g. "Aurora Trail (NOR)")
- country      (3-letter IOC code, e.g. "NOR")
- distance_km  (integer, between 50 and 250)
- n_finishers  (integer, between 30 and 300)
No commentary, no markdown fence - pure JSON.
"""

event = None
try:
    from databricks.sdk.runtime import dbutils as _du  # noqa: F401
    import requests

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.apiUrl().get()
    token = ctx.apiToken().get()

    resp = requests.post(
        f"{host}/serving-endpoints/databricks-meta-llama-3-1-70b-instruct/invocations",
        headers={"Authorization": f"Bearer {token}"},
        json={"messages": [{"role": "user", "content": LLM_PROMPT}], "max_tokens": 200},
        timeout=20,
    )
    resp.raise_for_status()
    raw_content = resp.json()["choices"][0]["message"]["content"]
    event = _sanitize_event(raw_content)
except Exception as e:
    print(f"LLM call/sanitization failed ({e}); using static fallback")
    event = dict(FALLBACK_EVENT)

print(event)


# 2. Generate synthetic finisher rows. The CSV schema matches the historical
#    TWO_CENTURIES_OF_UM_RACES.csv exactly so bronze reads both with one schema.
today = datetime.date.today()
rows = []
for i in range(event["n_finishers"]):
    finish_seconds = random.randint(7 * 3600, 30 * 3600)
    h, rem = divmod(finish_seconds, 3600)
    m, s = divmod(rem, 60)
    rows.append({
        "Year of event": today.year,
        "Event dates": today.strftime("%d.%m.%Y"),
        "Event name": event["event_name"],
        "Event distance/length": f'{event["distance_km"]}km',
        "Event number of finishers": event["n_finishers"],
        "Athlete performance": f"{h}:{m:02d}:{s:02d} h",
        "Athlete club": "",
        "Athlete country": event["country"],
        "Athlete year of birth": today.year - random.randint(20, 65),
        "Athlete gender": random.choice(["M", "W"]),
        "Athlete age category": "",
        "Athlete average speed": round(event["distance_km"] / (finish_seconds / 3600.0), 3),
        "Athlete ID": 10_000_000 + i,
    })

# Timestamp suffix keeps each run in its own subfolder so same-day, same-name
# events don't overwrite each other.
ts = datetime.datetime.now().strftime("%H%M%S")
slug = event["event_name"].split(" (")[0].replace(" ", "_").lower()
fname = f"{today.isoformat()}_{ts}_{slug}"
target = f"{SIMULATED_DIR}/{fname}"

# Bronze reads with an explicit schema and matches columns positionally, so pin
# the column order to the bronze schema (createDataFrame would sort alphabetically).
BRONZE_COLUMN_ORDER = [
    "Year of event", "Event dates", "Event name", "Event distance/length",
    "Event number of finishers", "Athlete performance", "Athlete club",
    "Athlete country", "Athlete year of birth", "Athlete gender",
    "Athlete age category", "Athlete average speed", "Athlete ID",
]

sdf = spark.createDataFrame(rows).select(*BRONZE_COLUMN_ORDER)
(
    sdf.coalesce(1)
    .write
    .option("header", True)
    .mode("overwrite")
    .csv(target)
)
print(f"wrote {target} ({len(rows)} rows)")


# 3. Sanity check - list the simulated folder. The next pipeline run ingests everything here.
display(dbutils.fs.ls(SIMULATED_DIR))
