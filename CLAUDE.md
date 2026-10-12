# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SensorFabric UltraHuman (UH) is an AWS Lambda data pipeline for the BioBayb study. It collects
UltraHuman ring data into S3/Athena and emails-out weekly health reports to participants through
MyDataHelps (MDH). Five Lambda handlers, all packaged in one shared container image:

| Handler (CDK `cmd`) | Role |
| --- | --- |
| `ultrahuman.uh_publisher.lambda_handler` | Daily: list active MDH participants, fan out one SNS message each |
| `ultrahuman.uh_uploader.lambda_handler` | SNS-triggered: pull UH API data, write raw JSON + parquet to S3 |
| `ultrahuman.uh_jwt_coordinator.lambda_handler` | Weekly: list participants, start the Step Functions fan-out |
| `ultrahuman.uh_jwt_worker.lambda_handler` | Per-participant: render report HTML → S3, mint JWT, write it back to MDH |
| `ultrahuman.templates.lambda_handler` | Public Function URL: validate JWT, serve the pre-rendered HTML from S3 |

Deployed function names are `{project_name}_{handler_key}_Lambda`
(e.g. `uh-biobayb-prod_biobayb_uh_jwt_worker_Lambda`). Every function is invoked through its
`LIVE` alias, not `$LATEST`.

## Commands

There is **no linter or CI** in this repo. There is a `unittest` suite under `tests/`
(no pytest — it is not declared in either requirements file):

```bash
python -m unittest discover -s tests          # from the repo root
python -m unittest discover -s tests -v       # verbose
python -m unittest discover -s tests -k window  # single pattern
```

`tests/` has no `__init__.py`, so `-t .` fails with "Start directory is not importable" — use
`-s tests` alone. The tests import `ultrahuman.*`, which pulls in `sensorfabric`, `pandas` and
friends, so the pinned deps must be installed. Each test file also carries a hardcoded
`sys.path.insert(0, '/home/duo/sensorfabric-uh')`, which is a no-op anywhere else and can be
ignored; running from the repo root is what makes the imports resolve.

Beyond that, "testing" means the local Flask server, the per-module `test_locally()` helpers,
and post-deploy smoke invokes.

### Local report development (the main inner loop)

Report/template work is iterated through a Flask server that calls the real Lambda code paths:

```bash
export AWS_SECRET_NAME="prod/biobayb/uh/keys"   # or uhsandbox/keys
export AWS_REGION="us-east-1"
export TEMPLATE_GENERATOR_URL="https://placeholder.lambda-url.us-east-1.on.aws/"
export TEMPLATE_MODE="PRODUCTION"               # PRESENT = fake data, see below

python ultrahuman/webserver.py --port 5000
```

- `/test?participant_id=BB-1234-5678&start_date=…&end_date=…` — renders a **real** report by
  running every `Helper` Athena query. This is how you verify query + template changes.
- `/mocktest` — renders the template with all metrics `None`, to check the empty-data layout.
- `/?t=<jwt>` — exercises `templates.lambda_handler` end to end (fetches HTML from S3).

Run from the repo root: `uh_jwt_worker` loads Jinja templates via the relative path
`ultrahuman/templates`, so the cwd must contain the `ultrahuman/` package.

### Other local entry points

```bash
export SF_DATA_BUCKET="uhsandbox-uh-data" SF_DATABASE_NAME="uhsandbox-uh" UH_ENVIRONMENT="development"

python -c "from ultrahuman.uh_publisher import test_locally; test_locally()"
python -c "from ultrahuman.uh_uploader import test_locally; test_locally(participant_id='BB-1234', email='user@example.com')"
```

### Deploy the report path (`deploy-dashboard.sh`) — the normal path

The UltraHuman ingestion half is being sunsetted; day-to-day work is report-only. Use
`deploy-dashboard.sh`, which touches only `jwt_worker`, `template_generator`, and
`jwt_coordinator` and never `publisher`/`uploader`.

```bash
# Tight loop: ship a template/query change and preview it for one participant
./deploy-dashboard.sh --profile <aws-profile> --config sandbox --worker-only \
    --regenerate BB-1234-5678

# Full report-path deploy
./deploy-dashboard.sh --profile <aws-profile> --config prod

# Re-render one participant without deploying
./deploy-dashboard.sh --profile <aws-profile> --config prod --regenerate-only BB-1234-5678
```

`--config` takes the **same selector as `cdk deploy -c config=<X>`** and resolves it to the
matching `project_name` and ECR repo, so you never have to remember which name is which:

| `--config` | `project_name` (names the Lambdas) | ECR repo |
| --- | --- | --- |
| `prod` | `uh-biobayb-prod` | `uh-biobayb` |
| `sandbox` | `uhsandbox-uh` | `uhsandbox-uh` |

`--project <name>` is the escape hatch for a config not in that table, and matches
`StackConfig.project_name` exactly. It wants the **`project_name`, not the `stack_name`** — the
script rejects `Ultrahuman-Prod` with a pointer to the right value. Exactly one target is
required; there is no default.

This project is not used with the default AWS profile — always pass `--profile`. It exports
`AWS_PROFILE` for every call including the ECR login, and overrides an inherited `AWS_PROFILE`.
`deploy.sh` has no such flag; prefix it with `AWS_PROFILE=<name>` instead.

Production targets prompt for confirmation (`--yes` to skip). `--worker-only` is the correct
scope for any `reportv2.html` / `helper.py` / `_generate_template` change. Override the ECR repo
with `ECR_REPOSITORY`.

**Regeneration is preview-by-default.** `--regenerate` / `--regenerate-only` invoke the worker
with `update_mdh=false`: the report renders and uploads to S3, and you get a working URL built
from the returned JWT, but `report_jwt` in MDH is untouched so the participant still sees their
old report. Add **`--update-mdh`** to publish — the worker then writes `report_jwt`, pointing the
participant at the new render. That prompts first (unless `--yes`) because it changes what a real
participant sees. Either way the script reports the worker's own `mdh_updated` value, so the
output states what actually happened rather than what was requested.

### Deploy everything (`deploy.sh`) — legacy / ingestion

```bash
./deploy.sh                 # build image, push to ECR, update deployed Lambdas directly
./deploy.sh --cdk           # build image, then `cdk deploy` (use this for infra changes)
./deploy.sh --build-only    # build + push to ECR only
./deploy.sh --stack uh-biobayb-prod   # restrict Lambda updates to one project_name
./deploy.sh --health-check  # describe deployed functions
./deploy.sh --test          # invoke the LIVE deployed functions with a synthetic SNS event
```

Still the only way to ship `uh_publisher` / `uh_uploader` changes, and `--cdk` is still required
for infrastructure changes.

Two things to know before running it:

- **The default (non-`--cdk`) path touches every stack.** `discover_lambda_functions()` lists all
  Lambdas in the account matching `*_biobayb_uh_*_Lambda` and repoints each at
  `$ECR_REGISTRY/$ECR_REPOSITORY:shared`, publishes a new version, and moves the `LIVE` alias.
  Pass `--stack <project_name>` to scope it, and set `ECR_REPOSITORY` to your own repo.
- **`--test` hits live functions**, including production, with a fake participant. It is currently
  commented out of the default pipeline and only runs when you pass `--test` explicitly. The
  default pipeline does end with `health_check`, which polls state + recent CloudWatch errors for
  every discovered function.

CDK directly:

```bash
cd cdk && pip install -r requirements.txt
cdk deploy --profile <aws-profile> -c config=sandbox   # omit -c to get prod
cdk diff   --profile <aws-profile> -c config=sandbox
```

`build_and_deploy.sh` is a deprecated shim that `exec`s `deploy.sh`.

## Architecture

### Two distinct Athena data sources

This is the single most important thing to internalize before touching `helper.py`. `Helper`
holds two separate `Needle` connections and the tables are **not** interchangeable:

- `self.athena_uh` — the pipeline's *own* Glue database (`UH_DATABASE` from the secret), populated
  by `uh_uploader`. Tables are UH metric names: `temp`, `hr`, `hrv`, `sleep_stages`, `steps`, …
  Partitioned by `pid`; time filtering uses integer `object_values_timestamp` (epoch **seconds**).
- `self.athena_mdh` — the MDH-managed project database, populated by MyDataHelps' own device
  integrations. Tables: `ourasleep`, `ouradailyactivity`, `omronbloodpressure`,
  `healthkitv2samples`, `projectdevicedata`. Filtering uses `participantidentifier` and date
  columns (`day`, `startdate`, `observationdate`).

In `uh_jwt_worker._generate_template`, **the two remaining UltraHuman-sourced metrics are
hardcoded to `None`** — `ringwear` and `temp`. Everything else now runs against MDH:
`movement` (`movementSummary`) and `hr` (`heartRateSummary`) were switched on in
c1dd5af/25ee250 and read Oura data. `Helper.ringWearTime` and `Helper.temperatureSummary`
still exist and still query `athena_uh`, but nothing calls them in the live path.

Every `Helper` metric method returns `None` when there is no data, and `reportv2.html` guards each
section with `{% if x is not none %}`. Adding a metric means: new `Helper` method → wire it into
the `data` dict in `uh_jwt_worker._generate_template` → add a guarded block to `reportv2.html` →
add a `_debugOutputs()` branch.

### Report generation is split across two Lambdas

Reports are rendered *offline*, not on request:

0. An EventBridge rule `{project}_UHJWTCoordinatorScheduleRule` fires `cron(0 7 ? * SUN *)` —
   Sundays 07:00 UTC, i.e. midnight Phoenix. It is defined in CDK, not added by hand. The
   payload is the stock `"Scheduled Event"` envelope with no dates, so the coordinator falls
   back to the last **7 completed days**: `end_date = today - 1` (yesterday),
   `start_date = today - 7`, both inclusive. It fires at 00:00 Phoenix, so "today" carries no
   sleep session yet; including it produced an 8-day window whose `end_date` doubled as the
   next run's `start_date`.
1. `uh_jwt_coordinator` lists enrolled participants → starts the Step Functions state machine
   with the participant array.
2. The state machine `Map` state (max concurrency 10) invokes `uh_jwt_worker` per participant.
3. `uh_jwt_worker` runs the `Helper` queries, renders `ultrahuman/templates/reportv2.html`,
   uploads to `s3://{bucket}/templates/template_{pid}_{start}_{end}_{ts}_{uuid}.html`, mints an
   HS256 JWT whose payload carries **`s3_path`**, and writes
   `report_jwt = "{TEMPLATE_GENERATOR_URL}?t={jwt}"` into the participant's MDH custom fields —
   the field holds a full clickable URL, not a bare token.
4. `templates.lambda_handler` (public Function URL, auth NONE) validates the JWT and streams the
   already-rendered S3 object back. It does no querying and no rendering.

So: **query/template bugs are fixed in `uh_jwt_worker.py` + `helper.py`, never in `templates.py`.**

**Invocation qualifiers are inconsistent, and it matters.** Only the coordinator runs through its
`LIVE` alias (the EventBridge rule targets `lambda_aliases[...]`). The Step Functions task is
given `lambda_functions["biobayb_uh_jwt_worker"]`, so it invokes the worker's **unqualified ARN
(`$LATEST`)**, and the template generator's Function URL is attached to the function rather than
the alias, so it also serves from `$LATEST`. When reproducing a scheduled run by hand, invoke the
worker unqualified — `:LIVE` is a different qualifier than production uses.

There is also a manual EventBridge rule on `source: sensorfabric.manual`, detail-type
`UltraHuman JWT Generation Request`. **It cannot target a single participant**: `PutEvents` nests
the payload under `event['detail']`, but the coordinator reads `event.get('participant_id')` at
the top level, so that route always runs the full cohort with default dates. Use a direct
`aws lambda invoke` to scope to one person.

### Data collection flow

`uh_publisher` (daily 07:00 UTC) → SNS `{stack_name}-{sns_topic_name}` → `uh_uploader`.

- Publisher selects participants where `enrolled` **and** `customFields.uh_enabled == "yes"`.
  The coordinator, by contrast, only filters on `enrolled`.
- Uploader walks day-by-day from `uh_sync_date` (falling back to `uh_start_date`) to `target_date`,
  capped at `MAX_SYNC_DAYS = 45`.
- **Temperature is the anchor.** A day with no `temp` values is skipped entirely, and
  `last_uh_timestamp` is advanced to temp's last timestamp so all metrics stay synchronized.
  `uh_sync_date`/`uh_sync_epoch` are written back to MDH only when records were written.
- Only `WHITELISTED_TABLES` (`uh_uploader.py:21`) are persisted. The `Sleep` metric is special-cased:
  its nested sub-objects are promoted to their own tables, with `bedtime_start`/`bedtime_end`
  copied onto each.
- Timezone priority is the device's `latest_time_zone` from the UH API, not the MDH `timeZone`.

S3 layout:

```
raw/json/pid={pid}/date={YYYY-MM-DD}/data.json   # verbatim API response
raw/dataset/{metric_type}/pid={pid}/*.parquet    # flattened, Glue-registered
templates/template_{pid}_{start}_{end}_*.html    # rendered reports
athena-results/                                  # Athena output (sandbox workgroup enforces this)
```

### Configuration lives in Secrets Manager, not only in env vars

Env vars (set by CDK) cover wiring — bucket, database, topic ARN, state machine ARN. The *runtime*
config comes from a single Secrets Manager JSON blob named by `AWS_SECRET_NAME`, fetched by the
near-identical `get_secret()` copied into each handler module. Keys include `MDH_SECRET_KEY`,
`MDH_ACCOUNT_NAME`, `MDH_PROJECT_ID`, `MDH_PROJECT_NAME`, `UH_API_KEY`, `UH_BASE_URL`,
`REPORT_SECRET` (JWT signing), and the Athena triple `UH_DATABASE` / `UH_WORKGROUP` /
`UH_S3_LOCATION`. Note the Athena target is a *secret* value — changing which database reports
read from is a secret edit, not a CDK change.

### Stacks

`cdk/app.py` defines exactly two live configs; the old `dev` stack is commented out.

- **prod** (default): stack `Ultrahuman-Prod`, project `uh-biobayb-prod`, db `uh-biobayb-prod2`,
  bucket `uoa-biobayb-uh-prod2`, secret `prod/biobayb/uh-prod/prod-keys`, `TEMPLATE_MODE=PRODUCTION`.
- **sandbox**: everything prefixed `uhsandbox-`, `TEMPLATE_MODE=PRESENT`,
  `UH_ENVIRONMENT=development`, plus a dedicated Athena workgroup. Selected with
  `cdk deploy -c config=sandbox` or `CDK_CONFIG=sandbox ./deploy.sh --cdk`. See `dev-setup.md`
  for the three resources you must create by hand first (bucket, ECR repo, secret) and
  `iam/uhsandbox-developer-policy.json` for the allow-list that confines sandbox developers.

Account `509812589231`, region `us-east-1` for both.

## Gotchas

- **`TEMPLATE_MODE=PRESENT` fabricates data.** Nearly every `Helper` method short-circuits to
  `Helper._debugOutputs()`, which returns hardcoded/`random` values by inspecting the caller's
  frame name. The sandbox stack ships with `PRESENT`, so sandbox reports are *not* real. Set
  `TEMPLATE_MODE=PRODUCTION` when validating queries.
- **`UH_ENVIRONMENT=development` ignores the participant's email.** `UltrahumanAPI.get_metrics`
  silently substitutes `DEVELOPMENT_EMAIL` (`uh.py:11`) and hits the staging host, so every
  participant gets the same demo person's data.
- **Unwired legacy modules.** `ultrahuman/shtemplate.py` (superseded by `templates.py`) and
  `ultrahuman/uh_jwt_generator.py` (the monolithic, pre-Step-Functions version of the worker) are
  not referenced by any CDK handler. `uh_jwt_generator.py` is nonetheless kept roughly in sync by
  hand — recent report commits edit it alongside `uh_jwt_worker.py`. Decide deliberately whether to
  mirror a change there. `templates/report.html` is likewise unused; only `reportv2.html` renders.
- **Deploying new code does not change what participants see.** Reports are pre-rendered into
  S3, so every existing `templates/*.html` stays stale until generation runs again. A report fix
  isn't live until you regenerate.
- **Expect image drift between the two deploy scripts.** All five functions share the
  `:shared` tag, but Lambda pins the image *digest* at `update-function-code` time. Since
  `deploy-dashboard.sh` only updates the three report functions, `publisher`/`uploader` keep
  running an older digest and will pick up accumulated changes whenever someone next runs
  `deploy.sh`. That is intentional while the UH half is being retired.
- **`deploy-dashboard.sh` ships code only; env vars need `cdk deploy`.** It calls
  `update-function-code` / `publish-version` / `update-alias` and nothing else, so anything
  CloudFormation owns — environment variables, memory, timeouts, the EventBridge schedule,
  Step Functions, IAM, the Function URL — is unchanged by it. The trap: when CDK gains a new
  env var that the code then requires, shipping that code with the script alone leaves the
  function without the variable. `MDH_ACTIVE_SEGMENT_ID` is the live example — the coordinator
  fails closed without it and every scheduled run sends zero reports. Run
  `cdk deploy -c config=<X>` first. The script now pre-flights this specific variable before
  updating the coordinator; add any future equivalents to `COORDINATOR_REQUIRED_ENV`.
  Note the coordinator also reads the value off the Secrets Manager payload, which is a way to
  set it without a CDK deploy.
- **All `Helper` SQL is f-string interpolated** with `participant_id` and dates. Preserve the
  existing shape when editing rather than introducing a new interpolation style.
- Container base image is `public.ecr.aws/lambda/python:3.13`; `requirements.txt` is pinned and is
  what lands in the image. `requirements-dev.txt` additionally carries Flask and `aws-cdk-lib`.
- Error classification is centralized in `ultrahuman/error_handling.py`: `handle_api_error()` either
  raises `RetryableError` (5xx, 429, connection errors → Lambda/SNS retry, 2 attempts) or ships the
  context to the SQS DLQ and returns. Unknown error types default to non-retryable.

## MDH participant custom fields

- `uh_enabled` — `"yes"` gates inclusion in daily collection
- `uh_email` — UltraHuman account email (falls back to demographics email, then account email)
- `uh_start_date`, `uh_sync_date`, `uh_sync_epoch` — backfill start and sync watermarks
- `report_jwt` — full report URL incl. JWT, written by `uh_jwt_worker`
- `timeZone` — fallback only; device timezone wins
- `ga_calculated_today_days` — gestational age, read by `Helper.weeksPregnant()`

## Manual triggers

```bash
# Collect for one participant / date
aws lambda invoke --function-name {project}_biobayb_uh_publisher_Lambda \
  --payload '{"participant_id": "BB-1234-5678", "target_date": "2024-01-15"}' /tmp/out.json

# Regenerate one report (omit participant_id for everyone; dates default to the last 7 days)
aws lambda invoke --function-name {project}_biobayb_uh_jwt_coordinator_Lambda \
  --payload '{"start_date": "2024-01-01", "end_date": "2024-01-08", "participant_id": "BB-1234-5678"}' /tmp/out.json

aws logs tail /aws/lambda/{project}_biobayb_uh_jwt_worker_Lambda --follow
```
