# Sandbox Developer Setup

This guide is for **trusted developers** deploying an isolated copy of the UltraHuman
pipeline. Access is confined by `iam/uhsandbox-developer-policy.json` to resources named
`uhsandbox-*` — you cannot read, modify, or deploy over any production resource.

Everything you deploy uses the `sandboxConfig` in `cdk/app.py`, which prefixes every
resource with `uhsandbox-`.

- **Account:** `509812589231`
- **Region:** `us-east-1`

---

## Manual prerequisites (run once)

The CDK stack **references** the data bucket, secret, and ECR repo by name but does not
create them, so you must create these three resources yourself before your first deploy.
All three names are inside the `uhsandbox-*` namespace, so your IAM policy permits it.

### 1. Create the data bucket

```bash
aws s3api create-bucket \
  --bucket uhsandbox-uh-data \
  --region us-east-1
```

This bucket holds raw JSON, parquet datasets, generated HTML reports, and Athena query
results (`s3://uhsandbox-uh-data/athena-results/`, enforced by the `uhsandbox-primary`
workgroup that CDK creates).

### 2. Create the ECR repository

```bash
aws ecr create-repository \
  --repository-name uhsandbox-uh \
  --region us-east-1
```

`deploy.sh` builds the shared Lambda image and pushes it here (see the deploy step below,
which sets `ECR_REPOSITORY=uhsandbox-uh`).

### 3. Create the secret

The Lambda functions read all of their configuration (MDH credentials, UltraHuman API
keys, JWT signing secret, and the Athena database/workgroup/results location) from this
one secret. Mirror the key schema of the production secret, but override the three
Athena-related keys so queries stay inside your sandbox namespace:

| Key | Sandbox value |
| --- | --- |
| `UH_DATABASE` | `uhsandbox-uh` |
| `UH_WORKGROUP` | `uhsandbox-primary` |
| `UH_S3_LOCATION` | `s3://uhsandbox-uh-data/athena-results/` |

> The exact set of keys (MDH_*, UltraHuman API keys, `REPORT_SECRET`, etc.) must match
> what the code expects. Ask an admin for the production secret's **key names** (not its
> values) — you will not have read access to `prod/biobayb/*`. Use test/staging
> credentials, never production values.

```bash
aws secretsmanager create-secret \
  --name uhsandbox/keys \
  --region us-east-1 \
  --secret-string '{
    "MDH_SECRET_KEY": "<staging>",
    "MDH_ACCOUNT_NAME": "<staging>",
    "MDH_PROJECT_ID": "<staging>",
    "MDH_PROJECT_NAME": "<staging>",
    "REPORT_SECRET": "<generate-a-random-string>",
    "UH_DATABASE": "uhsandbox-uh",
    "UH_WORKGROUP": "uhsandbox-primary",
    "UH_S3_LOCATION": "s3://uhsandbox-uh-data/athena-results/"
  }'
```

---

## Deploy

Once the three prerequisites exist, deploy the sandbox stack. The region is already
CDK-bootstrapped, so you do **not** run `cdk bootstrap`.

```bash
# from the repo root
export ECR_REPOSITORY=uhsandbox-uh   # build/push the image to your repo
export CDK_CONFIG=sandbox            # select the sandbox stack (defaults to prod otherwise)
./deploy.sh --cdk
```

Or drive CDK directly:

```bash
cd cdk
cdk deploy -c config=sandbox
```

This creates the `uhsandbox-uh` CloudFormation stack: five `uhsandbox-uh_*` Lambdas, the
`uhsandbox-uh-mdh_uh_sync` SNS topic, the `uhsandbox-uh-biobayb_uh_undeliverable` DLQ, the
`uhsandbox-uh-jwt-generation` state machine, the `uhsandbox-primary` Athena workgroup, and
the `uhsandbox-uh` Glue database (created on first data upload).

---

## Notes

- **Prod is untouched.** Omitting `CDK_CONFIG`/`-c config=sandbox` deploys prod as before;
  your IAM policy denies that anyway.
- **Athena workgroup.** CDK creates `uhsandbox-primary` with an enforced results location
  under your bucket. The runtime uses it via the `UH_WORKGROUP` value in your secret — do
  not leave that key blank, or queries fall back to the shared `primary` workgroup, which
  your policy denies.
- **Teardown:** `cd cdk && cdk destroy -c config=sandbox`. The bucket, secret, and ECR repo
  created above are outside CDK, so delete them manually if you want a full cleanup.
