#!/bin/bash

# Deployment script for the weekly report ("dashboard") path only.
#
# Covers the three report-path Lambdas:
#   - biobayb_uh_jwt_worker          renders reportv2.html, uploads to S3, mints the JWT
#   - biobayb_uh_template_generator  validates the JWT, serves the HTML from S3
#   - biobayb_uh_jwt_coordinator     lists participants, starts the Step Functions fan-out
#
# It deliberately does NOT touch biobayb_uh_publisher or biobayb_uh_uploader (the UltraHuman
# ingestion half, being sunsetted). Those keep running whatever image digest they were last
# deployed with; pushing a new :shared tag here does not move them. Use ./deploy.sh for those.
#
# Infrastructure (Step Functions, S3, Secrets Manager, Athena, IAM, Function URL) is CDK-owned
# and is never touched here. For infra changes run: cd cdk && cdk deploy -c config=<name>

# Not using `set -e`: `aws lambda wait` intermittently returns non-zero even when the update
# succeeded, same rationale as deploy.sh.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_REGION="${AWS_REGION:-us-east-1}"
ECR_REGISTRY="${ECR_REGISTRY:-509812589231.dkr.ecr.us-east-1.amazonaws.com}"
DOCKER_DIR="$SCRIPT_DIR/docker"
BUILD_DIR="$SCRIPT_DIR/build"
IMAGE_TAG="shared"
ALIAS_NAME="LIVE"

# Report-path function suffixes, in deploy order (worker first: it is the one that matters
# for template and query changes).
ALL_FUNCS="biobayb_uh_jwt_worker biobayb_uh_template_generator biobayb_uh_jwt_coordinator"
WORKER_FUNC="biobayb_uh_jwt_worker"
GENERATOR_FUNC="biobayb_uh_template_generator"
COORDINATOR_FUNC="biobayb_uh_jwt_coordinator"

# Env vars the deployed function must already carry for the new code to work.
# This script only ships code; env vars are CloudFormation-owned and arrive via
# `cdk deploy`. Anything listed here is checked before the function is updated.
COORDINATOR_REQUIRED_ENV="MDH_ACTIVE_SEGMENT_ID"

# Options
# PROJECT holds the CDK `project_name`, NOT `stack_name`. Lambda functions are named
# {project_name}_{handler}_Lambda (sensorfabric_lambda_stack.py:324), so this is the value
# every lookup in this script is built from. For prod the two differ:
#   stack_name   = Ultrahuman-Prod   (the CloudFormation stack)
#   project_name = uh-biobayb-prod   (the resource prefix)  <-- what we need
PROJECT=""
ACTION="deploy"
TARGET_FUNCS="$ALL_FUNCS"
REGEN_PID=""
START_DATE=""
END_DATE=""
PROFILE=""
UPDATE_MDH=false
ASSUME_YES=false
DEBUG="${DEBUG:-false}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warning() { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_header()  { echo -e "\n${BLUE}=== $* ===${NC}"; }
log_debug()   { [ "$DEBUG" = "true" ] && echo -e "[DEBUG] $*" || true; }

usage() {
    cat << EOF
Usage: $0 (--config prod|sandbox | --project <project_name>) [OPTIONS]

Deploys only the weekly report path. Does not touch the UltraHuman publisher/uploader.

Target (exactly one, no default on purpose):
  --config prod|sandbox   The same selector you pass to 'cdk deploy -c config=<X>'.
                          Resolves to the matching project_name and ECR repo:
                            prod     -> project uh-biobayb-prod, ecr uh-biobayb
                            sandbox  -> project uhsandbox-uh,     ecr uhsandbox-uh
  --project NAME          Explicit project_name, for a config not in the list above.

  NOTE: --project is the CDK StackConfig.project_name, NOT stack_name. They differ for
  prod (stack_name 'Ultrahuman-Prod' vs project_name 'uh-biobayb-prod'); Lambdas are
  named from project_name.

Options:
  --worker-only           Update only biobayb_uh_jwt_worker. This is the correct scope for
                          any reportv2.html / helper.py / _generate_template change, since
                          templates.py contains no Jinja and no Helper.
  --build-only            Build and push the image, do not update any function.
  --deploy-only           Skip the build, update functions from the existing :shared image.
  --regenerate PID        After deploying, render a report for one participant by invoking
                          the worker directly. Prints the S3 path and a ready-to-open URL.
                          Read-only by default (update_mdh=false): MDH is NOT written, so
                          the participant keeps seeing their existing report.
  --regenerate-only PID   Skip build and deploy, just regenerate for one participant.
  --update-mdh            Make a regeneration publish: passes update_mdh=true so the worker
                          writes report_jwt on the participant, pointing them at this new
                          render. Prompts first unless --yes. Requires --regenerate or
                          --regenerate-only.
  --start-date YYYY-MM-DD Report window start for --regenerate (default: 7 days ago).
  --end-date   YYYY-MM-DD Report window end for --regenerate (default: yesterday).
                          The defaults mirror the coordinator's window: the last 7
                          COMPLETED days, both ends inclusive.
  --profile NAME          AWS named profile to use for every call, including the ECR login.
                          Overrides AWS_PROFILE if both are set.
  --yes                   Skip the confirmation prompt on production stacks.
  --debug                 Verbose logging.
  --help, -h              This message.

Environment:
  ECR_REPOSITORY          Override the ECR repo (otherwise derived from the target).
  AWS_REGION              Default: us-east-1
  AWS_PROFILE             Used when --profile is not passed.

Examples:
  # Tight loop: ship a template change to sandbox and preview it for one participant
  $0 --profile my-sandbox --config sandbox --worker-only --regenerate BB-1234-5678

  # Full report-path deploy to production
  $0 --profile my-prod --config prod

  # Re-render an existing participant without deploying anything (preview only)
  $0 --profile my-prod --config prod --regenerate-only BB-1234-5678

  # Same, but publish it: update report_jwt in MDH so the participant sees this render
  $0 --profile my-prod --config prod --regenerate-only BB-1234-5678 --update-mdh

Note: deploying new code does NOT change what participants see. Reports are pre-rendered into
S3, so existing reports stay stale until generation runs again. Use --regenerate for a single
participant, or invoke the jwt_coordinator to re-render everyone.
EOF
}

# --- argument parsing -------------------------------------------------------

# Guard against a value-taking flag being passed with no value. Without this a bare
# `--project` at the end of the line makes `shift 2` fail, leaving $1 unchanged and
# spinning the loop forever.
require_value() {
    case "${2:-}" in
        ""|--*) log_error "$1 requires a value"; exit 1 ;;
    esac
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --config)           require_value "$1" "${2:-}"; CONFIG="$2"; shift 2 ;;
        --project)          require_value "$1" "${2:-}"; PROJECT="$2"; shift 2 ;;
        --worker-only)      TARGET_FUNCS="$WORKER_FUNC"; shift ;;
        --build-only)       ACTION="build-only"; shift ;;
        --deploy-only)      ACTION="deploy-only"; shift ;;
        --regenerate)       require_value "$1" "${2:-}"; REGEN_PID="$2"; shift 2 ;;
        --regenerate-only)  require_value "$1" "${2:-}"; ACTION="regenerate-only"; REGEN_PID="$2"; shift 2 ;;
        --start-date)       require_value "$1" "${2:-}"; START_DATE="$2"; shift 2 ;;
        --end-date)         require_value "$1" "${2:-}"; END_DATE="$2"; shift 2 ;;
        --profile)          require_value "$1" "${2:-}"; PROFILE="$2"; shift 2 ;;
        --update-mdh)       UPDATE_MDH=true; shift ;;
        --yes|-y)           ASSUME_YES=true; shift ;;
        --debug)            DEBUG=true; shift ;;
        --help|-h)          usage; exit 0 ;;
        *) log_error "Unknown option: $1"; echo; usage; exit 1 ;;
    esac
done

# Exported rather than threaded through each call: every `aws` invocation below, plus
# `aws ecr get-login-password` feeding `docker login`, picks this up automatically.
# An explicit --profile wins over an AWS_PROFILE already in the environment.
if [ -n "$PROFILE" ]; then
    export AWS_PROFILE="$PROFILE"
fi

# Mirror of the `configs` dict in cdk/app.py. --config is the ergonomic front door; it maps
# the same selector you pass to `cdk deploy -c config=<X>` onto the project_name and ECR repo
# that go with it, so you never have to remember which name is which.
resolve_config() {
    case "$1" in
        prod)    PROJECT="uh-biobayb-prod"; CONFIG_ECR="uh-biobayb" ;;
        sandbox) PROJECT="uhsandbox-uh";    CONFIG_ECR="uhsandbox-uh" ;;
        *)
            log_error "Unknown --config '$1'. Known values: prod, sandbox"
            log_error "For a config not listed here, pass --project <project_name> instead."
            exit 1
            ;;
    esac
}

if [ -n "${CONFIG:-}" ] && [ -n "$PROJECT" ]; then
    log_error "Pass either --config or --project, not both."
    exit 1
fi

if [ -n "${CONFIG:-}" ]; then
    resolve_config "$CONFIG"
fi

if [ -z "$PROJECT" ]; then
    log_error "A target is required. Refusing to guess."
    log_error "  --config prod|sandbox      (same selector as 'cdk deploy -c config=<X>')"
    log_error "  --project <project_name>   (explicit, e.g. uh-biobayb-prod)"
    echo
    usage
    exit 1
fi

# Catch the most likely mistake: passing a CloudFormation stack_name where a project_name
# belongs. These two differ for prod, and the resulting "function not found" is cryptic.
case "$PROJECT" in
    Ultrahuman-Prod)
        log_error "'Ultrahuman-Prod' is the CloudFormation stack_name, not the project_name."
        log_error "Lambdas are named from project_name. You want:"
        log_error "  --config prod            (or)  --project uh-biobayb-prod"
        exit 1
        ;;
esac

if [ "$ACTION" = "regenerate-only" ] && [ -z "$REGEN_PID" ]; then
    log_error "--regenerate-only needs a participant id"
    exit 1
fi

if [ "$UPDATE_MDH" = true ] && [ -z "$REGEN_PID" ]; then
    log_error "--update-mdh only applies to a regeneration."
    log_error "Add --regenerate <PID> or --regenerate-only <PID>."
    exit 1
fi

# --- helpers ----------------------------------------------------------------

resolve_ecr_repository() {
    # Precedence: explicit env var > value implied by --config > inferred from --project.
    if [ -n "${ECR_REPOSITORY:-}" ]; then
        echo "$ECR_REPOSITORY"
        return 0
    fi
    if [ -n "${CONFIG_ECR:-}" ]; then
        echo "$CONFIG_ECR"
        return 0
    fi
    case "$PROJECT" in
        uhsandbox-*)     echo "uhsandbox-uh" ;;
        uh-biobayb-prod) echo "uh-biobayb" ;;
        *)
            log_error "Cannot derive an ECR repository for project '$PROJECT'."
            log_error "Set ECR_REPOSITORY explicitly, e.g. ECR_REPOSITORY=my-repo $0 --project $PROJECT"
            return 1
            ;;
    esac
}

# Mirrors sensorfabric_lambda_stack.py:324 -> {project_name}_{handler}_Lambda
func_name() { echo "${PROJECT}_${1}_Lambda"; }

days_ago() {
    # GNU and BSD date disagree on relative-date syntax.
    if date -v-"$1"d +%Y-%m-%d >/dev/null 2>&1; then
        date -v-"$1"d +%Y-%m-%d
    else
        date -d "$1 days ago" +%Y-%m-%d
    fi
}

validate_prerequisites() {
    log_header "Validating prerequisites"

    for cmd in aws docker python3; do
        if ! command -v "$cmd" &> /dev/null; then
            log_error "$cmd is not installed"
            exit 1
        fi
    done

    local account
    if ! account=$(aws sts get-caller-identity \
        --region "$AWS_REGION" --query Account --output text 2>/dev/null) \
        || [ -z "$account" ]; then
        if [ -n "${AWS_PROFILE:-}" ]; then
            log_error "Could not authenticate with AWS profile '${AWS_PROFILE}'."
            log_error "Check it exists (aws configure list-profiles) and its credentials are current."
        else
            log_error "AWS credentials are not configured or have expired."
            log_error "If you use a named profile, pass --profile <name>."
        fi
        exit 1
    fi

    log_info "Profile: ${AWS_PROFILE:-<default>}"
    log_info "Account: $account"
    log_info "Region:  $AWS_REGION"
    log_info "Project: $PROJECT"
}

verify_functions_exist() {
    log_header "Verifying target functions exist"

    local missing=0
    for suffix in $TARGET_FUNCS; do
        local fn
        fn="$(func_name "$suffix")"
        if aws lambda get-function-configuration \
            --function-name "$fn" --region "$AWS_REGION" >/dev/null 2>&1; then
            log_info "found $fn"
        else
            log_error "not found: $fn"
            missing=$((missing + 1))
        fi
    done

    if [ "$missing" -gt 0 ]; then
        log_error "$missing function(s) missing. Check --config/--project, or deploy the stack first:"
        log_error "  cd cdk && cdk deploy -c config=<prod|sandbox>"
        exit 1
    fi
}

check_coordinator_env() {
    # The coordinator reads MDH_ACTIVE_SEGMENT_ID to pick the weekly report audience
    # and fails closed without it, so a code-only deploy can silently stop every
    # Sunday run. This script cannot set the variable: `cdk deploy` owns it.
    case " $TARGET_FUNCS " in
        *" $COORDINATOR_FUNC "*) ;;
        *) return 0 ;;
    esac

    local fn value
    fn="$(func_name "$COORDINATOR_FUNC")"

    for key in $COORDINATOR_REQUIRED_ENV; do
        value=$(aws lambda get-function-configuration \
            --function-name "$fn" --region "$AWS_REGION" \
            --query "Environment.Variables.$key" --output text 2>/dev/null)

        if [ -n "$value" ] && [ "$value" != "None" ]; then
            log_info "$key is set on the coordinator"
            continue
        fi

        echo
        log_warning "$key is NOT set on $fn."
        log_warning "The coordinator selects the weekly report audience from that MDH"
        log_warning "segment and fails closed without it: every scheduled run raises and"
        log_warning "NO reports go out. This script only ships code, so it cannot set it."
        log_warning "Fix with:  cd cdk && cdk deploy -c config=<prod|sandbox>"
        log_warning "or add $key to the Secrets Manager payload, which the"
        log_warning "coordinator also reads."

        case "$PROJECT" in
            *prod*)
                if [ "$ASSUME_YES" != true ]; then
                    printf "Continue updating the coordinator anyway? [y/N] "
                    read -r reply
                    case "$reply" in
                        [yY]|[yY][eE][sS]) ;;
                        *) log_info "Aborted."; exit 0 ;;
                    esac
                fi
                ;;
            *)
                log_warning "(expected on sandbox, which ships no segment id)"
                ;;
        esac
    done
}

confirm_production() {
    if [ "$ASSUME_YES" = true ]; then
        return 0
    fi
    case "$PROJECT" in
        *prod*)
            echo
            log_warning "Target project '$PROJECT' looks like production."
            log_warning "Functions to be updated:"
            for suffix in $TARGET_FUNCS; do
                log_warning "  $(func_name "$suffix")"
            done
            printf "Continue? [y/N] "
            read -r reply
            case "$reply" in
                [yY]|[yY][eE][sS]) ;;
                *) log_info "Aborted."; exit 0 ;;
            esac
            ;;
    esac
}

# --- build ------------------------------------------------------------------

build_and_push_image() {
    local repo image
    repo="$(resolve_ecr_repository)" || exit 1
    image="$ECR_REGISTRY/$repo:$IMAGE_TAG"

    log_header "Building image -> $image"

    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi

    log_info "Authenticating to ECR"
    if ! aws ecr get-login-password --region "$AWS_REGION" \
        | docker login --username AWS --password-stdin "$ECR_REGISTRY" >/dev/null 2>&1; then
        log_error "ECR login failed for $ECR_REGISTRY"
        exit 1
    fi

    # Build context mirrors deploy.sh so the Docker layer cache is shared between the two
    # scripts. requirements.txt is copied before ultrahuman/, so a template-only change
    # reuses the cached pip layer and only the final COPY is rebuilt.
    local ctx="$BUILD_DIR/shared"
    rm -rf "$ctx"
    mkdir -p "$ctx"
    cp "$SCRIPT_DIR/requirements.txt" "$ctx/"
    # __pycache__ is excluded deliberately: running the local Flask server generates it, and
    # letting it into the context changes the COPY layer's content hash on every build, which
    # would defeat the layer cache this fast path depends on.
    if command -v rsync &> /dev/null; then
        rsync -a --exclude '__pycache__' --exclude '*.pyc' \
            "$SCRIPT_DIR/ultrahuman/" "$ctx/ultrahuman/"
    else
        mkdir -p "$ctx/ultrahuman"
        cp -r "$SCRIPT_DIR"/ultrahuman/* "$ctx/ultrahuman/"
        find "$ctx/ultrahuman" -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null
        find "$ctx/ultrahuman" -name '*.pyc' -delete 2>/dev/null
    fi
    cp "$DOCKER_DIR/Dockerfile.shared" "$ctx/Dockerfile"

    if ! docker buildx build --platform linux/amd64 --provenance=false \
        -t "$image" -f "$ctx/Dockerfile" "$ctx"; then
        log_error "Image build failed"
        exit 1
    fi
    log_info "Built $image"

    log_info "Pushing $image"
    if ! docker push "$image"; then
        log_error "Image push failed"
        exit 1
    fi
    log_info "Pushed $image"

    rm -rf "$ctx"
}

# --- deploy -----------------------------------------------------------------

update_functions() {
    local repo image
    repo="$(resolve_ecr_repository)" || exit 1
    image="$ECR_REGISTRY/$repo:$IMAGE_TAG"

    log_header "Updating report-path functions"
    log_info "Image: $image"

    local failed=0
    for suffix in $TARGET_FUNCS; do
        local fn
        fn="$(func_name "$suffix")"

        log_info "Updating $fn"
        if ! aws lambda update-function-code \
            --function-name "$fn" \
            --image-uri "$image" \
            --region "$AWS_REGION" \
            --no-cli-pager --output json > /dev/null; then
            log_error "update-function-code failed for $fn"
            failed=$((failed + 1))
            continue
        fi

        if ! timeout 300 aws lambda wait function-updated \
            --function-name "$fn" --region "$AWS_REGION"; then
            log_warning "wait timed out for $fn; the update most likely still succeeded"
        fi

        # Functions are invoked through the LIVE alias, so updating $LATEST alone changes
        # nothing. Publish a version and move the alias.
        local version
        version=$(aws lambda publish-version \
            --function-name "$fn" --region "$AWS_REGION" \
            --query 'Version' --output text 2>/dev/null)

        if [ -z "$version" ] || [ "$version" = "None" ]; then
            log_error "publish-version failed for $fn; $ALIAS_NAME still points at the old code"
            failed=$((failed + 1))
            continue
        fi

        if aws lambda update-alias \
            --function-name "$fn" --name "$ALIAS_NAME" \
            --function-version "$version" --region "$AWS_REGION" \
            --no-cli-pager --output json > /dev/null; then
            log_info "$fn -> version $version ($ALIAS_NAME)"
        else
            log_error "Failed to move $ALIAS_NAME for $fn to version $version"
            failed=$((failed + 1))
        fi
    done

    if [ "$failed" -gt 0 ]; then
        log_error "$failed function update(s) failed"
        exit 1
    fi
    log_info "All targeted functions updated"
}

health_check() {
    log_header "Health check"

    local unhealthy=0
    for suffix in $TARGET_FUNCS; do
        local fn state
        fn="$(func_name "$suffix")"
        state=$(aws lambda get-function-configuration \
            --function-name "$fn" --region "$AWS_REGION" \
            --query 'State' --output text 2>/dev/null)

        if [ "$state" = "Active" ]; then
            log_info "$fn is Active"
        else
            log_error "$fn is $state"
            unhealthy=$((unhealthy + 1))
        fi
    done

    [ "$unhealthy" -eq 0 ] || exit 1
}

# --- regenerate -------------------------------------------------------------

regenerate_one() {
    local pid="$1"
    # Mirrors the coordinator's auto-generated window (uh_jwt_coordinator.py): the
    # report covers the last 7 COMPLETED calendar days, so end is yesterday, not today.
    # Keep these in step with that function or --regenerate stops reproducing the
    # Sunday run.
    local start="${START_DATE:-$(days_ago 7)}"
    local end="${END_DATE:-$(days_ago 1)}"
    local fn
    fn="$(func_name "$WORKER_FUNC")"

    log_header "Regenerating report for $pid"
    log_info "Window: $start -> $end"

    if [ "$UPDATE_MDH" = true ]; then
        log_warning "update_mdh=true -- this WRITES to the participant's MDH record."
        log_warning "report_jwt on $pid will be replaced with a URL for this new render,"
        log_warning "changing the report they see. Their previous link keeps working until"
        log_warning "its own JWT expires, but it will show the older report."
        if [ "$ASSUME_YES" != true ]; then
            printf "Update MDH for %s? [y/N] " "$pid"
            read -r reply
            case "$reply" in
                [yY]|[yY][eE][sS]) ;;
                *) log_info "Aborted; nothing was regenerated."; return 1 ;;
            esac
        fi
    else
        log_info "update_mdh=false (report_jwt in MDH is left untouched)"
    fi

    local payload response_file cli_out
    payload=$(printf '{"participant_id":"%s","start_date":"%s","end_date":"%s","update_mdh":%s}' \
        "$pid" "$start" "$end" "$UPDATE_MDH")
    response_file="$(mktemp)"

    # Invoked unqualified ($LATEST), deliberately. The Step Functions task targets the
    # worker's unqualified ARN (sensorfabric_lambda_stack.py: LambdaInvoke is given
    # lambda_functions[...], not lambda_aliases[...]), so $LATEST is what the Sunday run
    # actually executes. Invoking :LIVE here would preview a different qualifier.
    cli_out=$(aws lambda invoke \
        --function-name "${fn}" \
        --payload "$payload" \
        --cli-binary-format raw-in-base64-out \
        --region "$AWS_REGION" \
        --no-cli-pager --output json \
        "$response_file" 2>&1)

    if [ $? -ne 0 ]; then
        log_error "Invoke failed: $cli_out"
        rm -f "$response_file"
        return 1
    fi

    if echo "$cli_out" | grep -q '"FunctionError"'; then
        log_error "Worker raised an error:"
        cat "$response_file"
        echo
        rm -f "$response_file"
        return 1
    fi

    # The worker returns {"statusCode":…, "body":"<json string>"}.
    local parsed
    parsed=$(python3 - "$response_file" << 'PY'
import json, sys

with open(sys.argv[1]) as fh:
    raw = fh.read()

try:
    envelope = json.loads(raw)
except json.JSONDecodeError:
    print("PARSE_ERROR", raw[:500])
    sys.exit(0)

body = envelope.get("body")
if isinstance(body, str):
    try:
        body = json.loads(body)
    except json.JSONDecodeError:
        body = {}
body = body or {}

if not body.get("success"):
    print("FAILED", body.get("error", "unknown error"))
    sys.exit(0)

print("OK")
print(body.get("s3_path", ""))
print(body.get("jwt_token", ""))
# Reported by the worker itself, so this reflects what actually happened rather than
# what we asked for.
print("yes" if body.get("mdh_updated") else "no")
PY
)
    rm -f "$response_file"

    local status
    status=$(echo "$parsed" | sed -n '1p' | cut -d' ' -f1)

    if [ "$status" != "OK" ]; then
        log_error "Report generation failed: $(echo "$parsed" | cut -d' ' -f2-)"
        return 1
    fi

    local s3_path token mdh_done
    s3_path=$(echo "$parsed" | sed -n '2p')
    token=$(echo "$parsed" | sed -n '3p')
    mdh_done=$(echo "$parsed" | sed -n '4p')

    log_info "Rendered: $s3_path"

    if [ "$mdh_done" = "yes" ]; then
        log_info "MDH updated: report_jwt on $pid now points at this render."
    else
        log_info "MDH untouched: report_jwt on $pid still points at the previous render."
    fi

    # Catch a silent divergence between what we asked for and what the worker reports.
    if [ "$UPDATE_MDH" = true ] && [ "$mdh_done" != "yes" ]; then
        log_warning "Asked for --update-mdh but the worker reported mdh_updated=false."
        log_warning "Check the worker logs; the render succeeded but MDH may not have."
    fi

    # The JWT carries participant_id + s3_path and is signed with REPORT_SECRET, so it is
    # directly usable against the template generator's Function URL.
    local url
    url=$(aws lambda get-function-url-config \
        --function-name "$(func_name "$GENERATOR_FUNC")" \
        --region "$AWS_REGION" \
        --query 'FunctionUrl' --output text 2>/dev/null)

    if [ -n "$url" ] && [ "$url" != "None" ] && [ -n "$token" ]; then
        echo
        log_info "Preview URL:"
        echo "${url%/}/?t=${token}"
        echo
    else
        log_warning "Could not build a preview URL (no Function URL on the template generator)"
    fi
}

# --- main -------------------------------------------------------------------

validate_prerequisites

regenerated=false

case "$ACTION" in
    build-only)
        build_and_push_image
        log_info "Build complete. No functions were updated."
        log_warning "The :shared tag has moved but nothing is running it yet."
        ;;
    deploy-only)
        verify_functions_exist
        check_coordinator_env
        confirm_production
        update_functions
        health_check
        ;;
    regenerate-only)
        verify_functions_exist
        ;;
    deploy)
        verify_functions_exist
        check_coordinator_env
        confirm_production
        build_and_push_image
        update_functions
        health_check
        ;;
esac

if [ -n "$REGEN_PID" ]; then
    if ! regenerate_one "$REGEN_PID"; then
        log_error "Report regeneration failed for $REGEN_PID"
        exit 1
    fi
    regenerated=true
fi

log_header "Done"
if [ "$ACTION" != "build-only" ] && [ "$regenerated" = false ]; then
    log_warning "Reports already in S3 are still the previous render."
    log_warning "Re-render one with: $0 --project $PROJECT --regenerate-only <PID>"
    log_warning "Re-render everyone by invoking $(func_name biobayb_uh_jwt_coordinator)"
fi
exit 0
