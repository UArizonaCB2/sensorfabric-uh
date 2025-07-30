#!/bin/bash

# Comprehensive deployment automation script for SensorFabric Lambda functions
# This script provides a complete deployment pipeline with validation and rollback capabilities

# set -e  # Exit on any error. this will break the script because of how aws responds waiting for lambda updates sometimes.

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_NAME="sensorfabric-uh"
ECR_REGISTRY="509812589231.dkr.ecr.us-east-1.amazonaws.com"
ECR_REPOSITORY="uh-biobayb"
AWS_REGION="us-east-1"
CDK_DIR="$SCRIPT_DIR/cdk"
DOCKER_DIR="docker"
BUILD_DIR="build"

# Lambda function mappings - will be populated dynamically
declare -A LAMBDA_FUNCTIONS

# Stack filter for operations (empty means all stacks)
STACK_FILTER=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Discover deployed Lambda functions
discover_lambda_functions() {
    log_header "Discovering deployed Lambda functions..."
    
    # Clear existing mappings
    unset LAMBDA_FUNCTIONS
    declare -gA LAMBDA_FUNCTIONS
    
    # Get all Lambda functions that match our naming pattern
    log_debug "Querying AWS Lambda for functions matching our pattern..."
    local functions_json=$(aws lambda list-functions \
        --region "$AWS_REGION" \
        --query 'Functions[?contains(FunctionName, `_biobayb_uh_uploader_Lambda`) || contains(FunctionName, `_biobayb_uh_publisher_Lambda`)].FunctionName' \
        --output json 2>/dev/null || echo "[]")
    
    log_debug "Raw AWS response: $functions_json"
    
    # Convert JSON array to space-separated list
    local functions=""
    if command -v jq &> /dev/null; then
        functions=$(echo "$functions_json" | jq -r '.[]' 2>/dev/null | tr '\n' ' ')
    else
        # Fallback without jq - extract function names from JSON manually
        functions=$(echo "$functions_json" | grep -o '"[^"]*biobayb_uh_[^"]*"' | sed 's/"//g' | tr '\n' ' ')
    fi
    
    log_debug "Processed functions list: '$functions'"
    
    if [ -z "$functions" ] || [ "$functions" = " " ]; then
        log_warning "No Lambda functions found matching our pattern"
        # Fallback to legacy naming for backward compatibility
        LAMBDA_FUNCTIONS["uh_uploader"]="biobayb_uh_uploader"
        LAMBDA_FUNCTIONS["uh_publisher"]="biobayb_uh_sns_publisher"
        log_info "Using legacy function names as fallback"
        return
    fi
    
    # Process discovered functions
    log_debug "Starting to process functions..."
    local function_count=0
    for func in $functions; do
        # Skip empty strings
        if [ -z "$func" ]; then
            continue
        fi
        
        function_count=$((function_count + 1))
        log_debug "Processing function #$function_count: '$func'"
        
        if [[ "$func" == *"_biobayb_uh_uploader_Lambda" ]]; then
            # Extract project name from function name (format: {project_name}_biobayb_uh_uploader_Lambda)
            local project_name="${func%_biobayb_uh_uploader_Lambda}"
            log_debug "Extracted project name for uploader: '$project_name'"
            
            # Apply stack filter if specified (match against project name)
            if [ -n "$STACK_FILTER" ] && [ "$project_name" != "$STACK_FILTER" ]; then
                log_debug "Skipping $func (not in filtered stack: $STACK_FILTER)"
                continue
            fi
            
            local key="${project_name}-uh_uploader"
            LAMBDA_FUNCTIONS["$key"]="$func"
            log_info "Mapped $key -> $func"
        elif [[ "$func" == *"_biobayb_uh_publisher_Lambda" ]]; then
            # Extract project name from function name (format: {project_name}_biobayb_uh_publisher_Lambda)
            local project_name="${func%_biobayb_uh_publisher_Lambda}"
            log_debug "Extracted project name for publisher: '$project_name'"
            
            # Apply stack filter if specified (match against project name)
            if [ -n "$STACK_FILTER" ] && [ "$project_name" != "$STACK_FILTER" ]; then
                log_debug "Skipping $func (not in filtered stack: $STACK_FILTER)"
                continue
            fi
            
            local key="${project_name}-uh_publisher"
            LAMBDA_FUNCTIONS["$key"]="$func"
            log_info "Mapped $key -> $func"
        else
            log_debug "Function '$func' doesn't match expected patterns"
        fi
    done
    
    log_debug "Processed $function_count functions total"
    
    if [ ${#LAMBDA_FUNCTIONS[@]} -eq 0 ]; then
        if [ -n "$STACK_FILTER" ]; then
            log_error "No Lambda functions found for stack: $STACK_FILTER"
        else
            log_error "No valid Lambda functions discovered"
        fi
        exit 1
    fi
    
    local stack_info=""
    if [ -n "$STACK_FILTER" ]; then
        stack_info=" for stack: $STACK_FILTER"
    else
        stack_info=" across deployed stacks"
    fi
    log_info "Discovered ${#LAMBDA_FUNCTIONS[@]} Lambda functions${stack_info}"
}

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_header() {
    echo -e "${BLUE}[HEADER]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_debug() {
    if [ "$DEBUG" = "true" ]; then
        echo -e "${PURPLE}[DEBUG]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
    fi
}

# Build-related functions
ecr_login() {
    log_header "Authenticating with ECR..."
    
    aws ecr get-login-password --region $AWS_REGION | \
        docker login --username AWS --password-stdin $ECR_REGISTRY
    
    if [ $? -eq 0 ]; then
        log_info "Successfully authenticated with ECR"
    else
        log_error "Failed to authenticate with ECR"
        exit 1
    fi
}

setup_build_directories() {
    log_header "Setting up build directories..."
    
    # Clean up existing build directory
    if [ -d "$BUILD_DIR" ]; then
        rm -rf "$BUILD_DIR"
    fi
    
    # Create build directories for each lambda function type
    for func_type in "uh_uploader" "uh_publisher"; do
        mkdir -p "$BUILD_DIR/$func_type"
        
        # Copy requirements.txt
        cp requirements.txt "$BUILD_DIR/$func_type/"
        
        # Copy entire ultrahuman package
        cp -r ultrahuman/ "$BUILD_DIR/$func_type/"
        
        # Copy the specific Dockerfile for this function type
        cp "$DOCKER_DIR/Dockerfile.$func_type" "$BUILD_DIR/$func_type/Dockerfile"
        
        # Add cache busting to ensure fresh builds
        # local cache_bust=$(date +%s)
        # echo "# Cache bust: $cache_bust" >> "$BUILD_DIR/$func_type/Dockerfile"
        
        log_info "Created build directory for $func_type"
    done
}

build_lambda_image() {
    local func_type=$1
    local docker_tag="biobayb_${func_type}"
    local image_name="$ECR_REGISTRY/$ECR_REPOSITORY:$docker_tag"
    local build_context="$BUILD_DIR/$func_type"
    
    log_header "Building Docker image for $func_type -> $docker_tag..."
    
    # Build the Docker image
    docker buildx build --platform linux/amd64 --provenance=false \
        -t "$image_name" \
        -f "$build_context/Dockerfile" \
        "$build_context"
    
    if [ $? -eq 0 ]; then
        log_info "Successfully built $image_name"
        
        # Tag with version if available
        if [ -n "$VERSION" ]; then
            local versioned_image="$ECR_REGISTRY/$ECR_REPOSITORY:$docker_tag-$VERSION"
            docker tag "$image_name" "$versioned_image"
            log_info "Tagged $versioned_image"
        fi
    else
        log_error "Failed to build $image_name"
        exit 1
    fi
}

push_to_ecr() {
    local func_type=$1
    local docker_tag="biobayb_${func_type}"
    local image_name="$ECR_REGISTRY/$ECR_REPOSITORY:$docker_tag"
    
    log_header "Pushing image to ECR: $image_name"
    
    docker push "$image_name"
    
    if [ $? -eq 0 ]; then
        log_info "Successfully pushed $image_name to ECR"
        
        # Push versioned image if available
        if [ -n "$VERSION" ]; then
            local versioned_image="$ECR_REGISTRY/$ECR_REPOSITORY:$docker_tag-$VERSION"
            docker push "$versioned_image"
            log_info "Successfully pushed $versioned_image to ECR"
        fi
    else
        log_error "Failed to push $image_name to ECR"
        exit 1
    fi
}

update_lambda_functions() {
    log_header "Updating Lambda functions with new container images..."
    
    local total_updated=0
    local total_failed=0
    
    for local_func in "${!LAMBDA_FUNCTIONS[@]}"; do
        local aws_func=${LAMBDA_FUNCTIONS[$local_func]}
        
        # Determine function type from the key
        local func_type=""
        if [[ "$local_func" == *"uh_uploader" ]]; then
            func_type="uh_uploader"
        elif [[ "$local_func" == *"uh_publisher" ]]; then
            func_type="uh_publisher"
        else
            log_warning "Cannot determine function type for $local_func, skipping..."
            continue
        fi
        
        local docker_tag="biobayb_${func_type}"
        local image_uri="$ECR_REGISTRY/$ECR_REPOSITORY:$docker_tag"
        
        log_info "Updating Lambda function: $aws_func with image: $image_uri"
        
        # Update function code to use new container image
        if aws lambda update-function-code \
            --function-name "$aws_func" \
            --image-uri "$image_uri" \
            --region "$AWS_REGION" \
            --output table; then
            
            log_info "Successfully updated Lambda function $aws_func"
            
            # Wait for function to be updated
            log_info "Waiting for function update to complete..."
            if ! timeout 300 aws lambda wait function-updated --function-name "$aws_func" --region "$AWS_REGION"; then
                log_warning "Wait for function update timed out or failed for $aws_func, but update likely succeeded"
            else
                log_info "Function update completed for $aws_func"
            fi
            
            # Check if function has aliases and update them
            log_info "Checking for aliases on $aws_func..."
            local aliases=$(aws lambda list-aliases \
                --function-name "$aws_func" \
                --region "$AWS_REGION" \
                --query 'Aliases[].Name' \
                --output text 2>/dev/null || echo "")
            
            if [ -n "$aliases" ] && [ "$aliases" != "None" ]; then
                # Publish a new version from $LATEST
                log_info "Publishing new version for $aws_func..."
                local new_version=$(aws lambda publish-version \
                    --function-name "$aws_func" \
                    --region "$AWS_REGION" \
                    --query 'Version' \
                    --output text 2>/dev/null)
                
                if [ -n "$new_version" ] && [ "$new_version" != "None" ]; then
                    log_info "Published version $new_version for $aws_func"
                    
                    for alias in $aliases; do
                        log_info "Updating alias '$alias' to point to version $new_version..."
                        if aws lambda update-alias \
                            --function-name "$aws_func" \
                            --name "$alias" \
                            --function-version "$new_version" \
                            --region "$AWS_REGION" \
                            --output table; then
                            log_info "Successfully updated alias '$alias' for $aws_func"
                        else
                            log_warning "Failed to update alias '$alias' for $aws_func"
                        fi
                    done
                else
                    log_warning "Failed to publish new version for $aws_func, skipping alias updates"
                fi
            else
                log_info "No aliases found for $aws_func"
            fi
            
            ((total_updated++))
        else
            log_error "Failed to update Lambda function $aws_func"
            ((total_failed++))
        fi
    done
    
    # Summary
    if [ $total_failed -eq 0 ]; then
        log_info "Successfully updated all $total_updated Lambda functions"
    else
        log_error "Updated $total_updated functions, but $total_failed failed"
        if [ $total_updated -eq 0 ]; then
            exit 1
        fi
    fi
}

deploy_with_cdk() {
    if [ -d "$CDK_DIR" ]; then
        log_header "Deploying infrastructure with CDK..."
        
        cd "$CDK_DIR"
        
        # Install CDK dependencies if needed
        if [ -f "requirements.txt" ]; then
            pip install -r requirements.txt
        fi
        
        # Deploy CDK stack
        cdk deploy --all --require-approval never
        
        cd ..
        log_info "CDK deployment completed"
    else
        log_warning "CDK directory not found. Skipping CDK deployment."
    fi
}

build_pipeline() {
    log_header "Starting build pipeline..."
    
    # Check Docker daemon
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    # ECR authentication
    ecr_login
    
    # Setup build directories
    setup_build_directories
    
    # Build and push each lambda function type
    for func_type in "uh_uploader" "uh_publisher"; do
        build_lambda_image "$func_type"
        push_to_ecr "$func_type"
    done
    
    # Clean up build artifacts
    cleanup_build_artifacts
    
    log_info "Build pipeline completed successfully"
}

cleanup_build_artifacts() {
    log_header "Cleaning up build artifacts..."
    
    # Remove build directory
    if [ -d "$BUILD_DIR" ]; then
        rm -rf "$BUILD_DIR"
        log_info "Removed build directory"
    fi
    
    # Remove local Docker images
    for func_type in "uh_uploader" "uh_publisher"; do
        local docker_tag="biobayb_${func_type}"
        local image_name="$ECR_REGISTRY/$ECR_REPOSITORY:$docker_tag"
        
        if docker images -q "$image_name" &> /dev/null; then
            docker rmi "$image_name" &> /dev/null || true
            log_info "Removed local image: $image_name"
        fi
        
        # Remove versioned image if exists
        if [ -n "$VERSION" ]; then
            local versioned_image="$ECR_REGISTRY/$ECR_REPOSITORY:$docker_tag-$VERSION"
            if docker images -q "$versioned_image" &> /dev/null; then
                docker rmi "$versioned_image" &> /dev/null || true
                log_info "Removed local versioned image: $versioned_image"
            fi
        fi
    done
    
    log_info "Cleanup completed"
}

# Validation functions
validate_prerequisites() {
    log_header "Validating prerequisites..."
    
    local errors=0
    
    # Check required tools
    local required_tools=("docker" "aws" "cdk" "python3" "pip")
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            log_error "$tool is not installed or not in PATH"
            errors=$((errors + 1))
        else
            log_debug "$tool is available"
        fi
    done
    
    # Check Docker daemon
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        errors=$((errors + 1))
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials are not configured"
        errors=$((errors + 1))
    else
        local aws_account=$(aws sts get-caller-identity --query Account --output text)
        local aws_region=$(aws configure get region)
        log_info "AWS Account: $aws_account, Region: $aws_region"
    fi
    
    # Check CDK bootstrap
    if ! aws cloudformation describe-stacks --stack-name CDKToolkit --region "$AWS_REGION" &> /dev/null; then
        log_warning "CDK is not bootstrapped in this region. Run 'cdk bootstrap' first."
    fi
    
    # Check project structure
    local required_files=("requirements.txt" "ultrahuman/" "docker/Dockerfile.uh_uploader" "docker/Dockerfile.uh_publisher")
    for file in "${required_files[@]}"; do
        if [ ! -e "$SCRIPT_DIR/$file" ]; then
            log_error "Required file/directory not found: $file"
            errors=$((errors + 1))
        fi
    done
    
    if [ $errors -gt 0 ]; then
        log_error "Validation failed with $errors errors"
        exit 1
    fi
    
    log_info "All prerequisites validated successfully"
    
    # Discover deployed Lambda functions
    discover_lambda_functions
}

# Backup current Lambda functions
backup_lambda_functions() {
    log_header "Creating backup of current Lambda functions..."
    
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local stack_suffix=""
    if [ -n "$STACK_FILTER" ]; then
        stack_suffix="_${STACK_FILTER}"
    fi
    local backup_dir="$SCRIPT_DIR/backup/${timestamp}${stack_suffix}"
    mkdir -p "$backup_dir"
    
    for local_func in "${!LAMBDA_FUNCTIONS[@]}"; do
        local aws_func=${LAMBDA_FUNCTIONS[$local_func]}
        
        log_info "Backing up $aws_func..."
        
        # Get function configuration
        aws lambda get-function-configuration \
            --function-name "$aws_func" \
            --region "$AWS_REGION" \
            --output json > "$backup_dir/${aws_func}_config.json" 2>/dev/null || {
            log_warning "Could not backup configuration for $aws_func (function may not exist)"
        }
        
        # Get function code location
        aws lambda get-function \
            --function-name "$aws_func" \
            --region "$AWS_REGION" \
            --output json > "$backup_dir/${aws_func}_function.json" 2>/dev/null || {
            log_warning "Could not backup function code info for $aws_func"
        }
    done
    
    echo "$backup_dir" > "$SCRIPT_DIR/.last_backup"
    log_info "Backup completed in: $backup_dir"
}

# Test Lambda functions
test_lambda_functions() {
    log_header "Testing Lambda functions..."
    
    for local_func in "${!LAMBDA_FUNCTIONS[@]}"; do
        local aws_func=${LAMBDA_FUNCTIONS[$local_func]}
        
        log_info "Testing $aws_func..."
        
        # Create test event
        local test_event='{
            "Records": [
                {
                    "eventSource": "aws:sns",
                    "eventVersion": "1.0",
                    "eventSubscriptionArn": "arn:aws:sns:us-east-1:123456789012:test-topic",
                    "sns": {
                        "Message": "{\"participant_id\": \"test-participant\", \"test\": true}",
                        "MessageId": "test-message-id",
                        "Subject": "Test Event",
                        "Timestamp": "2023-01-01T00:00:00.000Z",
                        "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"
                    }
                }
            ]
        }'
        
        # Invoke function with test event
        local response_file="/tmp/${aws_func}_test_response.json"
        local log_file="/tmp/${aws_func}_test_logs.txt"
        
        aws lambda invoke \
            --function-name "$aws_func" \
            --payload "$test_event" \
            --log-type Tail \
            --region "$AWS_REGION" \
            --cli-binary-format raw-in-base64-out \
            "$response_file" \
            --query 'LogResult' \
            --output text 2>/dev/null | base64 -d > "$log_file" || {
            log_error "Test failed for $aws_func"
            return 1
        }
        
        # Check response
        if grep -q "error" "$response_file" 2>/dev/null; then
            log_error "Test failed for $aws_func - check logs at $log_file"
            cat "$response_file"
            return 1
        else
            log_info "Test passed for $aws_func"
        fi
        
        # Clean up
        rm -f "$response_file" "$log_file"
    done
    
    log_info "All Lambda function tests passed"
}

# Rollback to previous version
rollback_deployment() {
    log_header "Rolling back deployment..."
    
    if [ ! -f "$SCRIPT_DIR/.last_backup" ]; then
        log_error "No backup found for rollback"
        return 1
    fi
    
    local backup_dir=$(cat "$SCRIPT_DIR/.last_backup")
    if [ ! -d "$backup_dir" ]; then
        log_error "Backup directory not found: $backup_dir"
        return 1
    fi
    
    log_info "Rolling back using backup: $backup_dir"
    
    for local_func in "${!LAMBDA_FUNCTIONS[@]}"; do
        local aws_func=${LAMBDA_FUNCTIONS[$local_func]}
        local config_file="$backup_dir/${aws_func}_config.json"
        local function_file="$backup_dir/${aws_func}_function.json"
        
        if [ -f "$config_file" ] && [ -f "$function_file" ]; then
            log_info "Rolling back $aws_func..."
            
            # Get previous image URI
            local image_uri=$(jq -r '.Code.ImageUri' "$function_file")
            
            if [ "$image_uri" != "null" ]; then
                aws lambda update-function-code \
                    --function-name "$aws_func" \
                    --image-uri "$image_uri" \
                    --region "$AWS_REGION" \
                    --output table
                
                log_info "Rolled back $aws_func to previous version"
            else
                log_warning "Could not find previous image URI for $aws_func"
            fi
        else
            log_warning "Backup files not found for $aws_func"
        fi
    done
    
    log_info "Rollback completed"
}

# Health check
health_check() {
    log_header "Performing health check..."
    
    local health_check_passed=true
    
    for local_func in "${!LAMBDA_FUNCTIONS[@]}"; do
        local aws_func=${LAMBDA_FUNCTIONS[$local_func]}
        
        log_info "Checking health of $aws_func..."
        
        # Check function state
        local state=$(aws lambda get-function-configuration \
            --function-name "$aws_func" \
            --region "$AWS_REGION" \
            --query 'State' \
            --output text 2>/dev/null)
        
        if [ "$state" = "Active" ]; then
            log_info "$aws_func is Active"
        else
            log_error "$aws_func is not Active (State: $state)"
            health_check_passed=false
        fi
        
        # Check recent errors
        local error_count=$(aws logs filter-log-events \
            --log-group-name "/aws/lambda/$aws_func" \
            --start-time $(date -d '5 minutes ago' +%s)000 \
            --filter-pattern "ERROR" \
            --region "$AWS_REGION" \
            --query 'length(events)' \
            --output text 2>/dev/null || echo "0")
        
        if [ "$error_count" -gt 0 ]; then
            log_warning "$aws_func has $error_count errors in the last 5 minutes"
        else
            log_info "$aws_func has no recent errors"
        fi
    done
    
    if [ "$health_check_passed" = true ]; then
        log_info "Health check passed"
        return 0
    else
        log_error "Health check failed"
        return 1
    fi
}

# Deploy pipeline
deploy_pipeline() {
    local deployment_method=${1:-"direct"}
    local skip_tests=${2:-false}
    
    log_header "Starting deployment pipeline (method: $deployment_method)..."
    
    # Validate prerequisites
    validate_prerequisites
    
    # Create backup
    backup_lambda_functions
    
    # Run build pipeline
    build_pipeline
    
    # Deploy
    log_header "Starting deployment phase..."
    if [ "$deployment_method" = "cdk" ]; then
        deploy_with_cdk
    else
        update_lambda_functions
    fi
    
    # Wait for deployment to stabilize
    log_info "Waiting for deployment to stabilize..."
    sleep 15
    
    # Run tests if not skipped
    # TODO fix tests.
    # if [ "$skip_tests" = false ]; then
    #     if ! test_lambda_functions; then
    #         log_error "Tests failed - initiating rollback"
    #         rollback_deployment
    #         exit 1
    #     fi
    # fi
    
    # Health check
    if ! health_check; then
        log_error "Health check failed - initiating rollback"
        rollback_deployment
        exit 1
    fi
    
    log_info "Deployment pipeline completed successfully"
}

# Main function with argument parsing
main() {
    local deployment_method="direct"
    local skip_tests=false
    local action="deploy"
    local build_only=false
    local deploy_only=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --cdk)
                deployment_method="cdk"
                shift
                ;;
            --skip-tests)
                skip_tests=true
                shift
                ;;
            --build-only)
                build_only=true
                action="build-only"
                shift
                ;;
            --deploy-only)
                deploy_only=true
                action="deploy-only"
                shift
                ;;
            --rollback)
                action="rollback"
                shift
                ;;
            --health-check)
                action="health-check"
                shift
                ;;
            --test)
                action="test"
                shift
                ;;
            --stack)
                STACK_FILTER="$2"
                shift 2
                ;;
            --debug)
                DEBUG=true
                shift
                ;;
            --help|-h)
                cat << EOF
Usage: $0 [OPTIONS]

Comprehensive deployment automation for Ultrahuman SensorFabric Lambda functions.

Options:
  --cdk                 Use CDK for deployment instead of direct Lambda updates
  --skip-tests          Skip function testing after deployment
  --build-only          Only build and push images, don't deploy
  --deploy-only         Only deploy (assumes images already exist in ECR)
  --rollback            Rollback to previous deployment
  --health-check        Perform health check only
  --test                Run tests only
  --stack STACK_NAME    Target specific stack (e.g., UltraHuman-AZ-1)
  --debug               Enable debug logging
  --help, -h            Show this help message

Examples:
  $0                    # Build and deploy with direct Lambda updates
  $0 --cdk              # Build and deploy using CDK
  $0 --build-only       # Only build and push to ECR
  $0 --deploy-only      # Only deploy from existing ECR images
  $0 --skip-tests       # Deploy without running tests
  $0 --rollback         # Rollback to previous version
  $0 --health-check     # Check current deployment health
  $0 --test             # Run tests on current deployment
  $0 --stack UltraHuman-AZ-1 --health-check  # Check specific stack health

Environment variables:
  DEBUG                 Enable debug logging (true/false)
  VERSION               Version tag for Docker images
  AWS_PROFILE           AWS profile to use
EOF
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    case $action in
        deploy)
            deploy_pipeline "$deployment_method" "$skip_tests"
            ;;
        build-only)
            validate_prerequisites
            build_pipeline
            ;;
        deploy-only)
            validate_prerequisites
            if [ "$deployment_method" = "cdk" ]; then
                deploy_with_cdk
            else
                update_lambda_functions
            fi
            ;;
        rollback)
            rollback_deployment
            ;;
        health-check)
            health_check
            ;;
        test)
            test_lambda_functions
            ;;
        *)
            log_error "Unknown action: $action"
            exit 1
            ;;
    esac
}

# Error handling
trap 'log_error "Script failed at line $LINENO"' ERR

# Change to script directory
cd "$SCRIPT_DIR"

# Run main function
main "$@"