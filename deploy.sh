#!/bin/bash

# Comprehensive deployment automation script for SensorFabric Lambda functions
# This script provides a complete deployment pipeline with validation and rollback capabilities

# set -e  # Exit on any error. this will break the script because of how aws responds waiting for lambda updates sometimes.

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PROJECT_NAME="sensorfabric-uh"  # Currently unused
ECR_REGISTRY="509812589231.dkr.ecr.us-east-1.amazonaws.com"
ECR_REPOSITORY="uh-biobayb"
AWS_REGION="us-east-1"
CDK_DIR="$SCRIPT_DIR/cdk"
DOCKER_DIR="docker"
BUILD_DIR="build"

# Lambda function mappings - will be populated dynamically
# Using arrays instead of associative arrays for cross-platform compatibility
LAMBDA_FUNCTION_KEYS=()
LAMBDA_FUNCTION_VALUES=()

# Helper function to add key-value pairs to our pseudo-associative array
add_lambda_function() {
    local key="$1"
    local value="$2"
    LAMBDA_FUNCTION_KEYS+=("$key")
    LAMBDA_FUNCTION_VALUES+=("$value")
}

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
    LAMBDA_FUNCTION_KEYS=()
    LAMBDA_FUNCTION_VALUES=()
    
    # Get all Lambda functions that match our naming pattern
    log_debug "Querying AWS Lambda for functions matching our pattern..."
    local functions_json
    functions_json=$(aws lambda list-functions \
        --region "$AWS_REGION" \
        --query 'Functions[?contains(FunctionName, `_biobayb_uh_uploader_Lambda`) || contains(FunctionName, `_biobayb_uh_publisher_Lambda`) || contains(FunctionName, `_biobayb_uh_template_generator_Lambda`) || contains(FunctionName, `_biobayb_uh_jwt_generator_Lambda`)].FunctionName' \
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
        add_lambda_function "uh_uploader" "biobayb_uh_uploader"
        add_lambda_function "uh_publisher" "biobayb_uh_sns_publisher"
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
            add_lambda_function "$key" "$func"
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
            add_lambda_function "$key" "$func"
            log_info "Mapped $key -> $func"
        elif [[ "$func" == *"_biobayb_uh_template_generator_Lambda" ]]; then
            # Extract project name from function name (format: {project_name}_biobayb_uh_template_generator_Lambda)
            local project_name="${func%_biobayb_uh_template_generator_Lambda}"
            log_debug "Extracted project name for template generator: '$project_name'"
            
            # Apply stack filter if specified (match against project name)
            if [ -n "$STACK_FILTER" ] && [ "$project_name" != "$STACK_FILTER" ]; then
                log_debug "Skipping $func (not in filtered stack: $STACK_FILTER)"
                continue
            fi
            
            local key="${project_name}-uh_template_generator"
            add_lambda_function "$key" "$func"
            log_info "Mapped $key -> $func"
        elif [[ "$func" == *"_biobayb_uh_jwt_generator_Lambda" ]]; then
            # Extract project name from function name (format: {project_name}_biobayb_uh_jwt_generator_Lambda)
            local project_name="${func%_biobayb_uh_jwt_generator_Lambda}"
            log_debug "Extracted project name for JWT generator: '$project_name'"
            
            # Apply stack filter if specified (match against project name)
            if [ -n "$STACK_FILTER" ] && [ "$project_name" != "$STACK_FILTER" ]; then
                log_debug "Skipping $func (not in filtered stack: $STACK_FILTER)"
                continue
            fi
            
            local key="${project_name}-uh_jwt_generator"
            add_lambda_function "$key" "$func"
            log_info "Mapped $key -> $func"
        else
            log_debug "Function '$func' doesn't match expected patterns"
        fi
    done
    
    log_debug "Processed $function_count functions total"
    
    if [ ${#LAMBDA_FUNCTION_KEYS[@]} -eq 0 ]; then
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
    log_info "Discovered ${#LAMBDA_FUNCTION_KEYS[@]} Lambda functions${stack_info}"
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
    
    if aws ecr get-login-password --region $AWS_REGION | \
        docker login --username AWS --password-stdin $ECR_REGISTRY; then
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
    
    # Create single build directory for shared image
    mkdir -p "$BUILD_DIR/shared"
    
    log_debug "Build directory created: $BUILD_DIR/shared"
    log_debug "Current working directory: $(pwd)"
    log_debug "Checking source files existence..."
    
    # Copy requirements.txt
    cp requirements.txt "$BUILD_DIR/shared/"
    
    # Copy entire ultrahuman package (ensure compatibility across platforms)
    if [ -d "ultrahuman" ]; then
        log_debug "ultrahuman directory found, contents:"
        if [ "$DEBUG" = "true" ]; then
            ls -la ultrahuman/
        fi
        
        # Use rsync for better cross-platform compatibility if available, fallback to cp
        if command -v rsync &> /dev/null; then
            log_debug "Using rsync to copy ultrahuman directory"
            rsync -a ultrahuman/ "$BUILD_DIR/shared/ultrahuman/"
        else
            # Create target directory first to ensure it exists
            log_debug "Using cp to copy ultrahuman directory"
            mkdir -p "$BUILD_DIR/shared/ultrahuman"
            cp -r ultrahuman/* "$BUILD_DIR/shared/ultrahuman/"
        fi
        
        log_debug "Verifying ultrahuman copy completed:"
        if [ "$DEBUG" = "true" ]; then
            ls -la "$BUILD_DIR/shared/ultrahuman/"
        fi
    else
        log_error "ultrahuman directory not found in $(pwd)"
        log_debug "Contents of current directory:"
        ls -la
        exit 1
    fi
    
    # Copy the shared Dockerfile
    cp "$DOCKER_DIR/Dockerfile.shared" "$BUILD_DIR/shared/Dockerfile"
    
    log_info "Created shared build directory"
}

build_shared_lambda_image() {
    local docker_tag="shared"
    local image_name="$ECR_REGISTRY/$ECR_REPOSITORY:$docker_tag"
    local build_context="$BUILD_DIR/shared"
    
    log_header "Building shared Docker image -> $docker_tag..."
    
    # Build the Docker image
    if docker buildx build --platform linux/amd64 --provenance=false \
        -t "$image_name" \
        -f "$build_context/Dockerfile" \
        "$build_context"; then
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

push_shared_to_ecr() {
    local docker_tag="shared"
    local image_name="$ECR_REGISTRY/$ECR_REPOSITORY:$docker_tag"
    
    log_header "Pushing shared image to ECR: $image_name"
    
    if docker push "$image_name"; then
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

cleanup_old_ecr_images() {
    log_header "Cleaning up untagged ECR images..."
    
    # Get list of untagged images (images with no tags)
    local untagged_images
    untagged_images=$(aws ecr describe-images \
        --repository-name "$ECR_REPOSITORY" \
        --region "$AWS_REGION" \
        --query 'imageDetails[?imageTags==null].imageDigest' \
        --output json 2>/dev/null || echo "[]")
    
    if [ "$untagged_images" = "[]" ]; then
        log_info "No untagged images found in ECR repository"
        return
    fi
    
    # Count untagged images
    local image_count=0
    if command -v jq &> /dev/null; then
        image_count=$(echo "$untagged_images" | jq length 2>/dev/null || echo 0)
    else
        # Fallback count without jq
        image_count=$(echo "$untagged_images" | grep -o '"sha256:[^"]*"' | wc -l)
    fi
    
    if [ "$image_count" -eq 0 ]; then
        log_info "No untagged images found for cleanup"
        return
    fi
    
    log_info "Found $image_count untagged images to delete"
    
    # Delete untagged images in batches (AWS limit is 100 per batch)
    local deleted_count=0
    local batch_size=100
    
    if command -v jq &> /dev/null; then
        # Process in batches using jq
        local total_batches=$(( (image_count + batch_size - 1) / batch_size ))
        
        for ((batch=0; batch<total_batches; batch++)); do
            local start_index=$((batch * batch_size))
            local batch_digests
            batch_digests=$(echo "$untagged_images" | jq -r ".[$start_index:$((start_index + batch_size))][]" 2>/dev/null)
            
            if [ -n "$batch_digests" ]; then
                # Create image IDs JSON array for batch delete
                local image_ids_json="["
                local first=true
                while IFS= read -r digest; do
                    if [ -n "$digest" ]; then
                        if [ "$first" = true ]; then
                            first=false
                        else
                            image_ids_json="$image_ids_json,"
                        fi
                        image_ids_json="$image_ids_json{\"imageDigest\":\"$digest\"}"
                    fi
                done <<< "$batch_digests"
                image_ids_json="$image_ids_json]"
                
                if [ "$image_ids_json" != "[]" ]; then
                    log_info "Deleting batch $((batch + 1))/$total_batches of untagged images..."
                    
                    if aws ecr batch-delete-image \
                        --repository-name "$ECR_REPOSITORY" \
                        --region "$AWS_REGION" \
                        --image-ids "$image_ids_json" \
                        --output text >/dev/null 2>&1; then
                        
                        local batch_count
                        batch_count=$(echo "$batch_digests" | wc -l)
                        deleted_count=$((deleted_count + batch_count))
                        log_info "Successfully deleted $batch_count untagged images in this batch"
                    else
                        log_warning "Failed to delete some images in batch $((batch + 1))"
                    fi
                fi
            fi
        done
    else
        log_warning "jq not available, skipping ECR cleanup of untagged images"
        return
    fi
    
    if [ $deleted_count -gt 0 ]; then
        log_info "Successfully cleaned up $deleted_count untagged images from ECR"
    else
        log_warning "No untagged images were deleted"
    fi
}

update_lambda_functions() {
    log_header "Updating Lambda functions with new shared container image..."
    
    local total_updated=0
    local total_failed=0
    local shared_image_uri="$ECR_REGISTRY/$ECR_REPOSITORY:shared"
    
    for i in "${!LAMBDA_FUNCTION_KEYS[@]}"; do
        local local_func="${LAMBDA_FUNCTION_KEYS[$i]}"
        local aws_func="${LAMBDA_FUNCTION_VALUES[$i]}"
        
        log_info "Updating Lambda function: $aws_func with shared image: $shared_image_uri"
        
        # Update function code to use new shared container image
        if aws lambda update-function-code \
            --function-name "$aws_func" \
            --image-uri "$shared_image_uri" \
            --region "$AWS_REGION" \
            --no-cli-pager \
            --output json > /dev/null; then
            
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
            local aliases
            aliases=$(aws lambda list-aliases \
                --function-name "$aws_func" \
                --region "$AWS_REGION" \
                --query 'Aliases[].Name' \
                --output text 2>/dev/null || echo "")
            
            if [ -n "$aliases" ] && [ "$aliases" != "None" ]; then
                # Publish a new version from $LATEST
                log_info "Publishing new version for $aws_func..."
                local new_version
                new_version=$(aws lambda publish-version \
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
                            --no-cli-pager \
                            --output json > /dev/null; then
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
        
        (cd "$CDK_DIR" && {
            # Install CDK dependencies if needed
            if [ -f "requirements.txt" ]; then
                pip install -r requirements.txt
            fi
            
            # Deploy CDK stack
            cdk deploy --all --require-approval never
        })
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
    
    # Build and push single shared image
    build_shared_lambda_image
    push_shared_to_ecr
    
    # Clean up untagged ECR images after all pushes are complete
    cleanup_old_ecr_images
    
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
    
    # Remove local shared Docker image
    local docker_tag="shared"
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
    
    log_info "Cleanup completed"
}

# Security validation functions
check_public_s3_buckets() {
    log_header "Checking for public S3 buckets..."
    
    local public_buckets=()
    
    # Get all S3 buckets
    log_debug "Retrieving list of S3 buckets..."
    local buckets
    
    if ! buckets=$(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null) || [ -z "$buckets" ]; then
        log_warning "Could not retrieve S3 bucket list or no buckets found"
        return 0
    fi
    
    # Check each bucket for public access
    for bucket in $buckets; do
        # Skip buckets that start with 'sagemaker-' or 'cdk-', or whitelist 'ymap-resources'
        if [[ "$bucket" == sagemaker-* ]] || [[ "$bucket" == cdk-* ]] || [[ "$bucket" == "ymap-resources" ]]; then
            log_debug "Skipping bucket: $bucket (ignored prefix or whitelisted)"
            continue
        fi
        
        log_debug "Checking bucket: $bucket"
        
        # Check bucket policy for public access
        local has_public_policy=false
        if aws s3api get-bucket-policy --bucket "$bucket" --output text &>/dev/null; then
            local policy
            policy=$(aws s3api get-bucket-policy --bucket "$bucket" --query 'Policy' --output text 2>/dev/null)
            if echo "$policy" | grep -q '"Principal":\s*"\*"' || echo "$policy" | grep -q '"Principal":\s*{\s*"AWS":\s*"\*"'; then
                has_public_policy=true
                log_debug "Bucket $bucket has public policy"
            fi
        fi
        
        # Check bucket ACL for public access
        local has_public_acl=false
        local acl_check
        acl_check=$(aws s3api get-bucket-acl --bucket "$bucket" --query 'Grants[?Grantee.URI==`http://acs.amazonaws.com/groups/global/AllUsers` || Grantee.URI==`http://acs.amazonaws.com/groups/global/AuthenticatedUsers`]' --output json 2>/dev/null)
        if [ "$acl_check" != "[]" ] && [ "$acl_check" != "null" ]; then
            has_public_acl=true
            log_debug "Bucket $bucket has public ACL"
        fi
        
        # Check public access block settings
        local public_access_blocked=true
        local pab_settings
        if pab_settings=$(aws s3api get-public-access-block --bucket "$bucket" --query 'PublicAccessBlockConfiguration' --output json 2>/dev/null) && [ "$pab_settings" != "null" ]; then
            # If any of these settings is false, public access might be allowed
            local block_public_acls block_public_policy ignore_public_acls restrict_public_buckets
            if command -v jq &> /dev/null; then
                block_public_acls=$(echo "$pab_settings" | jq -r '.BlockPublicAcls // true')
                block_public_policy=$(echo "$pab_settings" | jq -r '.BlockPublicPolicy // true')
                ignore_public_acls=$(echo "$pab_settings" | jq -r '.IgnorePublicAcls // true')
                restrict_public_buckets=$(echo "$pab_settings" | jq -r '.RestrictPublicBuckets // true')
                
                if [ "$block_public_acls" = "false" ] || [ "$block_public_policy" = "false" ] || 
                   [ "$ignore_public_acls" = "false" ] || [ "$restrict_public_buckets" = "false" ]; then
                    public_access_blocked=false
                    log_debug "Bucket $bucket has public access block settings that allow public access"
                fi
            else
                # Fallback without jq - assume potentially public if we can't parse
                if echo "$pab_settings" | grep -q '"false"'; then
                    public_access_blocked=false
                    log_debug "Bucket $bucket may have public access (fallback check)"
                fi
            fi
        else
            # No public access block configured - potentially public
            public_access_blocked=false
            log_debug "Bucket $bucket has no public access block configured"
        fi
        
        # Determine if bucket is effectively public
        if [ "$has_public_policy" = true ] || [ "$has_public_acl" = true ] || [ "$public_access_blocked" = false ]; then
            # Double-check by testing actual public access
            local is_actually_public=false
            
            # Try to list objects without authentication (this is a more definitive test)
            if curl -s -f "https://$bucket.s3.amazonaws.com/" >/dev/null 2>&1; then
                is_actually_public=true
                log_debug "Bucket $bucket confirmed publicly accessible via HTTP"
            fi
            
            # If any indicators suggest it might be public, add to list
            if [ "$is_actually_public" = true ] || [ "$has_public_policy" = true ] || [ "$has_public_acl" = true ]; then
                public_buckets+=("$bucket")
                log_warning "Found potentially public bucket: $bucket"
            fi
        fi
    done
    
    # Report results
    if [ ${#public_buckets[@]} -gt 0 ]; then
        echo ""
        echo -e "${RED}╔════════════════════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${RED}║                                 SECURITY WARNING                               ║${NC}"
        echo -e "${RED}║                                                                                ║${NC}"
        echo -e "${RED}║                            PUBLIC S3 BUCKETS DETECTED!                        ║${NC}"
        echo -e "${RED}║                                                                                ║${NC}"
        echo -e "${RED}║  The following S3 buckets appear to have public access permissions:           ║${NC}"
        echo -e "${RED}║                                                                                ║${NC}"
        for bucket in "${public_buckets[@]}"; do
            echo -e "${RED}║  • $bucket${NC}"
            printf "${RED}║%-80s║${NC}\n" ""
        done
        echo -e "${RED}║                                                                                ║${NC}"
        echo -e "${RED}║  This is a SECURITY RISK and violates your security policy.                   ║${NC}"
        echo -e "${RED}║                                                                                ║${NC}"
        echo -e "${RED}║  Please review and secure these buckets before proceeding with deployment.    ║${NC}"
        echo -e "${RED}║                                                                                ║${NC}"
        echo -e "${RED}║  Deployment has been CANCELLED for security reasons.                          ║${NC}"
        echo -e "${RED}╚════════════════════════════════════════════════════════════════════════════════╝${NC}"
        echo ""
        
        log_error "Deployment cancelled due to ${#public_buckets[@]} public S3 bucket(s)"
        exit 1
    else
        log_info "No public S3 buckets detected - security check passed"
    fi
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
        local aws_account
        local aws_region
        aws_account=$(aws sts get-caller-identity --query Account --output text)
        aws_region=$(aws configure get region)
        log_info "AWS Account: $aws_account, Region: $aws_region"
    fi
    
    # Check CDK bootstrap
    if ! aws cloudformation describe-stacks --stack-name CDKToolkit --region "$AWS_REGION" &> /dev/null; then
        log_warning "CDK is not bootstrapped in this region. Run 'cdk bootstrap' first."
    fi
    
    # Check project structure
    local required_files=("requirements.txt" "ultrahuman/" "docker/Dockerfile.shared")
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


# Test Lambda functions
test_lambda_functions() {
    log_header "Testing Lambda functions..."
    
    for i in "${!LAMBDA_FUNCTION_KEYS[@]}"; do
        local local_func="${LAMBDA_FUNCTION_KEYS[$i]}"
        local aws_func="${LAMBDA_FUNCTION_VALUES[$i]}"
        
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


# Health check
health_check() {
    log_header "Performing health check..."
    
    local health_check_passed=true
    
    for i in "${!LAMBDA_FUNCTION_KEYS[@]}"; do
        local local_func="${LAMBDA_FUNCTION_KEYS[$i]}"
        local aws_func="${LAMBDA_FUNCTION_VALUES[$i]}"
        
        log_info "Checking health of $aws_func..."
        
        # Check function state
        local state
        state=$(aws lambda get-function-configuration \
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
        local error_count
        local start_time
        start_time=$(date -d '5 minutes ago' +%s)000
        error_count=$(aws logs filter-log-events \
            --log-group-name "/aws/lambda/$aws_func" \
            --start-time "$start_time" \
            --filter-pattern "error" \
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
    
    # Security check - ensure no public S3 buckets exist
    check_public_s3_buckets
    
    
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
    #         log_error "Tests failed after deployment"
    #         exit 1
    #     fi
    # fi
    
    # Health check
    if ! health_check; then
        log_error "Health check failed"
        exit 1
    fi
    
    log_info "Deployment pipeline completed successfully"
}

# Main function with argument parsing
main() {
    local deployment_method="direct"
    local skip_tests=false
    local action="deploy"
    
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
                action="build-only"
                shift
                ;;
            --deploy-only)
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
            check_public_s3_buckets
            build_pipeline
            ;;
        deploy-only)
            validate_prerequisites
            check_public_s3_buckets
            if [ "$deployment_method" = "cdk" ]; then
                deploy_with_cdk
            else
                update_lambda_functions
            fi
            ;;
        rollback)
            log_error "Rollback functionality has been removed from deploy.sh"
            exit 1
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
cd "$SCRIPT_DIR" || exit

# Run main function
main "$@"