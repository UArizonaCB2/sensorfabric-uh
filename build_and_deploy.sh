#!/bin/bash

# DEPRECATED: This script has been consolidated into deploy.sh
# Please use deploy.sh instead for all build and deployment operations

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "================================================="
echo "DEPRECATION NOTICE"
echo "================================================="
echo "build_and_deploy.sh has been consolidated into deploy.sh"
echo "This script now redirects to the consolidated version."
echo ""
echo "For equivalent functionality, use:"

# Map old arguments to new ones
case "${1:-}" in
    --help|-h)
        echo "  ./deploy.sh --help"
        echo ""
        exec "$SCRIPT_DIR/deploy.sh" --help
        ;;
    --build-only)
        echo "  ./deploy.sh --build-only"
        echo ""
        exec "$SCRIPT_DIR/deploy.sh" --build-only
        ;;
    --deploy-only)
        echo "  ./deploy.sh --deploy-only"
        echo ""
        exec "$SCRIPT_DIR/deploy.sh" --deploy-only
        ;;
    --use-cdk)
        echo "  ./deploy.sh --cdk"
        echo ""
        exec "$SCRIPT_DIR/deploy.sh" --cdk
        ;;
    --clean|-c)
        echo "  Clean functionality has been integrated into deploy.sh"
        echo "  Build artifacts are automatically cleaned up."
        exit 0
        ;;
    "")
        echo "  ./deploy.sh              # Full build and deploy"
        echo ""
        exec "$SCRIPT_DIR/deploy.sh"
        ;;
    *)
        echo "  ./deploy.sh [options]"
        echo ""
        echo "Available options:"
        echo "  --build-only    Only build and push images"
        echo "  --deploy-only   Only deploy from existing images"
        echo "  --cdk           Use CDK for deployment"
        echo "  --help          Show full help"
        echo ""
        echo "Redirecting to deploy.sh with equivalent options..."
        
        # Try to map other arguments
        new_args=()
        for arg in "$@"; do
            case "$arg" in
                --use-cdk)
                    new_args+=(--cdk)
                    ;;
                *)
                    new_args+=("$arg")
                    ;;
            esac
        done
        
        exec "$SCRIPT_DIR/deploy.sh" "${new_args[@]}"
        ;;
esac