#!/bin/bash
# Validation script for CI/CD systems
# Can be used standalone or integrated into any CI/CD platform

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
TOTAL=0
PASSED=0
FAILED=0

echo "========================================="
echo "  Datacore Configuration Validator"
echo "========================================="
echo ""

# Function to validate a file
validate_file() {
    local file=$1
    TOTAL=$((TOTAL + 1))
    
    echo -n "Validating: $file ... "
    
    if prodi validate --config "$file" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ PASSED${NC}"
        PASSED=$((PASSED + 1))
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        FAILED=$((FAILED + 1))
        
        # Show error details
        echo -e "${YELLOW}Error details:${NC}"
        prodi validate --config "$file" 2>&1 | sed 's/^/  /'
        echo ""
        
        return 1
    fi
}

# Validate configurations
echo "--- Validating Configuration Files ---"
echo ""

if [ -d "configs/envs" ]; then
    while IFS= read -r -d '' file; do
        validate_file "$file"
    done < <(find configs/envs -type f \( -name "*.yml" -o -name "*.yaml" \) -print0)
else
    echo -e "${YELLOW}No configs/envs directory found${NC}"
fi

echo ""

# Validate examples
echo "--- Validating Example Files ---"
echo ""

if [ -d "examples" ]; then
    while IFS= read -r -d '' file; do
        validate_file "$file"
    done < <(find examples -type f \( -name "*.yml" -o -name "*.yaml" \) -print0)
else
    echo -e "${YELLOW}No examples directory found${NC}"
fi

# Summary
echo ""
echo "========================================="
echo "  Validation Summary"
echo "========================================="
echo "Total files:   $TOTAL"
echo -e "${GREEN}Passed:        $PASSED${NC}"
echo -e "${RED}Failed:        $FAILED${NC}"
echo ""

# Exit with appropriate code
if [ $FAILED -gt 0 ]; then
    echo -e "${RED}❌ Validation FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}✅ All validations PASSED${NC}"
    exit 0
fi
