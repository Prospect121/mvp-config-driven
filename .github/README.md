# CI/CD Integration

This directory contains CI/CD workflows for automated validation.

## GitHub Actions

### 1. [validate.yml](workflows/validate.yml)

**Triggered by**:
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop`
- Manual workflow dispatch

**Jobs**:
- ✅ **validate-configs**: Validates all files in `configs/envs/`
- ✅ **validate-examples**: Validates all files in `examples/`
- ✅ **validate-schemas**: Validates JSON schema files
- 📊 **summary**: Aggregates results and reports status

**Usage**:
```bash
# Runs automatically on push/PR
# Or trigger manually from GitHub Actions UI
```

### 2. [pre-commit.yml](workflows/pre-commit.yml)

**Triggered by**: Pull requests (opened, synchronized, reopened)

**Features**:
- Validates only changed YAML files (faster)
- Comments on PR if validation fails
- Uses `tj-actions/changed-files` for efficiency

**Example output**:
```
✅ All changed files are valid!
```

## Local Pre-commit Hook

To validate locally before pushing:

```bash
# Install pre-commit
pip install pre-commit

# Create .pre-commit-config.yaml
cat > .pre-commit-config.yaml << 'EOF'
repos:
  - repo: local
    hooks:
      - id: validate-configs
        name: Validate configuration files
        entry: bash -c 'for f in "$@"; do prodi validate --config "$f" || exit 1; done' --
        language: system
        files: \.(yml|yaml)$
        pass_filenames: true
EOF

# Install git hooks
pre-commit install
```

## Azure DevOps

Create `azure-pipelines.yml`:

```yaml
trigger:
  branches:
    include:
      - main
      - develop
  paths:
    include:
      - configs/**/*.yml
      - examples/**/*.yaml

pool:
  vmImage: 'ubuntu-latest'

steps:
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.10'
  
- script: |
    pip install -e .
  displayName: 'Install dependencies'

- script: |
    for file in configs/envs/**/*.yml examples/**/*.yaml; do
      echo "Validating: $file"
      prodi validate --config "$file" || exit 1
    done
  displayName: 'Validate configurations and examples'
```

## GitLab CI

Create `.gitlab-ci.yml`:

```yaml
stages:
  - validate

validate-configs:
  stage: validate
  image: python:3.10
  before_script:
    - pip install -e .
  script:
    - |
      for file in configs/envs/**/*.yml; do
        echo "Validating: $file"
        prodi validate --config "$file"
      done
  only:
    changes:
      - configs/**/*.yml
      - examples/**/*.yaml

validate-examples:
  stage: validate
  image: python:3.10
  before_script:
    - pip install -e .
  script:
    - |
      find examples -name "*.yaml" | while read file; do
        echo "Validating: $file"
        prodi validate --config "$file"
      done
  only:
    changes:
      - examples/**/*.yaml
```

## Jenkins

Create `Jenkinsfile`:

```groovy
pipeline {
    agent any
    
    stages {
        stage('Setup') {
            steps {
                sh 'pip install -e .'
            }
        }
        
        stage('Validate Configs') {
            steps {
                sh '''
                    for file in configs/envs/**/*.yml; do
                        echo "Validating: $file"
                        prodi validate --config "$file"
                    done
                '''
            }
        }
        
        stage('Validate Examples') {
            steps {
                sh '''
                    find examples -name "*.yaml" | while read file; do
                        echo "Validating: $file"
                        prodi validate --config "$file"
                    done
                '''
            }
        }
    }
    
    post {
        failure {
            echo 'Validation failed!'
        }
        success {
            echo 'All validations passed!'
        }
    }
}
```

## Status Badges

Add to your README.md:

### GitHub Actions
```markdown
![Validate](https://github.com/<owner>/<repo>/actions/workflows/validate.yml/badge.svg)
```

### Azure DevOps
```markdown
[![Build Status](https://dev.azure.com/<org>/<project>/_apis/build/status/<pipeline>?branchName=main)](https://dev.azure.com/<org>/<project>/_build/latest?definitionId=<id>)
```

## Best Practices

1. **Run validation on every PR** - Catch issues early
2. **Validate changed files only** in PRs for speed
3. **Full validation on main/develop** - Ensure integrity
4. **Cache Python dependencies** - Speed up builds
5. **Fail fast** - Stop on first validation error
6. **Report results clearly** - Use emojis and summaries

## Monitoring

Track validation metrics:
- ✅ Success rate
- ⏱️ Average validation time
- 📊 Files validated per run
- 🔍 Common failure patterns

## Troubleshooting

### Validation fails in CI but passes locally

**Cause**: Different Python/package versions

**Solution**:
```bash
# Use exact versions
pip install -e .[dev]
pip freeze > requirements-ci.txt
```

### Slow validation times

**Cause**: Validating all files on every PR

**Solution**: Use changed-files detection (see pre-commit.yml)

### False positives

**Cause**: Schema changes not synchronized

**Solution**: Update schemas first, then configs/examples
