# Display available commands and their descriptions (default target)
default:
    @just --list

# Format Rust code (cargo fmt)
fmt:
    cargo +nightly fmt --all

# Check Rust code formatting (cargo fmt --check)
fmt-check:
    cargo +nightly fmt --all -- --check

# Check Rust code (cargo clippy)
check *EXTRA_FLAGS:
    cargo clippy {{EXTRA_FLAGS}} -- -D warnings --force-warn deprecated --force-warn dead-code

# Run Rust unit tests
test-unit *EXTRA_FLAGS:
    cargo test {{EXTRA_FLAGS}} 'tests::' -- --skip 'tests::it_'

# Build the dipper-service image locally. Tags as ghcr.io/edgeandnode/dipper-service:local
# by default; override with `DIPPER_TAG=foo just build-image`. Used by local-network, which
# references the image via DIPPER_VERSION in its .env.
build-image:
    docker compose build

# Run Rust integration tests
test-it *EXTRA_FLAGS:
    @printf "\e[1;92m[1/2]\e[0m Running in-tree integration tests...\n"
    cargo test {{EXTRA_FLAGS}} 'tests::it_'
    @printf "\e[1;92m[2/2]\e[0m Running public API integration tests...\n"
    cargo test {{EXTRA_FLAGS}} --test '*'

# Create symbolic links for migration files
create-migrations-links:
    #!/usr/bin/env bash
    set -euo pipefail

    # The project root directory
    ROOT_DIR="$(pwd)"

    TARGET_DIRS=(
        "${ROOT_DIR}/migrations"
        "${ROOT_DIR}/bin/dipper-service/migrations"
    )
    SRC_DIRS=(
        "${ROOT_DIR}/dipper-pgmq/migrations"
        "${ROOT_DIR}/dipper-pgregistry/migrations"
    )

    # Create symbolic links for each target directory
    for TARGET_DIR in "${TARGET_DIRS[@]}"; do
        # Check if the target directory exists
        if [ ! -d "$TARGET_DIR" ]; then
            mkdir -p "$TARGET_DIR"
        fi

        # Create symbolic links from the source directories
        for SRC_DIR in "${SRC_DIRS[@]}"; do
            # If the source directory does not exist (or it's empty), skip it
            if [ ! -d "$SRC_DIR" ] || [ -z "$(ls -A "$SRC_DIR" 2>/dev/null || true)" ]; then
                continue
            fi

            # Create symbolic links relative to the target directory
            ln --symbolic --relative --force "$SRC_DIR"/* "$TARGET_DIR"/
        done

        echo "Symbolic links created in '$TARGET_DIR'"
    done

# Install Git hooks
install-git-hooks:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Check if pre-commit is installed
    if ! command -v "pre-commit" &> /dev/null; then
        >&2 echo "=============================================================="
        >&2 echo "Required command 'pre-commit' not available"
        >&2 echo ""
        >&2 echo "Please install pre-commit using your preferred package manager"
        >&2 echo "  pip install pre-commit"
        >&2 echo "  pacman -S pre-commit"
        >&2 echo "  apt-get install pre-commit"
        >&2 echo "  brew install pre-commit"
        >&2 echo "=============================================================="
        exit 1
    fi

    # Install the pre-commit hooks
    pre-commit install --config .github/pre-commit-config.yaml

# Remove Git hooks
remove-git-hooks:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Check if pre-commit is installed
    if ! command -v "pre-commit" &> /dev/null; then
        >&2 echo "=============================================================="
        >&2 echo "Required command 'pre-commit' not available"
        >&2 echo ""
        >&2 echo "Please install pre-commit using your preferred package manager"
        >&2 echo "  pip install pre-commit"
        >&2 echo "  pacman -S pre-commit"
        >&2 echo "  apt-get install pre-commit"
        >&2 echo "  brew install pre-commit"
        >&2 echo "=============================================================="
        exit 1
    fi

    # Remove the pre-commit hooks
    pre-commit uninstall --config .github/pre-commit-config.yaml

# Generate event protobuf bindings (RUSTFLAGS="--cfg gen_event_proto" cargo check)
[group: 'codegen']
gen-event-protos:
    RUSTFLAGS="--cfg gen_event_proto" cargo check -p dipper-producer

alias gen-indexing-agreement-events-proto := gen-event-protos