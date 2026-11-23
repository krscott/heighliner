set positional-arguments

default:
    just --list

# Build with Cargo.
build:
    cargo build --locked

# Run with Cargo, passing arguments to heighliner.
run *args:
    cargo run --locked -- "$@"

# Run tests with Cargo.
test:
    cargo test --locked

# Format files, or all tracked files when no files are provided.
format *files:
    @just _format write "$@"

# Check formatting without changing files.
format-check *files:
    @just _format check "$@"

[private]
_format mode *files:
    #!/usr/bin/env bash
    set -euo pipefail

    declare -a files=()
    mode="$1"
    shift

    if (($# == 0)); then
        mapfile -d '' -t files < <(git ls-files -z)
    else
        files=("$@")
    fi

    for file in "${files[@]}"; do
        if [ ! -f "$file" ]; then
            continue
        fi

        case "$file" in
            justfile|Justfile|.justfile|*/justfile|*/Justfile|*/.justfile)
                if [[ "$mode" == check ]]; then
                    just --unstable --fmt --check --justfile "$file"
                else
                    just --unstable --fmt --justfile "$file"
                fi
            ;;
            *.nix)
                if [[ "$mode" == check ]]; then
                    nixfmt --check "$file"
                else
                    nixfmt "$file"
                fi
            ;;
            *.rs)
                # skip_children: format only this file, not the modules it declares.
                if [[ "$mode" == check ]]; then
                    rustfmt --check --config skip_children=true "$file"
                else
                    rustfmt --config skip_children=true "$file"
                fi
            ;;
            *.sh|.githooks/*|*/.githooks/*)
                if [[ "$mode" == check ]]; then
                    shfmt -d -i 4 "$file"
                else
                    shfmt -w -i 4 "$file"
                fi
            ;;
        esac
    done

# Lint Rust code, tracked shell scripts, and Git hooks.
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    while IFS= read -r -d '' file; do
        [[ -f "$file" ]] || continue
        case "$file" in
            *.sh|.githooks/*) shellcheck "$file" ;;
        esac
    done < <(git ls-files -z)
    cargo clippy --locked --all-targets -- -D warnings

# Run non-mutating formatting, lint, and Nix checks.
check: format-check lint
    nix flake check -L --no-update-lock-file

# Run the NixOS VM tests, or the ones named; they need KVM.
vm-test *names:
    #!/usr/bin/env bash
    set -euo pipefail
    names=("$@")
    if (($# == 0)); then
        all=$(nix eval --raw .#vmTests --apply 'tests: toString (builtins.attrNames tests)')
        read -ra names <<<"$all"
    fi
    for name in "${names[@]}"; do
        nix build -L --no-link --no-update-lock-file ".#vmTests.$name"
    done

# Format staged files for git pre-commit.
pre-commit:
    #!/usr/bin/env bash
    set -euo pipefail

    declare -a staged_files
    declare -a format_files=()
    declare -A partially_staged
    file=''

    mapfile -d '' -t staged_files < <(
        git diff --cached --name-only --diff-filter=ACMR -z
    )

    while IFS= read -r -d '' file; do
        partially_staged["$file"]=1
    done < <(git diff --name-only -z)

    for file in "${staged_files[@]}"; do
        if [[ -v partially_staged["$file"] ]]; then
            continue
        fi

        format_files+=("$file")
    done

    if ((${#format_files[@]} == 0)); then
        exit 0
    fi

    just format "${format_files[@]}"
    git add -- "${format_files[@]}"

# Install a pre-commit hook that formats git-changed files.
install-hooks:
    git config core.hooksPath .githooks
