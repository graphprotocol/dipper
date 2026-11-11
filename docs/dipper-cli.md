# Dipper Admin CLI

## Authentication

### Introduction

The Dipper Admin CLI supports multiple authentication mechanisms to securely manage your configuration and secrets. 

> [!IMPORTANT]
> Before using any authentication mechanism, ensure you understand and follow these security best practices.
>
> #### Security Best Practices
>
> 1. **Access Control**
>    - Store signing keys in dedicated 1Password vaults
>    - Use environment-specific vaults (development, staging, production)
>    - Regularly audit vault access permissions
>    - Implement the principle of least privilege for key access
>
> 2. **Secret Management**
>    - Never commit actual signing keys to version control
>    - Use descriptive item names in 1Password
>    - Regularly rotate signing keys
>    - Keep the 1Password CLI authenticated
>    - Avoid storing secrets in plain text files
>    - Use secure secret rotation procedures
>
> 3. **Configuration**
>    - Use template `.env` files with 1Password references
>    - Document the required 1Password items and fields
>    - Maintain separate configurations for different environments
>    - Keep configuration templates in version control
>    - Document security requirements and procedures

### Authentication Mechanisms

The CLI provides multiple ways to handle authentication and configuration, each with its own use cases and security considerations.

#### Environment Variables

All environment variables used by the CLI are prefixed with `DIPS_`. The main configuration-related environment variables are:

- `DIPS_SERVER_URL`: The URL of the DIPs gateway server
- `DIPS_SIGNING_KEY`: The secret key used to sign requests (in hex format)
- `DIPS_IPFS_URL`: The IPFS API that subgraph manifests are read from (defaults to `https://ipfs.thegraph.com`)

A command-line flag takes precedence over the matching environment variable. Environment variables can be used to avoid repeating common configuration:

```bash
# Set the server URL in the environment
export DIPS_SERVER_URL=https://admin.dips.example.com

# Run a command without specifying the server URL
dipper-cli init keygen --output .env.unencrypted
```

#### .env File

You can use a `.env` file to store the configuration of the `indexings` and `agreements` commands. They look for a `.env` file in the current directory only, never in the directories above it, and also take a custom path with the `--env-file` flag. A file named that way is read instead of the one in the current directory, so a setting it leaves out, such as a private `DIPS_IPFS_URL` kept only in the other file, falls back to your shell or to the default, not to that file:

```bash
dipper-cli indexings --env-file /path/to/.env <subcommand>
```

Each setting comes from the first of these that sets it: a command-line flag, a variable already set in your shell, then the file. The CLI prints a warning, naming the setting but never its value, whenever a flag or a shell variable overrides a setting in the file, whether you named it with `--env-file` or the CLI found it in the current directory. It also warns when the server URL and the signing key come from different places, naming both, so a variable left exported in your shell cannot quietly send a command to a different server. A flag or variable that repeats the file's value, even written differently, such as a URL without its trailing slash, raises no warning.

Only `DIPS_` settings are read from a file. Other variables in it are not passed on, so a proxy setting such as `HTTPS_PROXY`, which the manifest and networks registry downloads in `set-target-candidates` follow, has to be exported in your shell. The `init keygen` and `init placeholder` commands read no file at all: they take the server URL from `--server-url` or from `DIPS_SERVER_URL` in your shell.

#### CLI Flags

The `indexings` and `agreements` commands take these flags for authentication, before or after their subcommand; `init keygen` and `init placeholder` take only `--server-url`:

- `--server-url <URL>`: Specify the DIPs gateway server URL
- `--signing-key <KEY>`: Provide the signing key in hex format
- `--env-file <FILE>`: Specify a custom `.env` file path

### 1Password Integration

The CLI supports integration with 1Password for secure secret management. There are two main approaches to provide credentials using 1Password:

#### Using `op read` with Environment Variables or Flags

You can use the 1Password CLI (`op`) to read secrets directly into environment variables or CLI flags:

```bash
# Using environment variables
export DIPS_SIGNING_KEY=$(op read "op://mainnet/dips/signing-key-1")
dipper-cli <command>

# Using CLI flags
dipper-cli indexings --signing-key "$(op read "op://mainnet/dips/signing-key-1")" <subcommand>
```

This approach is useful for:
- One-off commands
- CI/CD pipelines
- Scripts where you need direct access to the secret value

#### Using `op inject` with Template Files

For more complex configurations, you can use 1Password's environment variable injection feature:

1. Create a template `.env.template` file with 1Password references:
```bash
# .env.template
DIPS_SERVER_URL=https://admin.dips.example.com
DIPS_SIGNING_KEY={{ op://mainnet/dips/signing-key-1 }}
```

2. Use `op inject` to write a copy of the template with the secrets filled in, then point the CLI at that copy:
```bash
op inject --in-file .env.template --out-file .env.injected
dipper-cli indexings --env-file .env.injected <subcommand>
```

3. The injected file holds the signing key in plain text, so delete it once you are done:
```bash
rm .env.injected
```

As with any `--env-file` file, a variable already set in your shell beats the one in `.env.injected`, and the CLI warns when it does; a `.env` file in the current directory is not read at all.

This approach is beneficial when:
- Managing multiple secrets
- Working with configuration files
- Sharing configuration templates without exposing secrets

## Initialization

The `init` command helps you bootstrap your DIPs Admin CLI configuration. It provides two subcommands for different use cases:

### Generating Keys

The `init keygen` command generates a new key pair for signing requests:

```bash
# Generate a new key pair and display it
dipper-cli init keygen 

# Generate a new key pair and save it to a .env file
dipper-cli init keygen --server-url https://admin.dips.example.com --output .env.unencrypted
```

This command will:
1. Generate a new random key pair
2. Display the public and private keys
3. Optionally save the configuration to a file if `--output` is specified

> [!IMPORTANT]
> When saving to a file, the command will display a security warning about storing private keys in environment files. 
> Consider using a secure credential manager instead.

### Creating 1Password Templates

The `init placeholder` command helps you set up configuration with 1Password integration:

```bash
# Create a template .env.template file with 1Password references
dipper-cli init placeholder op://mainnet/dips/signing-key-1 --server-url https://admin.dips.example.com --output .env.template
```

This command will:
1. Create a template configuration file with 1Password references
2. Show instructions for using `op inject` to fill in the secret
3. Save the template to the specified output file (defaults to `.env.template`)

The generated template will contain the 1Password reference wrapped in double curly braces, which is the format required by `op inject`:

```bash
# .env.template
DIPS_SERVER_URL=https://admin.dips.example.com
DIPS_SIGNING_KEY={{ op://mainnet/dips/signing-key-1 }}
```

To use the generated template, inject the secrets into a copy, run the command with it, then delete the copy, which holds the signing key in plain text:
```bash
op inject --in-file .env.template --out-file .env.injected
dipper-cli indexings --env-file .env.injected <subcommand>
rm .env.injected
```

## Development

This section provides information for developers who want to contribute to the Dipper CLI module.

### Building the CLI

To build the Dipper CLI, use the following Cargo commands:

```bash
# Build the CLI in debug mode
cargo build --bin dipper-cli

# Build the CLI in release mode (optimized)
cargo build --bin dipper-cli --release
```

The compiled binary will be available in:
- Debug build: `target/debug/dipper-cli`
- Release build: `target/release/dipper-cli`

### Running the CLI

You can build and run the CLI directly using Cargo:

```bash
# Run a specific command
cargo run --bin dipper-cli <command> [options]

# Examples:
cargo run --bin dipper-cli init keygen --server-url https://admin.dips.example.com
cargo run --bin dipper-cli indexings --env-file .env list
```

### Debugging the CLI

To enable debug logging, set the `RUST_LOG` environment variable. 

```bash
# Enable debug logging 
export RUST_LOG=debug

# Enable trace logging for maximum verbosity
export RUST_LOG=trace
```

The Dipper CLI uses the following module names for more specific logging:

```bash
# Enable debug logging for all Dipper CLI modules
export RUST_LOG=dipper_cli=debug

# Enable debug logging for specific modules
export RUST_LOG=dipper_cli::cmd=debug,dipper_cli::config=debug
```

You can combine this with running the CLI:

```bash
RUST_LOG=debug cargo run --bin dipper-cli <command>
```

For more detailed debugging with backtraces:

```bash
RUST_LOG=debug RUST_BACKTRACE=1 cargo run --bin dipper-cli <command>
```
