# UX Review: AWS IAM Authentication Feature

## Summary

The AWS IAM authentication implementation provides clear documentation and consistent error handling across operators. However, there are opportunities to improve discoverability of credential options, clarify error messages around credential validation, and ensure consistent naming conventions across the codebase.

## Findings

### P3 · Inconsistent credential option field naming across codebases · 87%

- **File**: `libtenzir/include/tenzir/aws_iam.hpp:40-44`, `plugins/kafka/include/kafka/configuration.hpp:42-46`, `plugins/s3/include/operator.hpp:54-59`
- **Severity**: P3 — Minor
- **Confidence**: 87%
- **Issue**: The field name for role assumption differs between modules:
  - `libtenzir`: `role` and `ext_id`
  - `plugins/kafka`: `role` and `ext_id`
  - `plugins/s3`: Uses raw fields without clear naming pattern
  The changelog documents `assume_role` and `external_id`, but the implementation uses abbreviated field names like `ext_id`, creating a disconnect between user-facing documentation and internal naming.
- **Suggestion**: Standardize on either full names (`assume_role`, `external_id`) or abbreviated names (`role`, `ext_id`) consistently across all modules. Add inline documentation explaining the abbreviated names if kept.

### P3 · Missing documentation for credential priority/precedence · 85%

- **File**: `changelog/unreleased/aws-iam-authentication-for-load-sqs-save-sqs-from-s3-to-s3-from-kafka-and-to-kaf.md`, `libtenzir/src/aws_iam.cpp:97-102`
- **Severity**: P3 — Minor
- **Confidence**: 85%
- **Issue**: The code validates that `profile` cannot be used with explicit credentials (lines 97-102), but this constraint is not documented in the changelog. Users encountering this error won't understand why the combination is invalid or what alternatives exist.
- **Suggestion**: Add a note in the changelog explaining credential precedence: explicit credentials take priority over profiles, and users should choose one method. Add to help documentation which method is recommended for different scenarios (local dev vs CI/CD vs production).

### P3 · Unclear error message for conflicting credential methods · 83%

- **File**: `plugins/s3/builtins/from_s3.cpp:47-82`
- **Severity**: P3 — Minor
- **Confidence**: 83%
- **Issue**: Error message states "conflicting authentication methods specified" but doesn't clearly indicate which methods were detected or what the valid options are. Users see the error without understanding which options they selected that caused the conflict.
- **Suggestion**: Improve error message to explicitly name detected methods: "conflicting authentication methods: cannot use both `anonymous` and `role` together. Choose one of: anonymous, explicit credentials (access_key/secret_key), aws_iam, or role assumption."

### P4 · Default credential chain not explicitly mentioned in error cases · 80%

- **File**: `plugins/kafka/builtins/from_kafka.cpp:346-352`, `plugins/sqs/include/operator.hpp:92-105`
- **Severity**: P4 — Trivial
- **Confidence**: 80%
- **Issue**: Documentation mentions the AWS SDK's default credential chain is used when no `aws_iam` option is specified, but error messages don't clarify that users can rely on environment variables or AWS configuration files as fallback options.
- **Suggestion**: Add note to error messages when credential resolution fails: "No credentials provided. The AWS SDK will attempt to use default credential chain (environment variables, AWS config files, IAM roles). If this fails, configure `aws_iam` explicitly."

### P3 · Inconsistent validation logic for credential dependencies · 82%

- **File**: `libtenzir/src/aws_iam.cpp:81-102`, `plugins/s3/builtins/from_s3.cpp:73-95`
- **Severity**: P3 — Minor
- **Confidence**: 82%
- **Issue**: Validation logic is duplicated across modules with slight differences:
  - `aws_iam.cpp` validates `access_key_id` and `secret_access_key` must be paired (line 82)
  - `from_s3.cpp` performs similar validation (lines 83-95)
  This duplication makes it harder to maintain consistency and increases risk of divergent behavior.
- **Suggestion**: Centralize credential validation in `aws_iam_options::from_record()` so all modules benefit from the same validation rules. Document which validations are performed so operators understand what is allowed.

### P2 · No guidance on secret reference syntax in documentation · 88%

- **File**: `changelog/unreleased/aws-iam-authentication-for-load-sqs-save-sqs-from-s3-to-s3-from-kafka-and-to-kaf.md:18-19`
- **Severity**: P2 — Important
- **Confidence**: 88%
- **Issue**: The changelog shows `secret()` references in examples but doesn't explain how to create or populate those secrets, or what systems are supported for secret storage. Users won't know how to properly use this feature without additional documentation.
- **Suggestion**: Add explicit documentation linking to secret management guidance. Example: "To use credentials stored in Tenzir secrets, use `secret("aws-key")`. See [Secret Management](link) for how to configure secret storage backends."

### P3 · Credential resolution error messages lack actionable guidance · 84%

- **File**: `plugins/kafka/builtins/from_kafka.cpp:59-77`, `plugins/sqs/include/operator.hpp:92-105`
- **Severity**: P3 — Minor
- **Confidence**: 84%
- **Issue**: When credential resolution fails during secret resolution (`co_yield ctrl.resolve_secrets_must_yield()`), the error messages don't explain what went wrong or how to troubleshoot. Users see generic failure messages without clear remediation steps.
- **Suggestion**: Add specific error handling with context: "Failed to resolve secret 'aws-key' for access_key_id. Verify the secret name is correct and the secret storage backend is configured. See [Troubleshooting Secrets](link)."

### P4 · Role assumption syntax inconsistency in documentation · 79%

- **File**: `changelog/unreleased/aws-iam-authentication-for-load-sqs-save-sqs-from-s3-to-s3-from-kafka-and-to-kaf.md:43-51`
- **Severity**: P4 — Trivial
- **Confidence**: 79%
- **Issue**: The example shows `assume_role: "arn:..."` but internal field is named `role`. While field mapping is handled during parsing, this minor disconnect could cause confusion if users reference internal code.
- **Suggestion**: Add inline comment in example: `assume_role: "arn:..." # Role ARN to assume`. Document the complete ARN format expected for clarity.

### P3 · Missing validation error for empty AWS region in Kafka MSK scenarios · 81%

- **File**: `plugins/kafka/include/kafka/configuration.hpp:42-46`, `plugins/kafka/builtins/from_kafka.cpp:346-352`
- **Severity**: P3 — Minor
- **Confidence**: 81%
- **Issue**: Changelog states region is "required for Kafka MSK" but code doesn't validate this requirement. If users forget to specify region for MSK, they'll get a generic failure downstream rather than a clear validation error at parse time.
- **Suggestion**: Add validation in `configuration::aws_iam_options::from_record()` to check that region is provided when using Kafka MSK (detect MSK via bootstrap.servers configuration). Provide clear error: "Region is required for AWS MSK authentication. Add `region: \"us-east-1\"` to aws_iam config."

### P3 · Session name guidance missing from documentation · 83%

- **File**: `changelog/unreleased/aws-iam-authentication-for-load-sqs-save-sqs-from-s3-to-s3-from-kafka-and-to-kaf.md:20-22`
- **Severity**: P3 — Minor
- **Confidence**: 83%
- **Issue**: The `session_name` field for role assumption is documented but there's no guidance on what value to use or what naming conventions are expected. Users may provide invalid session names.
- **Suggestion**: Add documentation explaining session_name requirements: "Session name for role assumption (optional, defaults to timestamp). Must be alphanumeric with hyphens/underscores, 2-64 characters. Used for audit trail and CloudTrail logs."

### P3 · Test setup script produces credentials in shell output · 86%

- **File**: `scripts/setup-sqs-test-credentials.sh:178-258`
- **Severity**: P3 — Minor
- **Confidence**: 86%
- **Issue**: The script outputs AWS credentials directly in the terminal (lines 178-179, 249-250). While useful for development, this creates security risk if terminal history is not cleared or if output is captured in logs. No warning about this.
- **Suggestion**: Add clear security warning at the top and before credential output: "WARNING: Credentials will be displayed below. Ensure terminal history is cleared after viewing. Consider using `export HISTCONTROL=ignorespace` before running, then prepending space to this command."

## Positive Observations

1. **Comprehensive validation logic** - The `aws_iam_options::from_record()` implementation includes thorough validation of credential combinations, preventing invalid configurations early.

2. **Clear changelog examples** - The unreleased changelog provides multiple concrete examples showing different credential configuration patterns, making it easy to understand supported approaches.

3. **Consistent error handling patterns** - Error diagnostic functions across Kafka and S3 operators follow similar patterns for reporting parsing and configuration errors.

4. **Thoughtful default values** - Default behavior using AWS SDK's credential chain provides good developer experience for containerized/cloud environments.

5. **Defensive secret validation** - Code validates that credentials requiring secrets (access_key_id, secret_access_key) are paired, preventing silent failures.

6. **Setup script documentation** - The test setup script includes four clearly labeled options for different authentication scenarios, helping users understand available approaches.
