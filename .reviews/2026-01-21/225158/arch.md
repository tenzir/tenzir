# Architecture Review

## Summary

This change introduces AWS IAM authentication support across multiple operators (SQS, S3, Kafka). The implementation creates a shared `aws_iam_options` struct in `libtenzir` for common authentication configuration, but each plugin maintains its own duplicate implementation with subtle differences. The API design for the `aws_iam` option is clean and intuitive for end users, but the internal architecture suffers from significant code duplication and inconsistent validation logic.

## Findings

### P2 · Duplicate aws_iam_options implementations across plugins · 92%

- **File**: `/home/tobim/t/tenzir/load_sqs-credentials/libtenzir/include/tenzir/aws_iam.hpp:34-90`, `/home/tobim/t/tenzir/load_sqs-credentials/plugins/kafka/include/kafka/configuration.hpp:42-76`
- **Severity**: P2 — Important
- **Confidence**: 92%
- **Issue**: There are two nearly identical `aws_iam_options` structs: one in `libtenzir/include/tenzir/aws_iam.hpp` and another in `plugins/kafka/include/kafka/configuration.hpp`. Both define the same fields (region, role, session_name, ext_id, access_key_id, secret_access_key, session_token) and the same `from_record` parsing logic, but with subtle differences:
  - The core library version has `region` as `optional<string>` while Kafka's version has it as a required `string`
  - The core library supports `profile` option while Kafka does not
  - Validation logic is duplicated in both `libtenzir/src/aws_iam.cpp:18-104` and `plugins/kafka/src/configuration.cpp:35-118`
- **Suggestion**: Refactor to use a single shared implementation. The Kafka plugin should use the core `aws_iam_options` and add Kafka-specific validation (e.g., region is required for MSK). This would be achieved by having Kafka wrap the core struct and add its own validation layer.

### P2 · Inconsistent validation between S3 plugins · 88%

- **File**: `/home/tobim/t/tenzir/load_sqs-credentials/plugins/s3/builtins/from_s3.cpp:35-111`, `/home/tobim/t/tenzir/load_sqs-credentials/plugins/s3/builtins/plugins.cpp:77-133`
- **Severity**: P2 — Important
- **Confidence**: 88%
- **Issue**: There are two different S3 operator implementations with different validation logic:
  - `from_s3.cpp` (used by `from_s3`) has extensive validation in `from_s3_args::validate()` including checks for conflicting authentication methods (lines 35-111)
  - `plugins.cpp` (used by `load_s3`/`save_s3`) has simpler validation (lines 91-117) with different constraints
  - `from_s3.cpp` explicitly blocks explicit credentials + role assumption (line 103-110), while `plugins/s3/include/operator.hpp:81-91` allows it via `get_options()`
- **Suggestion**: Consolidate validation logic. If explicit credentials + role assumption is intentionally unsupported for S3, enforce this consistently across all S3-related operators. Consider creating a shared validation helper function.

### P3 · S3 operators mix legacy and new aws_iam patterns · 85%

- **File**: `/home/tobim/t/tenzir/load_sqs-credentials/plugins/s3/builtins/from_s3.cpp:26-33`, `/home/tobim/t/tenzir/load_sqs-credentials/plugins/s3/include/operator.hpp:54-68`
- **Severity**: P3 — Minor
- **Confidence**: 85%
- **Issue**: The S3 plugin supports both legacy individual credential options (`access_key`, `secret_key`, `session_token`, `role`, `external_id`) and the new unified `aws_iam` option. While backward compatibility is good, the code complexity is increased by supporting both patterns:
  - `from_s3_args` has fields for both patterns (lines 26-33)
  - `s3_args` similarly has `config`, `role`, and `aws_iam` (lines 54-68)
  - Complex validation in `from_s3_args::validate()` must handle all combinations
- **Suggestion**: Consider deprecating the individual credential options in favor of `aws_iam` to simplify the codebase. Document the migration path and emit deprecation warnings when legacy options are used.

### P3 · SQS operator lacks profile support unlike core aws_iam_options · 82%

- **File**: `/home/tobim/t/tenzir/load_sqs-credentials/plugins/sqs/include/operator.hpp:51-62`
- **Severity**: P3 — Minor
- **Confidence**: 82%
- **Issue**: The SQS `sqs_queue` constructor accepts a `profile` parameter (line 55), and the `sqs_loader` passes `args_.aws->profile` (line 253), indicating profile support. However, the changelog documentation and the overall design suggest profile support should be uniform. The core `aws_iam_options` supports `profile`, but the validation in each plugin may handle it differently.
- **Suggestion**: Ensure consistent profile support across all operators. The feature should be documented in the changelog for SQS alongside the other operators.

### P3 · AWS SDK initialization/shutdown in Kafka callback is inefficient · 85%

- **File**: `/home/tobim/t/tenzir/load_sqs-credentials/plugins/kafka/src/configuration.cpp:139-145`
- **Severity**: P3 — Minor
- **Confidence**: 85%
- **Issue**: The `oauthbearer_token_refresh_cb` callback calls `Aws::InitAPI({})` and `Aws::ShutdownAPI({})` on every token refresh (lines 142-145). This is called periodically (every 900 seconds by default) and is wasteful. The AWS SDK documentation recommends initializing once at application startup and shutting down at application exit.
- **Suggestion**: Move AWS SDK initialization to a shared location that ensures single initialization/shutdown per process. The SQS plugin appears to rely on global initialization elsewhere, which should be unified.

### P3 · Missing error handling after Arrow S3 filesystem creation in s3_saver · 83%

- **File**: `/home/tobim/t/tenzir/load_sqs-credentials/plugins/s3/include/operator.hpp:223-237`
- **Severity**: P3 — Minor
- **Confidence**: 83%
- **Issue**: In `s3_saver::operator()`, after an error is emitted for failed filesystem creation (lines 224-230) or failed file info retrieval (lines 231-237), execution continues without returning early. The subsequent operations will likely fail with null pointer dereferences or invalid states.
- **Suggestion**: Add `co_return;` after each diagnostic error emission in the s3_saver coroutine to prevent undefined behavior.

### P4 · Unused default_message variable in to_kafka_operator · 90%

- **File**: `/home/tobim/t/tenzir/load_sqs-credentials/plugins/kafka/builtins/to_kafka.cpp:94-99`
- **Severity**: P4 — Trivial
- **Confidence**: 90%
- **Issue**: The `default_message` variable is defined (lines 94-99) but never used. The operator already has `args_.message` initialized with a default value in the struct definition (lines 27-32).
- **Suggestion**: Remove the unused `default_message` variable to clean up the code.

### P4 · Changelog claims support for profile but documentation incomplete · 80%

- **File**: `/home/tobim/t/tenzir/load_sqs-credentials/changelog/unreleased/aws-iam-authentication-for-load-sqs-save-sqs-from-s3-to-s3-from-kafka-and-to-kaf.md:17`
- **Severity**: P4 — Trivial
- **Confidence**: 80%
- **Issue**: The changelog mentions `profile: AWS CLI profile name for credential resolution` as a supported field (line 17), but there is no example showing profile usage in the documentation examples. Given that profile support is a common AWS credential resolution method, an example would be helpful.
- **Suggestion**: Add an example showing profile-based authentication in the changelog.

## Positive Observations

1. **Clean user-facing API**: The `aws_iam` option provides a clean, unified interface for AWS authentication across all supported operators. The record-based configuration with `secret()` support for sensitive values is well-designed.

2. **Consistent secret resolution pattern**: The implementation correctly uses `resolve_secrets_must_yield` across all operators for handling credential secrets, which is important for security.

3. **Good validation messages**: The diagnostic error messages are clear and specific, helping users understand configuration issues (e.g., "access_key_id and secret_access_key must be specified together").

4. **Role assumption support with automatic refresh**: The implementation uses `STSAssumeRoleCredentialsProvider` which handles automatic credential refresh for assumed roles, avoiding credential expiration issues during long-running operations.

5. **EC2 metadata service optimization**: The `is_ec2_instance()` check in `tenzir.cpp` (lines 64-78) is a thoughtful optimization that prevents long timeouts when running outside of EC2 environments.
