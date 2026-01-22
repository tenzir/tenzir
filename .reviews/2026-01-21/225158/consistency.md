# Consistency Review

## Summary

This review examines the AWS IAM authentication feature added to Tenzir's SQS, S3, and Kafka operators for style, naming, patterns, and conventions. The implementation demonstrates strong consistency across all modified files with only minor naming discrepancies and potential improvements.

## Findings

### P3 · Inconsistent field naming: `role` vs `assume_role` · 88%

- **File**: Multiple files (`libtenzir/src/aws_iam.cpp:75`, `plugins/kafka/src/configuration.cpp:97`, `plugins/s3/builtins/from_s3.cpp:40`, `plugins/sqs/include/operator.hpp:254`)
- **Severity**: P3 — Minor
- **Confidence**: 88%
- **Issue**: The core `aws_iam_options` struct in `libtenzir/include/tenzir/aws_iam.hpp` uses `role` as the field name (line 40), but the changelog and user-facing API document it as `assume_role` (changelog line 46). The Kafka plugin-specific version duplicates this struct and also uses `role`. This creates cognitive dissonance between documentation and implementation.
- **Suggestion**: Rename the struct field from `role` to `assume_role` in the core struct for consistency with the documented API. Update all references accordingly. This improves code clarity since `assume_role` semantically matches "IAM role ARN to assume."

### P3 · Inconsistent nesting of aws_iam_options structures · 85%

- **File**: `libtenzir/include/tenzir/aws_iam.hpp` vs `plugins/kafka/include/kafka/configuration.hpp`
- **Severity**: P3 — Minor
- **Confidence**: 85%
- **Issue**: There are two separate definitions of `aws_iam_options`: one in the core library (lines 34-90) and another in the Kafka plugin (lines 42-76). Both contain nearly identical code. The Kafka version lacks the `profile` field and `has_explicit_credentials()` method present in the core version. This duplication violates DRY principles and creates maintenance burden.
- **Suggestion**: Use the core library's `aws_iam_options` struct directly in the Kafka plugin instead of duplicating it. If the Kafka version needs different behavior, document why and consider making the core version more flexible through optional fields or specialized accessors.

### P3 · Missing `profile` field in Kafka aws_iam_options · 83%

- **File**: `plugins/kafka/include/kafka/configuration.hpp:42-76` and `plugins/kafka/src/configuration.cpp:35-119`
- **Severity**: P3 — Minor
- **Confidence**: 83%
- **Issue**: The Kafka plugin's `aws_iam_options` struct lacks the `profile` field that exists in the core library version. The Kafka implementation has no way to specify an AWS CLI profile, while the core supports it. This is inconsistent with the changelog which does not mention profile support limitations for Kafka.
- **Suggestion**: Add the `profile` field to the Kafka `aws_iam_options` struct and update the `from_record` parser and validation logic to support it, keeping it consistent with the core implementation.

### P3 · Inconsistent validation: `profile` XOR credentials check missing in Kafka · 82%

- **File**: `libtenzir/src/aws_iam.cpp:97-101` vs `plugins/kafka/src/configuration.cpp` (lines 35-119)
- **Severity**: P3 — Minor
- **Confidence**: 82%
- **Issue**: The core library validates that `profile` cannot be used together with explicit credentials (lines 97-101 in aws_iam.cpp). The Kafka plugin's `from_record` implementation (configuration.cpp lines 35-119) lacks this validation, even though the kafka plugin has explicit credentials support.
- **Suggestion**: Add validation in the Kafka `configuration::aws_iam_options::from_record` method to prevent using `profile` with explicit credentials, matching the core library's behavior.

### P3 · Inconsistent abbreviation: `ext_id` vs `external_id` · 80%

- **File**: Multiple files (struct fields use `ext_id`: `aws_iam.hpp:44`, `configuration.hpp:46`, but API documents `external_id`: changelog line 23)
- **Severity**: P3 — Minor
- **Confidence**: 80%
- **Issue**: The documentation and changelog refer to `external_id` but the struct field is abbreviated to `ext_id`. While abbreviations are common in code, this particular one creates a disconnect between what users read in documentation and what they must type in code.
- **Suggestion**: Consider keeping the full name `external_id` in struct fields for clarity and consistency with documentation, reducing user confusion. Alternatively, update all documentation to use `ext_id` consistently.

### P4 · Minor: Inconsistent comment placement in Kafka configuration · 70%

- **File**: `plugins/kafka/src/configuration.cpp:51-52` vs `libtenzir/src/aws_iam.cpp` (no equivalent comment)
- **Severity**: P4 — Trivial
- **Confidence**: 70%
- **Issue**: The Kafka implementation adds a requirement comment: "region' must be specified when using IAM" (lines 51-52), but the core library version does not enforce this requirement in validation. Kafka MSK requires region while S3/SQS may not always require it.
- **Suggestion**: This difference is acceptable since Kafka has different requirements. However, document why Kafka enforces region when the core library doesn't.

### P4 · Minor: Lambda parameter naming conventions · 65%

- **File**: `plugins/kafka/include/kafka/operator.hpp:113` and similar locations
- **Severity**: P4 — Trivial
- **Confidence**: 65%
- **Issue**: Some lambda functions use abbreviated parameter names (`auto&& x` in `offset_parser()`, line 113-132) while inline functions in the same file use more descriptive names. This is minor but inconsistent within the same file.
- **Suggestion**: Consider standardizing lambda parameter names to match the descriptive style used elsewhere in the file.

## Positive Observations

1. **Strong pattern consistency**: The implementation follows a consistent pattern across all three operators (SQS, S3, Kafka) for handling AWS credentials. Each uses nearly identical secret resolution logic via `make_secret_requests()`.

2. **Comprehensive validation**: All credential parsing includes thorough validation of credential combinations (XOR checks, mutual exclusivity), preventing invalid configurations from reaching runtime.

3. **Well-documented API**: The changelog provides clear examples of all four credential methods supported, making the feature easy to understand for users.

4. **Correct use of secret types**: All sensitive values (credentials, tokens) properly use the `secret` type with support for `secret()` references, following Tenzir's security conventions.

5. **Consistent error handling**: All implementations emit diagnostics with proper context (location, notes, hints) making errors actionable for users.

6. **Proper inspect() implementations**: All structs include correct `friend auto inspect()` methods for serialization/inspection, maintaining consistency with Tenzir patterns.

7. **Consistent operator lifecycle**: All operators follow the same pattern: resolve secrets → create configuration → establish connection, with proper diagnostic emission on failure.

8. **Nix/build consistency**: The nix configuration correctly adds SQS to the aws-sdk-cpp builds alongside existing services, maintaining proper dependency management.
