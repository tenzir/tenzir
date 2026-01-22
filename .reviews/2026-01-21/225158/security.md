# Security Review

## Summary

This feature adds AWS IAM authentication support across multiple operators (SQS, S3, Kafka). The implementation demonstrates good security practices with proper secret handling through the secret resolution framework, validation of credential pairs, and protection against credential conflicts. However, there are several security concerns around credential logging exposure, injection vulnerabilities in test scripts, and incomplete error handling that could leak sensitive information.

## Findings

### P1 · Credentials logged in verbose mode · 85%

- **File**: `plugins/sqs/include/operator.hpp:67`
- **Severity**: P1 — Critical
- **Confidence**: 85%
- **Issue**: AWS credentials may be logged via `TENZIR_VERBOSE` in the SQS operator constructor. While the code uses `creds->access_key_id` and `creds->secret_access_key`, if these values are logged or printed in debug/verbose output, it creates a credential exposure risk.
- **Suggestion**: Audit all TENZIR_VERBOSE and TENZIR_DEBUG statements to ensure AWS credentials are never logged, even partially. Consider masking credential values in any debugging output.

### P1 · Hardcoded AWS credentials in test script · 95%

- **File**: `scripts/setup-sqs-test-credentials.sh:178-184`
- **Severity**: P1 — Critical
- **Confidence**: 95%
- **Issue**: Script prints AWS credentials in plaintext to stdout, including access key ID and secret access key. This creates multiple risks: credentials in terminal history, logs, screenshots, or shared terminal sessions.
- **Suggestion**: Use AWS credential files or environment variables exclusively. If credentials must be displayed, require an explicit `--show-credentials` flag and warn users about security implications.

### P2 · Command injection vulnerability in test script · 90%

- **File**: `scripts/setup-sqs-test-credentials.sh:148-152`
- **Severity**: P2 — Important
- **Confidence**: 90%
- **Issue**: The script iterates over `$EXISTING_KEYS` without proper quoting in the loop, which could lead to word splitting or command injection if an attacker could manipulate the access key ID format (unlikely but possible).
- **Suggestion**: Quote variables properly: `for key in "$EXISTING_KEYS"` or better yet use array iteration with proper quoting.

### P2 · Incomplete validation of secret resolution errors · 82%

- **File**: `libtenzir/src/aws_iam.cpp:106-122`
- **Severity**: P2 — Important
- **Confidence**: 82%
- **Issue**: The `make_secret_requests` function creates secret requests but doesn't validate whether secret resolution will succeed. If a secret reference is invalid, the error might not surface until runtime in the operator execution, potentially after partial initialization.
- **Suggestion**: Consider adding validation in `from_record` to check that secrets exist before deferring resolution, or ensure error handling in operators properly cleans up partial state.

### P2 · Session token stored in memory without explicit zeroing · 80%

- **File**: `libtenzir/include/tenzir/aws_iam.hpp:27-28`, `plugins/kafka/include/kafka/configuration.hpp:39`
- **Severity**: P2 — Important
- **Confidence**: 80%
- **Issue**: Resolved credentials (including session tokens) are stored in `std::string` objects that are not explicitly zeroed when destroyed. This could leave sensitive data in memory longer than necessary, increasing exposure risk in case of memory dumps or debugging.
- **Suggestion**: Consider using a secure string implementation that zeros memory on destruction, or explicitly zero credential strings when they're no longer needed.

### P3 · Empty session token handled inconsistently · 81%

- **File**: `plugins/kafka/src/configuration.cpp:163`
- **Severity**: P3 — Minor
- **Confidence**: 81%
- **Issue**: When creating explicit credentials provider, the session token is passed directly without checking if it's empty. AWS SDK may interpret empty strings differently than absent tokens, potentially causing authentication failures.
- **Suggestion**: Check if `creds_->session_token.empty()` and pass an empty string explicitly or handle as optional parameter to match AWS SDK expectations.

### P3 · AWS region validation missing · 85%

- **File**: `libtenzir/src/aws_iam.cpp:73`, `plugins/kafka/src/configuration.cpp:96`
- **Severity**: P3 — Minor
- **Confidence**: 85%
- **Issue**: Region strings are validated for non-empty but not for valid AWS region format (e.g., "us-east-1"). Invalid regions could lead to confusing authentication failures or requests to unintended endpoints.
- **Suggestion**: Add regex validation for AWS region format: `^[a-z]{2}-[a-z]+-\d+$` or maintain a list of valid regions.

### P3 · Error messages may expose credential metadata · 83%

- **File**: `plugins/sqs/include/operator.hpp:152-156`, similar in S3 and Kafka
- **Severity**: P3 — Minor
- **Confidence**: 83%
- **Issue**: Error messages include AWS SDK error details that may contain metadata about credentials (e.g., "invalid access key format", partial key IDs). While not directly exposing credentials, this provides information to attackers.
- **Suggestion**: Sanitize AWS SDK error messages to remove any credential-related metadata before including in diagnostics. Log full details only in debug builds.

### P3 · Role ARN not validated · 82%

- **File**: `libtenzir/src/aws_iam.cpp:75`
- **Severity**: P3 — Minor
- **Confidence**: 82%
- **Issue**: IAM role ARNs are accepted as any non-empty string without format validation. Invalid ARNs (e.g., typos) will fail at runtime with potentially confusing errors.
- **Suggestion**: Validate ARN format: `^arn:aws:iam::\d{12}:role/[a-zA-Z0-9+=,.@_-]+$` to catch errors early.

### P3 · External ID not validated · 80%

- **File**: `libtenzir/src/aws_iam.cpp:77`
- **Severity**: P3 — Minor
- **Confidence**: 80%
- **Issue**: External IDs are accepted without validation. AWS external IDs have specific requirements (2-1224 characters, specific character set) that aren't enforced.
- **Suggestion**: Validate external ID format according to AWS requirements to prevent runtime failures.

## Positive Observations

1. **Strong secret handling architecture**: The implementation properly uses the secret resolution framework with `secret_request` patterns, avoiding direct credential exposure in most code paths.

2. **Credential pair validation**: The code correctly validates that `access_key_id` and `secret_access_key` must be specified together (XOR check), preventing partial credential configurations.

3. **Authentication method conflict detection**: Multiple authentication methods are properly detected and rejected with clear error messages, preventing confusing misconfigurations.

4. **Secure credential provider pattern**: For Kafka MSK, the implementation uses AWS SDK credential providers with automatic refresh rather than storing long-lived credentials.

5. **Profile isolation**: AWS CLI profiles are properly isolated from explicit credentials, preventing credential conflicts.

6. **Validation of session token dependencies**: The code correctly validates that session tokens require access key IDs.

7. **Role assumption with STS**: Uses AWS STS for role assumption with proper credential provider chains, following AWS best practices.

8. **Environment variable fallback**: Properly implements AWS credential chain fallback to environment variables while maintaining explicit control when needed.
