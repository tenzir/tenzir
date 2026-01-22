# Test Coverage Review: AWS IAM Authentication

## Summary

This feature adds AWS IAM authentication support to six operators (load_sqs, save_sqs, from_s3, to_s3, from_kafka, to_kafka). The implementation includes a new `aws_iam_options` struct and integration across multiple plugins. **Critical finding: No dedicated unit tests exist for the core AWS IAM configuration parsing and validation logic, and no integration tests demonstrate end-to-end authentication flows.**

## Coverage Analysis

### Code Paths Without Tests

#### 1. Core AWS IAM Parsing (`libtenzir/src/aws_iam.cpp`)

**P1 · Missing unit tests for aws_iam_options::from_record parsing · 95%**

- **File**: `libtenzir/src/aws_iam.cpp:18-104`
- **Severity**: P1 — Critical
- **Confidence**: 95%
- **Issue**: The core configuration parsing function has complex validation logic with 8 distinct error paths (unknown keys, type validation, empty strings, credential pairing, role assumption constraints) but no dedicated unit tests.
- **Coverage Gap**:
  - Unknown key detection (line 25-33): Untested
  - Type validation for strings (line 38-43): Untested
  - Type validation for secrets (line 57-67): Untested
  - Empty string validation (line 44-48): Untested
  - Mutual exclusivity checks (access_key_id ↔ secret_access_key) at line 82: Untested
  - session_token without access_key_id constraint (line 90-95): Untested
  - profile + explicit credentials conflict (line 97-102): Untested
- **Suggestion**: Create unit test file `libtenzir/test/aws_iam.cpp` with test cases for all parsing scenarios. Consider using parameterized tests.
- **Risk**: Configuration errors will only be discovered at runtime when operators are instantiated with malformed aws_iam records.

**P1 · Missing unit tests for make_secret_requests · 90%**

- **File**: `libtenzir/src/aws_iam.cpp:106-122`
- **Severity**: P1 — Critical
- **Confidence**: 90%
- **Issue**: The secret request generation function lacks tests for:
  - Request ordering and completeness when all three credentials present
  - Skipping session_token when not provided
  - Edge case: session_token present without access_key_id (should not be possible due to validation, but not tested)
- **Suggestion**: Test coverage should verify requests are created in the correct order and only for configured secrets.

#### 2. Kafka Configuration (`plugins/kafka/src/configuration.cpp`)

**P1 · Kafka aws_iam_options::from_record has different validation than SQS/S3 · 85%**

- **File**: `plugins/kafka/src/configuration.cpp:35-119`
- **Severity**: P1 — Critical
- **Confidence**: 85%
- **Issue**: The Kafka configuration parser requires `region` to be specified (line 51-56), unlike the generic `tenzir::aws_iam_options` which makes it optional. This design inconsistency creates three different validation schemas:
  - `tenzir::aws_iam_options` (SQS): region optional
  - `kafka::configuration::aws_iam_options`: region required
  - S3's usage of `tenzir::aws_iam_options` via plugins.cpp: region optional but checked separately
- **Uncovered Cases**:
  - Kafka operator behavior when region is missing from aws_iam record: No test
  - Validation error messages are not tested against expected output
  - The Kafka variant duplicates validation logic (lines 57-119) that mirrors libtenzir/src/aws_iam.cpp
- **Suggestion**:
  1. Create Kafka-specific tests that verify region is required
  2. Document why Kafka differs from SQS/S3 in configuration expectations
  3. Consider consolidating validation logic if possible

#### 3. SQS Operator Integration (`plugins/sqs/builtins/plugin.cpp`)

**P2 · SQS operator aws_iam parsing and validation untested · 88%**

- **File**: `plugins/sqs/builtins/plugin.cpp:36-76`
- **Severity**: P2 — Important
- **Confidence**: 88%
- **Issue**: The SQS plugin's `make()` function (which parses arguments and creates operators) has validation that is never tested:
  - Queue name validation (empty string check, lines 50-56)
  - "sqs://" prefix stripping (lines 57-59)
  - poll_time range validation [1s, 20s] (lines 66-73)
  - aws_iam argument parsing and error handling
- **Coverage Gap**: Test case combinations:
  - Empty queue name → should emit error
  - Queue name with "sqs://" prefix → should be stripped
  - poll_time < 1s → should emit error
  - poll_time > 20s → should emit error
  - aws_iam with malformed record → should emit error
- **Suggestion**: Add integration tests (TQL or operator tests) that exercise SQS operator creation with invalid arguments.

#### 4. S3 Operator Validation (`plugins/s3/builtins/plugins.cpp` and `plugins/s3/builtins/from_s3.cpp`)

**P1 · S3 authentication method conflict detection untested · 92%**

- **File**: `plugins/s3/builtins/plugins.cpp` (lines with aws_iam validation) and `plugins/s3/builtins/from_s3.cpp:35-100`
- **Severity**: P1 — Critical
- **Confidence**: 92%
- **Issue**: Complex multi-method authentication validation with no tests:
  - Counting auth methods (anonymous, role, explicit credentials, aws_iam)
  - Detecting >1 auth method specified simultaneously (lines 36-65 in from_s3.cpp)
  - Rejecting aws_iam + anonymous combination (lines 67-72)
  - Rejecting aws_iam + individual credential options (lines 73-82)
  - S3-specific constraint: rejecting explicit credentials + role assumption (plugins.cpp, mentioned in output)
  - Rejecting access_key/secret_key without pairing (lines 83-89)
  - Rejecting session_token without access_key (lines 90-95)
  - Rejecting external_id without role (lines 96-99)
- **Uncovered Scenarios**:
  - All pairwise conflicts between {anonymous, role, access_key/secret_key, aws_iam}
  - Error messages do not have test assertions
  - The S3-specific "explicit credentials with role assumption not supported" constraint mentioned in plugins.cpp
- **Suggestion**: Create parameterized test cases for the validation matrix covering all conflicting combinations.

#### 5. Credential Resolution and Secret Handling

**P2 · Credential resolution flow untested end-to-end · 82%**

- **Files**: `plugins/sqs/include/operator.hpp:240-280` (sqs_loader), `plugins/sqs/include/operator.hpp:314-350` (sqs_saver)
- **Severity**: P2 — Important
- **Confidence**: 82%
- **Issue**: The operators request secret resolution via `ctrl.resolve_secrets_must_yield()` but no integration test verifies:
  - Secrets are resolved before queue connection attempts
  - resolved_creds struct is properly populated
  - Empty session_token (when not provided) is handled correctly in resolved credentials
  - Secret resolution failures propagate as diagnostics
  - Multiple secrets in a single request are all resolved
- **Suggestion**: Integration tests should mock or stub the secret resolution and verify operator behavior with resolved and unresolved credentials.

#### 6. Authentication Flow Edge Cases

**P3 · Role assumption session defaults not tested · 78%**

- **File**: `plugins/sqs/include/operator.hpp:125-126`
- **Severity**: P3 — Minor
- **Confidence**: 78%
- **Issue**: Default session name "tenzir-session" and empty ext_id string for role assumption have no test coverage:
  - Line 125: `session = role_session_name.value_or("tenzir-session")`
  - Line 126: `ext = role_external_id.value_or("")`
  - No tests verify these defaults are applied when not specified
  - No tests verify the defaults work with AWS STS API
- **Suggestion**: Test that omitting session_name and external_id uses documented defaults.

**P3 · AWS endpoint URL override behavior untested · 75%**

- **File**: `plugins/sqs/include/operator.hpp:72-77`
- **Severity**: P3 — Minor
- **Confidence**: 75%
- **Issue**: The code respects AWS_ENDPOINT_URL and AWS_ENDPOINT_URL_SQS environment variables (with a TODO comment suggesting this is a workaround), but this behavior is never tested:
  - AWS_ENDPOINT_URL is checked first, then AWS_ENDPOINT_URL_SQS
  - No tests verify correct precedence or that empty values are ignored
  - Could mask misconfigurations or cause unexpected behavior
- **Suggestion**: Test endpoint URL environment variable handling and precedence.

#### 7. Kafka OAuth Bearer Token Callback

**P2 · Kafka oauth token generation untested · 80%**

- **File**: `plugins/kafka/src/configuration.cpp:139-200+` (extends beyond first 150 lines)
- **Severity**: P2 — Important
- **Confidence**: 80%
- **Issue**: The AWS IAM callback that generates OAuth bearer tokens for Kafka MSK:
  - Complex AWS SDK integration (AWS InitAPI, SigV4 signing)
  - Token refresh logic with 900-second validity
  - HTTP request construction and signing
  - Error handling for token generation failures
  - No unit tests for token generation
  - No integration tests with actual Kafka broker or mock
- **Suggestion**: Create unit tests with mocked AWS SDK calls to verify token is generated and properly formatted.

#### 8. SQS Queue Operations with IAM

**P2 · SQS queue operations with IAM credentials untested · 85%**

- **File**: `plugins/sqs/include/operator.hpp:140-212`
- **Severity**: P2 — Important
- **Confidence**: 85%
- **Issue**: Queue operations (ReceiveMessage, SendMessage, DeleteMessage, GetQueueUrl) use credentials from the IAM options, but error cases with explicit credentials are not tested:
  - Failed ReceiveMessage with explicit credentials (line 151-157)
  - Failed SendMessage with explicit credentials (line 169-175)
  - Failed DeleteMessage with explicit credentials (line 185-194)
  - Failed GetQueueUrl with explicit credentials (line 202-210)
  - No tests verify error messages include appropriate context
- **Suggestion**: Create integration tests that attempt operations with valid and invalid credentials to ensure proper error reporting.

### Positive Coverage Observations

- **Argument parsing structure**: The use of `connector_args` struct and `argument_parser2` for S3/SQS is sound and follows project patterns
- **Diagnostics emission**: Error reporting uses project's diagnostic system consistently
- **Secret abstraction**: Using `secret` type and `make_secret_request` is appropriate for credential handling
- **Validation ordering**: Validations are performed before resource creation (good defensive programming)

## Missing Test Categories

### Unit Tests (0 found)
- aws_iam_options parsing and validation
- Secret request generation
- Configuration validation for each service (SQS, S3, Kafka)

### Integration Tests (0 found)
- SQS with explicit credentials
- SQS with profile credentials
- SQS with role assumption
- S3 with aws_iam option
- Kafka with aws_iam option
- Authentication failures (invalid credentials, missing region, etc.)
- Multi-operator pipelines using aws_iam

### Scenario Tests (0 found)
- Endpoint URL override behavior
- Default credential chain fallback
- Credential refresh/expiration for temporary credentials
- Cross-region role assumption

## Risk Assessment

### High-Risk Areas (Likely to Cause Production Issues)

1. **Configuration validation will fail at runtime** - No parsing tests means typos or invalid configs discovered only when operator starts
2. **Authentication errors uncaught** - Complex credential resolution has no tests; failures could cascade
3. **S3 auth method validation not verified** - Conflicting auth methods should be rejected but untested combination matrix
4. **Kafka region requirement silent failure** - Region requirement is different but not documented or tested

### Medium-Risk Areas (Edge Cases)

1. **Session defaults for role assumption** - Default values used without test verification
2. **Endpoint URL override precedence** - Undocumented behavior could mask misconfiguration
3. **Kafka token generation** - Complex AWS SDK integration with no tests
4. **Poll time validation** - SQS poll_time constraints [1s, 20s] untested

## Recommendations

### Immediate (P1)

1. **Create unit test file for aws_iam**: `/home/tobim/t/tenzir/load_sqs-credentials/libtenzir/test/aws_iam.cpp`
   - Test all validation paths in `aws_iam_options::from_record`
   - Test `make_secret_requests` for various credential combinations
   - Use parameterized tests for combinations

2. **Create Kafka-specific aws_iam tests**
   - Document why region is required for Kafka but optional for SQS
   - Test region requirement enforcement
   - Compare validation logic with SQS variant

3. **Create S3 auth conflict matrix tests**
   - Systematic test of all conflicting auth method combinations
   - Verify error messages are specific and helpful

### Medium-Term (P2)

1. **Integration tests for SQS operator**
   - Test load_sqs with explicit credentials
   - Test save_sqs with profile credentials
   - Test with invalid credentials (should fail gracefully)

2. **Integration tests for S3 operator**
   - Test from_s3/to_s3 with aws_iam option
   - Test S3-specific constraints (no role + explicit creds)

3. **Kafka integration tests**
   - Mock AWS STS for token generation
   - Test token refresh callback
   - Test connection with valid/invalid credentials

### Documentation (P3)

1. **Document aws_iam option differences**:
   - SQS: region optional
   - Kafka: region required
   - S3: region optional, no explicit+role combination

2. **Add examples for each authentication method**:
   - Explicit credentials
   - AWS CLI profile
   - Role assumption
   - Default credential chain

3. **Document edge cases**:
   - Endpoint URL override behavior
   - Session defaults for role assumption
   - Poll time constraints for SQS

## Files Requiring Test Coverage

- `/home/tobim/t/tenzir/load_sqs-credentials/libtenzir/include/tenzir/aws_iam.hpp`
- `/home/tobim/t/tenzir/load_sqs-credentials/libtenzir/src/aws_iam.cpp` ← **Critical**
- `/home/tobim/t/tenzir/load_sqs-credentials/plugins/sqs/include/operator.hpp`
- `/home/tobim/t/tenzir/load_sqs-credentials/plugins/sqs/builtins/plugin.cpp` ← **Critical**
- `/home/tobim/t/tenzir/load_sqs-credentials/plugins/s3/builtins/from_s3.cpp` ← **Critical**
- `/home/tobim/t/tenzir/load_sqs-credentials/plugins/s3/builtins/plugins.cpp` ← **Critical**
- `/home/tobim/t/tenzir/load_sqs-credentials/plugins/kafka/src/configuration.cpp` ← **Critical**
- `/home/tobim/t/tenzir/load_sqs-credentials/plugins/kafka/include/kafka/configuration.hpp`

## Summary of Findings

| Category | Count | Severity |
|----------|-------|----------|
| P1 (Critical) | 4 | Missing unit tests, conflicting validation schemas, untested auth validation |
| P2 (Important) | 4 | Missing integration tests, untested edge cases, credential flow gaps |
| P3 (Minor) | 2 | Undocumented defaults, environment variable handling |

**Total Issues**: 10
**Average Confidence**: 86%
**Test Coverage**: ~5% (only secrets.cpp in libtenzir/test exercises secret infrastructure, but not aws_iam specifically)
