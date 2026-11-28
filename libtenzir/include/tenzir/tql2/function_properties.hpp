//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>

namespace tenzir {

/// How a function's return value may vary across calls with equal arguments.
/// Ordered from strictest to most general so lattice joins use `std::max`.
enum class Volatility : uint8_t {
  /// Pure function of its arguments: equal arguments always yield the same
  /// value (e.g., `abs`, `to_lower`, `sha256`, `parse_time`).
  immutable,
  /// Constant within a pipeline execution or dependent on external state or
  /// wall-clock time (e.g., `now`, `file_contents`, `secret`). Must not be
  /// constant-folded at compile time or duplicated across re-evaluations.
  stable,
  /// May produce a different value on every call, even for the same row
  /// (e.g., `random`, `uuid`, `encrypt_hpke`). Must not be duplicated or
  /// pushed across cardinality-expanding operators.
  volatile_,
};

/// Whether evaluating an expression or valid (instantiated) function call over
/// event input can emit warnings or fail the TQL pipeline.
///
/// This describes runtime evaluation over event input (not `const_eval`
/// without input, where event references like `this` and `root_field` fail),
/// and for function plugins it describes per-row kernel evaluation assuming
/// call instantiation succeeded. Ordered from safest to most fallible so
/// lattice joins use `std::max`.
enum class Fallibility : uint8_t {
  /// Never fails the pipeline and never emits warnings during runtime
  /// evaluation over event input across both executors (e.g., evaluable
  /// constants, `this`, `@meta`, `x?`, spreadless record literals over `total`
  /// fields, 0/1-element spreadless lists over `total` items, `is_null`,
  /// `type_of`).
  total,
  /// Never fails the pipeline in TQL, though it may emit warnings (e.g.,
  /// arithmetic/comparison operators, `field_access` including `a.?b`,
  /// `index_expr`, multi-element/spread lists, single-spread records, `abs`,
  /// `to_lower`).
  warns,
  /// May fail the TQL pipeline depending on runtime input (conservative default
  /// for function plugins; e.g., `slice`, `parse_*`).
  fails,
};

/// Relative per-row evaluation cost tier of an expression or function call.
/// Ordered from cheapest to most expensive so lattice joins use `std::max`.
enum class Cost : uint8_t {
  /// Field read, metadata read, or constant literal: free to duplicate.
  plain,
  /// Operators and O(1) scalar functions: acceptable to duplicate once.
  cheap,
  /// Parsers, regex, format strings, lambdas, or non-trivial functions:
  /// do not duplicate.
  expensive,
};

} // namespace tenzir
