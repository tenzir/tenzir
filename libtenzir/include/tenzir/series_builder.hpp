//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <arrow/type_fwd.h>

#include <memory>
#include <string_view>

// Things that we might want to support (not necessarily here):
// - Schema initialization (keep field types?)
// - No-infer mode with existing schema
// - Physical and logical (number -> port)
// - Casting (unsigned -> signed integer)
// --------------------------------------------------------------
// - Value type inference (e.g., string -> time)
// - Reading with selector field (internally tagged)
// - Unnest / unflatten
// - Timeout flushing
// - Batch size flushing

// Problem:
// - List with multiple types.
// - Hostname & ip adresses

// rr.data.json / suricata DNS   <ip -> hostname>

// Consider:
// - Create builder with unions (but not use them).
// - Removing unions *after* building could be more efficient (no need to switch
//   types multiple times), or less efficient (have to traverse unions multiple
//   times instead of LRU logic).
// - Might want rare variants to have their own timeout logic.
// - https://github.com/tenzir/issues/issues/566 could have value-dependant
//   solution
// - Reset JSON parser after every flush to reduce impact of bad data.
// - When `{"foo": {"bar": 42}}`, we could `{"foo": ""} -> {"foo": {}}`

namespace tenzir::experimental {

class series_builder;
class field_ref;
class list_ref;
class record_ref;

namespace detail {

class typed_builder;
class record_builder;
class list_builder;

} // namespace detail

/// Methods overwrite the field.
class field_ref {
public:
  field_ref(detail::record_builder* origin, std::string_view name)
    : origin_{origin}, name_{name} {
  }

  void null();

  void atom(int64_t value);

  auto record() -> record_ref;

  auto list() -> list_ref;

  /// TODO: Do we want this? How do we want it?
  /// Returns the current builder for this field.
  /// @warning Returns `nullptr` if this does field does not exist yet.
  auto builder() -> series_builder*;

private:
  detail::record_builder* origin_;
  std::string_view name_;
};

/// Method has no immediate effect.
class record_ref {
public:
  explicit record_ref(detail::record_builder* origin) : origin_{origin} {
  }

  auto field(std::string_view name) -> field_ref {
    return field_ref{origin_, name};
  }

private:
  detail::record_builder* origin_;
};

/// Methods append to the list.
class list_ref {
public:
  explicit list_ref(detail::list_builder* origin) : origin_{origin} {
  }

  // TODO: Subset of the methods in `series_builder`. Unify?
  void null();

  void atom(int64_t value);

  auto record() -> record_ref;

  auto list() -> list_ref;

private:
  detail::list_builder* origin_;
};

class series_builder {
public:
  series_builder();
  ~series_builder();
  series_builder(const series_builder&) = delete;
  series_builder(series_builder&&) = default;
  auto operator=(const series_builder&) -> series_builder& = delete;
  auto operator=(series_builder&&) -> series_builder& = default;

  explicit series_builder(std::unique_ptr<detail::typed_builder> builder);

  void null();

  void atom(int64_t value);

  auto record() -> record_ref;

  auto list() -> list_ref;

  // -----

  void resize(int64_t length);

  auto length() -> int64_t;

  // -----

  auto type() -> std::shared_ptr<arrow::DataType>;

  auto finish() -> std::shared_ptr<arrow::Array>;

  void reset();

private:
  friend class detail::record_builder;

  template <class Builder>
  auto prepare() -> Builder*;

  std::unique_ptr<detail::typed_builder> builder_;
};

} // namespace tenzir::experimental
