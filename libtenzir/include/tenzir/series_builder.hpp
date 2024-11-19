//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/series.hpp"
#include "tenzir/type.hpp"
#include "tenzir/variant.hpp"
#include "tenzir/view.hpp"

#include <arrow/type_fwd.h>

#include <memory>
#include <string_view>
#include <type_traits>

namespace tenzir {

class builder_ref;
class record_ref;
class series_builder;
struct data_view2;

namespace detail {

class dynamic_builder;
class series_builder_impl;
class builder_base;
template <class Type>
class typed_builder;
struct atom_view;

} // namespace detail

/// An adaptive builder suited for the row-wise creation of series.
///
/// This class is mostly used to build table slices, but it can also be used to
/// construct arbitrary series. The type of the series is iteratively upgraded
/// whenever possible, for example when new fields are added to a record.
///
/// The four main construction methods are `.record()`, `.field(name)`,
/// `.list()`, and `.atom(value)`. The handles returned for the construction of
/// nested objects become invalidated once a method is called on one of their
/// parents. For example, `auto r = builder.record()` is not valid anymore after
/// another record has been created with `auto s = builder.record()`. All
/// handles and the main builder itself can be moved freely without invalidation.
///
/// It can happen that type conflicts arise. For example, a field can be a
/// string in one element of the series, but a record in another. These
/// conflicts are normally solved by flushing previous events into their own
/// table slice, which is why `.finish()` returns a vector of table slices. The
/// data of the element that is still being built is retained while flushing.
/// Furthermore, the builder resets its internal state such that it matches the
/// state that the builder would have if the current event was added to a fresh
/// instance of the builder. This makes so that we avoid unnecessary type
/// conflicts in events further down the line.
///
/// However, some type conflicts can not be resolved in that way. This happens
/// if the type conflict is between elements of a list inside a single series
/// element. In that case, we convert the conflicting values into a string. For
/// example, `[{a: []}, {a: 42}]` becomes `[{a: "[]"}, {a: "42"}]`. This is the
/// only way to resolve the conflict without loss of data because we do not have
/// union types. The builder is currently always flushed before and after this
/// kind of conflict occurs.
///
/// Finally, the builder can be initialized with a type. This directly creates
/// the necessary sub-builders, instead of lazily discovering them. Furthermore,
/// the state associated with the type is not reset when the builder is flushed.
/// New fields can still be added and are removed again when flushing. The
/// `.is_protected()` method returns true if the associated builder was created
/// as part of the preparation for the given type. At the moment, the user of
/// this API must ensure that they do not cause a type conflict within a
/// protected builder, as this will trigger an assertion. This can be done by
/// checking `.kind()` before calling the builder method. In the future, we
/// could change the API to return an error in this case, or we could
/// temporarily change the type according to the normal rules and reset it
/// afterwards.
class series_builder {
public:
  /// Initializes the builder, optionally with a given type (see above).
  series_builder(std::optional<std::reference_wrapper<const tenzir::type>> ty);
  series_builder(const tenzir::type* ty = nullptr);
  series_builder(const tenzir::type& ty) : series_builder(&ty) {
  }

  ~series_builder();
  series_builder(const series_builder&) = delete;
  series_builder(series_builder&&) noexcept;
  auto operator=(const series_builder&) -> series_builder& = delete;
  auto operator=(series_builder&&) noexcept -> series_builder&;

  /// Adds a `null` value to the builder.
  ///
  /// This operation cannot fail. It is equivalent to `data(caf::none)`.
  void null();

  /// Attempts to add the given data to the builder.
  ///
  /// This only attempts conversions if the builder was initialized with a type.
  /// In that case, it fails if there is no eligible conversion. Otherwise, the
  /// type is inferred only based on the given value. Enumeration types cannot
  /// be inferred from their data and return an error instead.
  auto try_data(data_view2 value) -> caf::expected<void>;

  /// Same as `try_data(value)`, but asserts success.
  void data(data_view2 value);

  /// Begins building a new record.
  ///
  /// Unlike `data(record{...})`, the record fields can be specified on-the-fly.
  auto record() -> record_ref;

  /// Begins building a new list.
  ///
  /// Similar to `record()`.
  auto list() -> builder_ref;

  /// Finishes and returns the built data arrays.
  ///
  /// Returns a `vector` instead of a single array because type conflicts are
  /// handled by starting a new array. After calling this method, the builder is
  /// empty and can be directly used again. If the builder was initialized with
  /// a type, then this initialization is preserved.
  auto finish() -> std::vector<series>;

  /// Similar to `finish()`, but converts the result to table slices.
  ///
  /// If `name == ""`, then the name will match the name of the type that was
  /// used for initialization (if it had a name), and `tenzir.json` otherwise.
  ///
  /// @pre all top-level elements must be records
  auto
  finish_as_table_slice(std::string_view name = "") -> std::vector<table_slice>;

  /// Same as `finish_as_table_slice(name)`, but asserts that there is only one
  /// result.
  auto finish_assert_one_slice(std::string_view name = "") -> table_slice;

  /// Same as `finish_as_table_slice(name)`, but asserts that there is only one
  /// result.
  auto finish_assert_one_array() -> series;

  /// Returns the full type, which can be expensive. Use `kind()` if possible.
  auto type() -> type;

  /// Returns `type().kind()`, but can be significantly more efficient.
  auto kind() -> type_kind;

  /// Returns the number of elements that would be returned by `finish()`.
  auto length() const -> int64_t;

  /// Removes the element that is currently being built.
  void remove_last();

private:
  std::unique_ptr<detail::series_builder_impl> impl_;

  friend class builder_ref;
};

/// A temporary alternative to `data_view`.
///
/// Unlike `data_view`, this type is based on `std::variant` and is more
/// convenient to use as a parameter.
///
/// TODO: Consider eventually retiring the current `data_view`, perhaps
/// replacing it with an implementation that does not use ref-counts internally.
struct data_view2
  : caf::detail::tl_apply_t<caf::detail::tl_map_t<data::types, view_trait>,
                            variant> {
  using variant::variant;

  explicit(false) data_view2(const data& x) : data_view2(make_view(x)) {
  }

  explicit(false) data_view2(data_view x) {
    tenzir::match(std::move(x), [&](auto x) {
      emplace<decltype(x)>(std::move(x));
    });
  }

  explicit(false) data_view2(const record& x) : variant{make_view(x)} {
  }

  explicit(false) data_view2(const list& x) : variant{make_view(x)} {
  }
};

namespace detail {

class field_ref {
public:
  field_ref(detail::typed_builder<record_type>* origin, std::string_view name)
    : origin_{origin}, name_{name} {
  }

  void atom(detail::atom_view value);

  auto record() -> record_ref;

  auto list() -> builder_ref;

  auto kind() -> type_kind;

  auto type() -> type;

  auto is_protected() -> bool;

private:
  /// Returns the builder for this field, or `nullptr` if it does not exist.
  auto builder() -> detail::dynamic_builder*;

  detail::typed_builder<record_type>* origin_;
  std::string_view name_;
};

} // namespace detail

/// A type-erased reference to a builder.
///
/// See `series_builder` for documentation of the methods.
class builder_ref {
public:
  explicit(false) builder_ref(series_builder& ref) : ref_{ref.impl_.get()} {
  }

  explicit(false) builder_ref(detail::series_builder_impl& ref)
    : ref_{std::addressof(ref)} {
  }

  explicit(false) builder_ref(detail::dynamic_builder& ref)
    : ref_{std::addressof(ref)} {
  }

  explicit(false) builder_ref(detail::field_ref ref) : ref_{ref} {
  }

  void null();

  void data(data_view2 value);

  auto try_data(data_view2 value) -> caf::expected<void>;

  auto list() -> builder_ref;

  auto record() -> record_ref;

  auto kind() -> type_kind;

  auto type() -> type;

  /// Returns true if this builder was initialized with a type.
  auto is_protected() -> bool;

private:
  void atom(detail::atom_view value);

  auto try_atom(detail::atom_view) -> caf::expected<void>;

  template <class F>
  auto dispatch(F&& f) -> decltype(auto);

  std::variant<detail::series_builder_impl*, detail::dynamic_builder*,
               detail::field_ref>
    ref_;
};

class record_ref {
public:
  explicit record_ref(detail::typed_builder<record_type>* origin)
    : origin_{origin} {
  }

  /// Returns the builder for the given field.
  ///
  /// Note that this method has no immediate effect.
  auto field(std::string_view name) -> builder_ref {
    return detail::field_ref{origin_, name};
  }

  /// SAme as `field(name).data(value)`.
  void field(std::string_view name, data_view2 value) {
    field(name).data(std::move(value));
  }

private:
  detail::typed_builder<record_type>* origin_;
};

} // namespace tenzir
