//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core.hpp"
#include "tenzir/detail/enum.hpp"

#include <fmt/format.h>

#include <string>

namespace tenzir {

TENZIR_ENUM(secret_type_type, string);
TENZIR_ENUM(secret_encoding, none, encoded, decoded);

class secret;
class secret_view;

namespace detail {

// The underlying type used for the enums. This exists as a typedef to be able
// to ensure consistency between the enum, arrow and the flatbuffer
// representation.
using secret_enum_underlying_type = std::underlying_type_t<secret_type_type>;

// The implementation of the secret/secret_view
template <typename StringType>
class secret_common {
  friend class ::tenzir::secret;
  friend class ::tenzir::secret_view;

public:
  secret_common(StringType value_ = {}, StringType name_ = {},
                secret_type_type type = secret_type_type::string,
                secret_encoding encoding = secret_encoding::none)
    : value_{std::move(value_)},
      name_{std::move(name_)},
      type_{type},
      encoding_{encoding} {
  }

  // Gets the string representation of the secrets value. Take care not to
  // leak the secret.
  [[nodiscard]] auto unsafe_get_value_be_careful() const -> std::string_view {
    return value_;
  }
  /// The name of the secret, if it was loaded from a manager
  [[nodiscard]] auto name() const -> std::string_view {
    return name_;
  }
  /// The type of the secret. Anything other than `string` may require
  /// additional parsing to be done on the value
  [[nodiscard]] auto type() const -> secret_type_type {
    return type_;
  }
  /// Whether any encoding operations have been done on the secret. This is only
  /// meant to be used when recovering a pipeline from a checkpoint, after
  /// querying the value from the secret manager.
  [[nodiscard]] auto encoding() const -> secret_encoding {
    return encoding_;
  }

  // Clears `value_`
  auto clear() -> void {
    value_ = {};
  };

  auto print_to(auto& it) const -> bool {
    if (name_.empty()) {
      it = fmt::format_to(it, "<secret>");
    } else {
      it = fmt::format_to(it, "<secret:{}>", name_);
    }
    return true;
  }

  template <typename T1, typename T2>
  friend auto operator<=>(const secret_common<T1>& lhs,
                          const secret_common<T2>& rhs) -> std::strong_ordering;

  template <typename T1, typename T2>
  friend auto operator==(const secret_common<T1>& lhs,
                         const secret_common<T2>& rhs) -> bool;

  inline friend auto inspect(auto& f, secret_common& x) -> bool {
    return f.object(x).fields(f.field("name", x.name_),
                              f.field("value", x.value_),
                              f.field("type", x.type_),
                              f.field("encoding", x.encoding_));
  }

private:
  StringType value_ = {};
  StringType name_ = {};
  secret_type_type type_ = secret_type_type::string;
  secret_encoding encoding_ = secret_encoding::none;
};

template <typename StringType>
auto to_string(const secret_common<StringType>& v) {
  auto s = std::string{};
  auto it = std::back_inserter(s);
  s.reserve(7 + v.name().size());
  TENZIR_ASSERT(v.print_to(it));
  return s;
}

template <typename T1, typename T2>
auto operator<=>(const secret_common<T1>& lhs,
                 const secret_common<T2>& rhs) -> std::strong_ordering {
  return std::tie(lhs.value_, lhs.name_, lhs.type_, lhs.encoding_)
         <=> std::tie(rhs.value_, rhs.name_, rhs.type_, rhs.encoding_);
}

template <typename T1, typename T2>
auto operator==(const secret_common<T1>& lhs,
                const secret_common<T2>& rhs) -> bool {
  return (lhs <=> rhs) == std::strong_ordering::equal;
}

} // namespace detail

class secret final : public detail::secret_common<std::string> {
public:
  using impl = detail::secret_common<std::string>;
  friend class secret_view;
  using impl::impl;
};

class secret_view final : public detail::secret_common<std::string_view> {
public:
  using impl = detail::secret_common<std::string_view>;
  using impl::impl;
  secret_view(const secret& s) : impl{s.value_, s.name_, s.type_, s.encoding_} {
  }
};

inline auto materialize(secret_view v) -> secret {
  return secret{
    std::string{v.unsafe_get_value_be_careful()},
    std::string{v.name()},
    v.type(),
    v.encoding(),
  };
}

inline auto clear(secret_view v) -> secret_view {
  v.clear();
  return v;
}

template <class T>
struct secret_printer : printer_base<secret_printer<T>> {
  using attribute = T;

  template <class Iterator>
  bool print(Iterator& out, const attribute& x) const {
    return x.print_to(out);
  }
};

template <>
struct printer_registry<secret> {
  using type = secret_printer<secret>;
};

template <>
struct printer_registry<secret_view> {
  using type = secret_printer<secret_view>;
};

class table_slice;
/// Clears all secret values in the table slice
table_slice clear_secrets(table_slice);

} // namespace tenzir

namespace fmt {
template <typename StringType>
struct formatter<tenzir::detail::secret_common<StringType>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::detail::secret_common<StringType>& secret,
              FormatContext& ctx) const {
    auto it = ctx.out();
    secret.print_to(it);
    return it;
  }
};

template <>
struct formatter<tenzir::secret> : formatter<tenzir::secret::impl> {};

template <>
struct formatter<tenzir::secret_view> : formatter<tenzir::secret_view::impl> {};
} // namespace fmt
