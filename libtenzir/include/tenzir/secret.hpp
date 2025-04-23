//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/enum.hpp"
#include "tenzir/diagnostics.hpp"

#include <fmt/format.h>

#include <string>

namespace tenzir {

/// @brief, how the secret was created.
/// * `literal` means that it was a literal in the pipeline
/// * `managed` means that it should be queried from the platform/created by the
///    `secret` function
TENZIR_ENUM(secret_source_type, literal, managed, uninitialized);
/// @brief which encode/decode operations were done on the value in the pipeline
/// `This is important for both persistence and the actual value computation.
TENZIR_ENUM(secret_encoding, none, was_decoded);

class secret;
class secret_view;

namespace detail {

// The underlying type used for the enums. This exists as a typedef to be able
// to ensure consistency between the enum, arrow and the flatbuffer
// representation.
using secret_enum_underlying_type = std::underlying_type_t<secret_source_type>;

/// The implementation of the secret/secret_view types.
/// The actual value can be obtained using
/// `operator_control_plane::resolve_secret_must_yield`.
template <typename StringType>
class secret_common {
  friend class ::tenzir::secret;
  friend class ::tenzir::secret_view;

public:
  secret_common() : name_{}, source_type_{secret_source_type::uninitialized} {
  }

  secret_common(StringType value)
    : name_{std::move(value)}, source_type_{secret_source_type::literal} {
  }

  secret_common(StringType name_, secret_source_type source_type,
                secret_encoding encoding = secret_encoding::none)
    : name_{std::move(name_)}, source_type_{source_type}, encoding_{encoding} {
  }

  /// The name of the secret, if it was loaded from a manager
  [[nodiscard]] auto name() const -> std::string_view {
    return name_;
  }
  // The "source type"; how the secret was created.
  [[nodiscard]] auto source_type() const -> secret_source_type {
    return source_type_;
  }
  /// Whether any encoding operations have been done on the secret. This is only
  /// meant to be used when recovering a pipeline from a checkpoint, after
  /// querying the value from the secret manager.
  [[nodiscard]] auto encoding() const -> secret_encoding {
    return encoding_;
  }

  auto print_to(auto& it) const -> bool {
    if (source_type_ == secret_source_type::literal) {
      it = fmt::format_to(it, "secret::from_string(\"{}\")", name_);
    } else {
      it = fmt::format_to(it, "secret(\"{}\")", name_);
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
                              f.field("source_type", x.source_type_),
                              f.field("encoding", x.encoding_));
  }

private:
  StringType name_ = {};
  secret_source_type source_type_ = secret_source_type::uninitialized;
  secret_encoding encoding_ = secret_encoding::none;
};

extern template class secret_common<std::string>;
extern template class secret_common<std::string_view>;

template <typename StringType>
auto to_string(const secret_common<StringType>& v) {
  auto s = std::string{};
  auto it = std::back_inserter(s);
  if (v.source_type() == secret_source_type::managed) {
    s.reserve(std::size("secret(\"\")") + v.name().size());
  } else {
    s.reserve(std::size("secret::from_string(\"\")") + v.name().size());
  }
  TENZIR_ASSERT(v.print_to(it));
  return s;
}

template <typename T1, typename T2>
auto operator<=>(const secret_common<T1>& lhs,
                 const secret_common<T2>& rhs) -> std::strong_ordering {
  return std::tie(lhs.name_, lhs.source_type_, lhs.encoding_)
         <=> std::tie(rhs.name_, rhs.source_type_, rhs.encoding_);
}

template <typename T1, typename T2>
auto operator==(const secret_common<T1>& lhs,
                const secret_common<T2>& rhs) -> bool {
  return (lhs <=> rhs) == std::strong_ordering::equal;
}

} // namespace detail

/// @relates detail::secret_common
class secret final : public detail::secret_common<std::string> {
public:
  using impl = detail::secret_common<std::string>;
  friend class secret_view;
  using impl::impl;
};

/// @relates detail::secret_common
class secret_view final : public detail::secret_common<std::string_view> {
public:
  using impl = detail::secret_common<std::string_view>;
  using impl::impl;
  secret_view(const secret& s) : impl{s.name_, s.source_type_, s.encoding_} {
  }
};

inline auto materialize(secret_view v) -> secret {
  return secret{
    std::string{v.name()},
    v.source_type(),
    v.encoding(),
  };
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
