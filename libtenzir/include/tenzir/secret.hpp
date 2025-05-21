//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/fbs/data.hpp"
#include "tenzir/flatbuffer.hpp"

#include <fmt/format.h>

namespace tenzir {

class secret;
class secret_view;

namespace detail::secrets {

using owning_root_fbs_buffer = flatbuffer<fbs::data::Secret>;
using owning_fbs_buffer = child_flatbuffer<fbs::data::Secret>;
using viewing_fbs_buffer = child_flatbuffer<fbs::data::Secret>;

/// The implementation of the secret/secret_view types.
/// The actual value can be obtained using
/// `operator_control_plane::resolve_secret_must_yield`.
template <typename FlatbufferType>
class secret_common {
  friend class ::tenzir::secret;
  friend class ::tenzir::secret_view;

public:
  secret_common() = default;

  secret_common(FlatbufferType buffer) : buffer{std::move(buffer)} {
  }

  template <typename T1, typename T2>
  friend auto
  operator<=>(const secret_common<T1>& lhs, const secret_common<T2>& rhs)
    -> std::partial_ordering;

  template <typename T1, typename T2>
  friend auto
  operator==(const secret_common<T1>& lhs, const secret_common<T2>& rhs)
    -> bool;

  inline friend auto inspect(auto& f, secret_common& x) -> bool {
    return f.object(x).fields(f.field("buffer", x.buffer));
  }

  /// Creates a new secret with `literal` prepended to it.
  auto with_prepended(std::string_view literal) const -> secret;
  /// Creates a new secret with `literal` appended to it.
  auto with_appended(std::string_view literal) const -> secret;
  /// Creates a new secret with the contents of `other` appended to it.
  auto with_appended(const secret_common<viewing_fbs_buffer>& other) const
    -> secret;

  FlatbufferType buffer;
};

extern template class secret_common<owning_fbs_buffer>;
extern template class secret_common<viewing_fbs_buffer>;

template <typename T1, typename T2>
auto operator<=>(const secret_common<T1>& lhs, const secret_common<T2>& rhs)
  -> std::partial_ordering {
  const auto l = as_bytes(lhs.buffer);
  const auto r = as_bytes(rhs.buffer);
  if (l.size() != r.size()) {
    return std::partial_ordering::unordered;
  }
  if (std::equal(l.begin(), l.end(), r.begin())) {
    return std::partial_ordering::equivalent;
  }
  return std::partial_ordering::unordered;
}

template <typename T1, typename T2>
auto operator==(const secret_common<T1>& lhs, const secret_common<T2>& rhs)
  -> bool {
  return (lhs <=> rhs) == std::partial_ordering::equivalent;
}

template <typename FlatbufferType>
auto as_bytes(const secret_common<FlatbufferType>& s)
  -> std::span<const std::byte> {
  return as_bytes(s.buffer);
}

/// If we dont manually implement this, we run into some issues with the
/// recursive `hash_inspector`.
template <class HashAlgorithm, typename FlatbufferType>
void hash_append(HashAlgorithm& h, const secret_common<FlatbufferType>& s) {
  return hash_append(h, as_bytes(s));
}

} // namespace detail::secrets

/// @relates detail::secrets::secret_common
class secret final
  : public detail::secrets::secret_common<detail::secrets::owning_fbs_buffer> {
public:
  using impl
    = detail::secrets::secret_common<detail::secrets::owning_fbs_buffer>;
  friend class secret_view;
  using impl::impl;

  secret(std::string_view name, std::string_view operations, bool is_literal);

  static auto make_literal(std::string_view value) -> secret;
  static auto make_managed(std::string_view value) -> secret;
  static auto from_fb(const fbs::data::Secret*) -> secret;
};

/// @relates detail::secrets::secret_common
/// TODO: Currently a `secret_view` is identical to a `secret`, because both use
/// the owning tenzir::flatbuffer wrapper. We ideally want a non-owning version
/// of `flatbuffer` that does not hold a `chunk_ptr`, but only the `Table*`.
class secret_view final
  : public detail::secrets::secret_common<detail::secrets::viewing_fbs_buffer> {
public:
  using impl
    = detail::secrets::secret_common<detail::secrets::viewing_fbs_buffer>;
  using impl::impl;

  secret_view(const secret& s);
};

inline auto materialize(secret_view v) -> secret {
  return secret{v.buffer};
}

template <class HashAlgorithm>
void hash_append(HashAlgorithm& h, const secret& s) {
  return hash_append(h, static_cast<const secret::impl&>(s));
}

template <class HashAlgorithm>
void hash_append(HashAlgorithm& h, const secret_view& s) {
  return hash_append(h, static_cast<const secret_view::impl&>(s));
}

} // namespace tenzir

namespace fmt {

template <>
struct formatter<::tenzir::secret> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const ::tenzir::secret&, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "***");
  }
};

template <>
struct formatter<::tenzir::secret_view> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const ::tenzir::secret_view&, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "***");
  }
};

} // namespace fmt

namespace tenzir {

inline auto to_string(const secret& s) -> std::string {
  return fmt::format("{}", s);
}

inline auto to_string(const secret_view& s) -> std::string {
  return fmt::format("{}", s);
}

template <class T>
struct secret_printer : printer_base<secret_printer<T>> {
  using attribute = T;

  template <class Iterator>
  bool print(Iterator& out, const attribute&) const {
    std::ranges::copy("***", out);
    return true;
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
