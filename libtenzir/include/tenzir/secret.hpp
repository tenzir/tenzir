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

  auto print_to(auto& it) const -> bool {
    it = fmt::format_to(it, "****");
    return true;
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

  auto bytes() const -> std::span<const std::byte> {
    return std::span{buffer.chunk()->data(), buffer.chunk()->size()};
  }

  auto prepend(std::string_view literal) const
    -> secret_common<owning_fbs_buffer>;
  auto append(std::string_view literal) const
    -> secret_common<owning_fbs_buffer>;
  auto append(const secret_common<viewing_fbs_buffer>& other) const
    -> secret_common<owning_fbs_buffer>;

  FlatbufferType buffer;
};

extern template class secret_common<owning_fbs_buffer>;
extern template class secret_common<viewing_fbs_buffer>;

template <typename BlobType>
auto to_string(const secret_common<BlobType>& v) {
  auto s = std::string{};
  auto it = std::back_inserter(s);
  TENZIR_ASSERT(v.print_to(it));
  return s;
}

template <typename T1, typename T2>
auto operator<=>(const secret_common<T1>& lhs, const secret_common<T2>& rhs)
  -> std::partial_ordering {
  const auto l = lhs.bytes();
  const auto r = rhs.bytes();
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

/// If we dont manually implement this, we run into some issues with the
/// recursive `hash_inspector`.
template <class HashAlgorithm, typename FlatbufferType>
void hash_append(HashAlgorithm& h, const secret_common<FlatbufferType>& s) {
  return hash_append(h, s.bytes());
}

} // namespace detail::secrets

/// @relates detail::secret::secret_common
class secret final
  : public detail::secrets::secret_common<child_flatbuffer<fbs::data::Secret>> {
public:
  using impl
    = detail::secrets::secret_common<child_flatbuffer<fbs::data::Secret>>;
  friend class secret_view;
  using impl::impl;

  secret(const impl& base) : impl{base} {
  }
  secret(impl&& base) : impl{std::move(base)} {
  }

  secret(std::string_view name, std::string_view operations, bool is_literal);

  static auto make_literal(std::string_view value) -> secret;
  static auto make_managed(std::string_view value) -> secret;
  static auto from_fb(const fbs::data::Secret*) -> secret;
};

/// @relates detail::secret::secret_common
/// TODO: Currently a `secret_view` is identical to a `secret`, because both use
/// the owning tenzir::flatbuffer wrapper. We ideally want a non-owning version
/// of `flatbuffer` that does not hold a `chunk_ptr`, but only the `Table*`.
class secret_view final
  : public detail::secrets::secret_common<child_flatbuffer<fbs::data::Secret>> {
public:
  using impl
    = detail::secrets::secret_common<child_flatbuffer<fbs::data::Secret>>;
  using impl::impl;

  secret_view(const impl& base) : impl{base} {
  }
  secret_view(impl&& base) : impl{std::move(base)} {
  }
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

template <typename BlobType>
struct formatter<tenzir::detail::secrets::secret_common<BlobType>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::detail::secrets::secret_common<BlobType>& secret,
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
