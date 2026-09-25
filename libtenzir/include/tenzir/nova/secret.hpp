// SPDX-License-Identifier: BSD-3-Clause
#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/print.hpp"
#include "tenzir/ecc.hpp"

#include <fmt/format.h>

#include <cstddef>
#include <iterator>
#include <ranges>
#include <span>
#include <string_view>
#include <utility>

namespace tenzir::nova {

/// PLAIN TEXT secret value that must not be serialized or printed.
struct Secret {
  ecc::cleansing_blob data;

  /// Appends `bytes`, like `std::vector::append_range`.
  template <std::ranges::input_range Range>
    requires std::convertible_to<std::ranges::range_reference_t<Range>,
                                 std::byte>
  auto append_range(Range&& bytes) -> void {
    data.append_range(std::forward<Range>(bytes));
  }
};

/// View of a PLAIN TEXT secret value that must not be serialized or printed.
///
/// Mirrors the read-only interface of `std::string_view` and `blob_view` so
/// that generic byte-sequence code can handle all three alike. It deliberately
/// does not derive from `std::span`, which would make it implicitly
/// convertible to `blob_view`.
class SecretView {
public:
  using value_type = std::byte;

  SecretView() = default;

  SecretView(const std::byte* data, std::size_t size) : bytes_{data, size} {
  }

  template <std::contiguous_iterator It, std::sized_sentinel_for<It> End>
    requires std::same_as<std::iter_value_t<It>, std::byte>
  SecretView(It first, End last) : bytes_{first, last} {
  }

  SecretView(const Secret& secret) : bytes_{secret.data} {
  }

  auto data() const -> const std::byte* {
    return bytes_.data();
  }

  auto size() const -> std::size_t {
    return bytes_.size();
  }

  auto empty() const -> bool {
    return bytes_.empty();
  }

  auto begin() const -> const std::byte* {
    return bytes_.data();
  }

  auto end() const -> const std::byte* {
    return bytes_.data() + bytes_.size();
  }

private:
  std::span<const std::byte> bytes_;
};

} // namespace tenzir::nova

namespace fmt {

template <>
struct formatter<tenzir::nova::SecretView> : formatter<std::string_view> {
  auto format(const tenzir::nova::SecretView&, format_context& ctx) const {
    return formatter<std::string_view>::format("***", ctx);
  }
};

} // namespace fmt

namespace tenzir {

struct nova_secret_view_printer : printer_base<nova_secret_view_printer> {
  using attribute = nova::SecretView;

  template <class Iterator>
  bool print(Iterator& out, const attribute&) const {
    for (const auto c : std::string_view{"***"}) {
      *out++ = c;
    }
    return true;
  }
};

template <>
struct printer_registry<nova::SecretView> {
  using type = nova_secret_view_printer;
};

} // namespace tenzir
