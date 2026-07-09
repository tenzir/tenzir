//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval2.hpp"
#include "tenzir2/assert.hpp"
#include "tenzir2/type_system/array/access.hpp"
#include "tenzir2/type_system/array/record.hpp"

#include <algorithm>
#include <string>
#include <vector>

namespace tenzir2 {

namespace {

/// Assigns into an erased array, treating it as a (possibly missing) record.
auto assign_data(std::span<tenzir::ast::field_path::segment const> path,
                 array_<data> right, array_<data> input,
                 tenzir::diagnostic_handler& dh) -> array_<data>;

} // namespace

auto consume_path(std::span<tenzir::ast::field_path::segment const> path,
                  array_<data> value) -> array_<data> {
  if (path.empty()) {
    return value;
  }
  auto inner = consume_path(path.subspan(1), std::move(value));
  auto names = std::vector<std::string>{std::string{path[0].id.name}};
  auto arrays = std::vector<array_<data>>{};
  arrays.push_back(std::move(inner));
  return array_<record>{std::move(names), std::move(arrays)};
}

auto assign(std::span<tenzir::ast::field_path::segment const> path,
            array_<data> right, array_<record> input,
            tenzir::diagnostic_handler& dh) -> array_<record> {
  // A record target always has at least one segment; whole-record replacement
  // is handled at the `array_<data>` level by `assign_data`.
  TENZIR2_ASSERT(not path.empty());
  auto const& head = path[0].id.name;
  // Copy out the existing top-level fields.
  auto names = std::vector<std::string>{};
  auto arrays = std::vector<array_<data>>{};
  names.reserve(input.num_fields());
  arrays.reserve(input.num_fields());
  for (auto i = std::size_t{0}; i < input.num_fields(); ++i) {
    names.emplace_back(input.name(i));
    arrays.emplace_back(input.value_array(i));
  }
  auto const it = std::ranges::find(names, head);
  if (it != names.end()) {
    auto const idx = static_cast<std::size_t>(it - names.begin());
    arrays[idx]
      = assign_data(path.subspan(1), std::move(right), arrays[idx], dh);
  } else {
    names.emplace_back(head);
    arrays.push_back(consume_path(path.subspan(1), std::move(right)));
  }
  return array_<record>{std::move(names), std::move(arrays)};
}

namespace {

auto assign_data(std::span<tenzir::ast::field_path::segment const> path,
                 array_<data> right, array_<data> input,
                 tenzir::diagnostic_handler& dh) -> array_<data> {
  if (path.empty()) {
    return right;
  }
  // Descend into `input`, which we expect to be a record. If it is anything
  // else, we overwrite it with an implicit record (mirroring the legacy
  // `tenzir::assign` behavior) and warn.
  return access::transform(
    std::move(input), [&]<typename T>(array_<T>&& arr) -> array_<data> {
      if constexpr (std::same_as<T, record>) {
        return assign(path, std::move(right), std::move(arr), dh);
      } else {
        if constexpr (not std::same_as<T, null>) {
          tenzir::diagnostic::warning("implicit record for `{}` field "
                                      "overwrites existing value",
                                      path[0].id.name)
            .primary(path[0].id)
            .hint("if this is intentional, drop the parent field first")
            .emit(dh);
        }
        return consume_path(path, std::move(right));
      }
    });
}

} // namespace

} // namespace tenzir2
