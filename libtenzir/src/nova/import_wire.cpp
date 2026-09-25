//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#include "tenzir/nova/import_wire.hpp"

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/materialize.hpp"

#include <limits>
#include <type_traits>

namespace tenzir::nova {

namespace {

auto to_nova_data(data const& value) -> Result<Data, std::string> {
  return match(value, []<class T>(T const& item) -> Result<Data, std::string> {
    if constexpr (std::same_as<T, caf::none_t>) {
      return Data{Null{}};
    } else if constexpr (std::same_as<T, list>) {
      auto result = List{};
      result.reserve(item.size());
      for (auto const& element : item) {
        TRY(auto converted, to_nova_data(element));
        result.push_back(std::move(converted));
      }
      return Data{std::move(result)};
    } else if constexpr (std::same_as<T, record>) {
      auto result = Record{};
      for (auto const& [name, element] : item) {
        TRY(auto converted, to_nova_data(element));
        result.emplace(name, std::move(converted));
      }
      return Data{std::move(result)};
    } else if constexpr (data_type<T>) {
      return Data{item};
    } else {
      return Err{"unsupported value in import transport"};
    }
  });
}

} // namespace

auto to_import_wire(Events const& events) -> ImportWireBatch {
  auto result = ImportWireBatch{};
  auto const count = events.active_count();
  result.rows.reserve(count);
  result.names.reserve(count);
  result.import_times.reserve(count);
  result.internal.reserve(count);
  for (auto index : storage::true_bits(events.mask)) {
    auto row = record{};
    for (auto [name, value] : events.data.get(index)) {
      row.emplace(std::string{name}, materialize(value));
    }
    if (row.empty()) {
      continue;
    }
    result.rows.push_back(std::move(row));
    result.names.emplace_back(*events.meta.name.get(index));
    result.import_times.push_back(*events.meta.import_time.get(index));
    result.internal.push_back(*events.meta.internal.get(index));
  }
  return result;
}

auto from_import_wire(ImportWireBatch const& batch)
  -> Result<Events, std::string> {
  auto const size = batch.rows.size();
  if (size != batch.names.size() or size != batch.import_times.size()
      or size != batch.internal.size()) {
    return Err{"import transport metadata length differs from row count"};
  }
  if (size > static_cast<size_t>(std::numeric_limits<storage::Index>::max())) {
    return Err{"import transport batch is too large"};
  }
  auto records = ArrayBuilder<Record>{};
  auto names = ArrayBuilder<String>{};
  auto times = ArrayBuilder<Time>{};
  auto internals = ArrayBuilder<Bool>{};
  for (auto i = size_t{0}; i < size; ++i) {
    auto row = records.record();
    for (auto const& [name, value] : batch.rows[i]) {
      TRY(auto converted, to_nova_data(value));
      append_data(row.field(name), converted);
    }
    names.data(batch.names[i]);
    times.data(batch.import_times[i]);
    internals.data(batch.internal[i] != 0);
  }
  return Events{
    records.finish(), storage::BitMap{static_cast<storage::Index>(size), true},
    Events::Meta{names.finish(), times.finish(), internals.finish()}};
}

} // namespace tenzir::nova
