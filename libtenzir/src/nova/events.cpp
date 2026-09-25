//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/events.hpp"

#include "tenzir/nova/array_builder.hpp"

#include <string>
#include <string_view>

namespace tenzir::nova {

Events::Events(Array<Record> data, storage::BitMap mask, Meta meta)
  : data{std::move(data)}, mask{std::move(mask)}, meta{std::move(meta)} {
}

auto Events::Meta::make_empty(storage::Index length, std::string_view name)
  -> Meta {
  using ConstantString
    = storage::ConstantStorage<std::string, std::string_view>;
  return Meta{
    .name = Array<String>{ConstantString{length, std::string{name}}},
    .import_time = Array<Time>{storage::ConstantStorage<Time>{length, {}}},
    .internal = Array<Bool>{storage::BitMap{length, false}},
  };
}

auto subslice(Events const& events, storage::Index begin, storage::Index end)
  -> Events {
  TENZIR_ASSERT_LEQ(0, begin);
  TENZIR_ASSERT_LEQ(begin, end);
  TENZIR_ASSERT_LEQ(end, events.length());
  auto data = ArrayBuilder<Record>{};
  auto mask = storage::BitMap::Builder{};
  auto names = ArrayBuilder<String>{};
  auto import_times = ArrayBuilder<Time>{};
  auto internal = ArrayBuilder<Bool>{};
  for (auto i = begin; i < end; ++i) {
    auto record = data.record();
    for (auto [name, value] : events.data.get(i)) {
      append_row(record.field(name), value);
    }
    mask.emplace_back(events.mask.get(i));
    names.data(*events.meta.name.get(i));
    import_times.data(*events.meta.import_time.get(i));
    internal.data(*events.meta.internal.get(i));
  }
  return Events{data.finish(), mask.finish(),
                Events::Meta{names.finish(), import_times.finish(),
                             internal.finish()}};
}

} // namespace tenzir::nova
