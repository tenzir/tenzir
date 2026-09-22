//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir::nova;

TEST("subslice events preserves physical rows and masks") {
  auto data = ArrayBuilder<Record>{};
  auto names = ArrayBuilder<String>{};
  for (auto i = 0; i < 4; ++i) {
    data.record().field("x").data(static_cast<Int>(i));
  }
  names.data("name-0");
  names.data("name-1");
  names.data("name-2");
  names.data("name-3");
  auto mask = storage::BitMap::Builder{};
  mask.emplace_back(true);
  mask.emplace_back(false);
  mask.emplace_back(true);
  mask.emplace_back(false);
  auto events
    = Events{data.finish(), mask.finish(),
             Events::Meta{names.finish(),
                          Array<Time>{storage::ConstantStorage<Time>{4, {}}},
                          Array<Bool>{storage::BitMap{4, false}}}};

  auto result = subslice(events, 1, 3);

  CHECK_EQUAL(result.length(), 2);
  CHECK(not result.mask.get(0));
  CHECK(result.mask.get(1));
  CHECK_EQUAL(*result.meta.name.get(0), "name-1");
  CHECK_EQUAL(*result.meta.name.get(1), "name-2");
}
