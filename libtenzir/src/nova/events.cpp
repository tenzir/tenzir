//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/events.hpp"

#include <string>
#include <string_view>

namespace tenzir::nova {

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

} // namespace tenzir::nova
