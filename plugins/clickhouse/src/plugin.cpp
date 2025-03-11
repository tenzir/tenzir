//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

namespace tenzir::plugins::clickhouse {

namespace {

class clickhouse_stub final : public virtual plugin {
public:
  auto name() const -> std::string override {
    return "clickhouse";
  }
};

} // namespace

} // namespace tenzir::plugins::clickhouse

TENZIR_REGISTER_PLUGIN(tenzir::plugins::clickhouse::clickhouse_stub)
