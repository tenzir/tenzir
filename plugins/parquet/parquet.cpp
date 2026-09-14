//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/drain_bytes.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::parquet {

namespace {

class stub final : public virtual plugin {
public:
  auto name() const -> std::string override {
    return "parquet";
  }
};

} // namespace

} // namespace tenzir::plugins::parquet

// Finally, register our plugin.
TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::stub)
