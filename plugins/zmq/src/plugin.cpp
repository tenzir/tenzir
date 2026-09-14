//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/chunk.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/uuid.hpp>

using namespace std::chrono_literals;

namespace tenzir::plugins::zmq {

namespace {

class registrar final : public plugin {
  auto name() const -> std::string override {
    return "zmq";
  }
};

} // namespace

} // namespace tenzir::plugins::zmq

TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::registrar)
