//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/uuid.hpp>

#include "operator.hpp"

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
