//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pcap.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <pcap/pcap.h>

#include "operator.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::nic {

namespace {

class load_plugin : public virtual operator_plugin2<nic_loader> {
  auto make(invocation inv, session ctx) const -> failure_or<operator_ptr> {
    // FIXME: Arg parser doesn't support uint32_t
    auto snaplen = std::optional<located<uint64_t>>{};
    auto args = loader_args{};
    auto parser = argument_parser2::operator_(name());
    parser.positional("iface", args.iface);
    parser.named("snaplen", snaplen);
    parser.named("emit_file_headers", args.emit_file_headers);
    TRY(parser.parse(inv, ctx));
    if (snaplen) {
      args.snaplen
        = {detail::narrow<uint32_t>(snaplen->inner), snaplen->source};
    }
    return std::make_unique<nic_loader>(std::move(args));
  }
};

class tql2_plugin : public virtual operator_plugin2<nics_operator> {
  auto name() const -> std::string override {
    return "tql2.nics";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<nics_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::nic

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nic::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::nic::tql2_plugin)
