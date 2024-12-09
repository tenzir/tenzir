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
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <pcap/pcap.h>

#include <chrono>

#include "operator.hpp"

namespace tenzir::plugins::nic {

namespace {

class plugin final : public virtual loader_plugin<nic_loader>,
                     public virtual operator_plugin<nics_operator>,
                     public virtual operator_factory_plugin {
public:
  auto initialize(const record& config,
                  const record& /* global_config */) -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto name() const -> std::string override {
    return "nic";
  }

  auto operator_name() const -> std::string override {
    return "nics";
  }

  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    argument_parser2::operator_("nics").parse(inv, ctx).ignore();
    return std::make_unique<nics_operator>();
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"nics", "https://docs.tenzir.com/"
                                          "operators/nics"};
    parser.parse(p);
    return std::make_unique<nics_operator>();
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = loader_args{};
    parser.add(args.iface, "<iface>");
    parser.add("-s,--snaplen", args.snaplen, "<count>");
    parser.add("-e,--emit-file-headers", args.emit_file_headers);
    parser.parse(p);
    return std::make_unique<nic_loader>(std::move(args));
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::nic

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nic::plugin)
