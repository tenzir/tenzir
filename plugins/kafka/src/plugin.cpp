//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/operator.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/concept/parseable/tenzir/option_set.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/die.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <string>

namespace tenzir::plugins::kafka {
namespace {

class plugin final : public virtual loader_plugin<kafka_loader>,
                     public virtual saver_plugin<kafka_saver> {
public:
  auto initialize(const record& config,
                  const record& /* global_config */) -> caf::error override {
    config_ = config;
    if (!config_.contains("bootstrap.servers")) {
      config_["bootstrap.servers"] = "localhost";
    }
    if (!config_.contains("client.id")) {
      config_["client.id"] = "tenzir";
    }
    return caf::none;
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = loader_args{};
    auto options = std::optional<located<std::string>>{};
    parser.add("-t,--topic", args.topic, "<topic>");
    parser.add("-c,--count", args.count, "<n>");
    parser.add("-e,--exit", args.exit);
    parser.add("-o,--offset", args.offset, "<offset>");
    // We use -X because that's standard in Kafka applications, cf. kcat.
    parser.add("-X,--set", options, "<key=value>,...");
    parser.parse(p);
    if (args.offset) {
      if (!offset_parser()(args.offset->inner)) {
        diagnostic::error("invalid `--offset` value")
          .primary(args.offset->source)
          .note(
            "must be `beginning`, `end`, `store`, `<offset>` or `-<offset>`")
          .throw_();
      }
    }
    if (options) {
      args.options.source = options->source;
      if (!parsers::kvp_list(options->inner, args.options.inner)) {
        diagnostic::error("invalid list of key=value pairs")
          .primary(options->source)
          .throw_();
        return {};
      }
    }
    return std::make_unique<kafka_loader>(std::move(args), config_);
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = saver_args{};
    auto options = std::optional<located<std::string>>{};
    parser.add("-t,--topic", args.topic, "<topic>");
    parser.add("-k,--key", args.key, "<key>");
    parser.add("-T,--timestamp", args.timestamp, "<time>");
    // We use -X because that's standard in Kafka applications, cf. kcat.
    parser.add("-X,--set", options, "<key=value>,...");
    parser.parse(p);
    if (args.timestamp) {
      if (!parsers::time(args.timestamp->inner)) {
        diagnostic::error("could not parse `--timestamp` as time")
          .primary(args.timestamp->source)
          .throw_();
      }
    }
    if (options) {
      args.options.source = options->source;
      if (!parsers::kvp_list(options->inner, args.options.inner)) {
        diagnostic::error("invalid list of key=value pairs")
          .primary(options->source)
          .throw_();
        return {};
      }
    }
    return std::make_unique<kafka_saver>(std::move(args), config_);
  }

  auto name() const -> std::string override {
    return "kafka";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::kafka

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::plugin)
// TENZIR_REGISTER_PLUGIN(tenzir::plugins::kafka::save_plugin)
