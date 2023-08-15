//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>

#include <fluent-bit/fluent-bit-minimal.h>

using namespace std::chrono_literals;

namespace tenzir::plugins::fluentbit {

namespace {

class fluentbit_loader final : public plugin_loader {
public:
  fluentbit_loader() = default;

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make = []() -> generator<chunk_ptr> {
      while (true) {
        // TODO: Implement
        co_yield {};
      }
    };
    return make();
  }

  auto to_string() const -> std::string override {
    auto result = name();
    return result;
  }

  auto name() const -> std::string override {
    return "fluent-bit";
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, fluentbit_loader& x) -> bool {
    return f.object(x).pretty_name("fluent-bit_loader").fields();
  }
};

class fluentbit_saver final : public plugin_saver {
public:
  fluentbit_saver() = default;

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    return [](chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0)
        return;
      // TODO: Implement
    };
  }

  auto name() const -> std::string override {
    return "fluent-bit";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, fluentbit_saver& x) -> bool {
    return f.object(x).pretty_name("fluent-bit_saver").fields();
  }
};

class plugin final : public virtual loader_plugin<fluentbit_loader>,
                     public virtual saver_plugin<fluentbit_saver> {
public:
  auto initialize(const record& config, const record& /* global_config */)
    -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/next/connectors/{}", name())};
    // TODO: Implement
    return std::make_unique<fluentbit_loader>();
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/next/connectors/{}", name())};
    // TODO: Implement
    return std::make_unique<fluentbit_saver>();
  }

  auto name() const -> std::string override {
    return "fluent-bit";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::fluentbit

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::plugin)
