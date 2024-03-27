//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/record_batch.h>

namespace tenzir::plugins::feather2 {
namespace {

auto parse_feather(generator<chunk_ptr> input, operator_control_plane& ctrl)
  -> generator<table_slice> {
  // TOOD: implement
  co_return;
}

auto print_feather(table_slice input, operator_control_plane& ctrl)
  -> generator<chunk_ptr> {
  // TOOD: implement
  auto batch = to_record_batch(input); // work with this instead
  co_return;
}

class feather2_parser final : public plugin_parser {
public:
  feather2_parser() = default;

  auto name() const -> std::string override {
    return "feather2";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_feather(std::move(input), ctrl);
  }

  friend auto inspect(auto& f, feather2_parser& x) -> bool {
    return f.object(x).fields();
  }
};

class feather2_printer final : public plugin_printer {
public:
  feather2_printer() = default;

  auto name() const -> std::string override {
    // FIXME: Rename this and the file to just feather.
    return "feather2";
  }

  auto instantiate([[maybe_unused]] type input_schema,
                   operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    return printer_instance::make(
      [&ctrl](table_slice slice) -> generator<chunk_ptr> {
        return print_feather(std::move(slice), ctrl);
      });
  }

  auto allows_joining() const -> bool override {
    return false;
  };

  friend auto inspect(auto& f, feather2_printer& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual parser_plugin<feather2_parser>,
                     public virtual printer_plugin<feather2_printer> {
  auto name() const -> std::string override {
    return "feather2";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"feather2", "https://docs.tenzir.com/next/"
                                              "formats/feather2"};
    parser.parse(p);
    return std::make_unique<feather2_parser>();
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{"feather2", "https://docs.tenzir.com/next/"
                                              "formats/feather2"};
    parser.parse(p);
    return std::make_unique<feather2_printer>();
  }
};

} // namespace
} // namespace tenzir::plugins::feather2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::feather2::plugin)
