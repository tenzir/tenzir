//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/view3.hpp"
#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

#include <tenzir/tql2/plugin.hpp>

#include <ai/openai.h>
#include <ai/types/client.h>
#include <ai/types/generate_options.h>

namespace tenzir::plugins::ai {

namespace {

struct operator_args {
  std::string prompt;
  ast::field_path response_field;
};

// Does nothing with the input.
class ai_operator final : public crtp_operator<ai_operator> {
public:
  ai_operator() = default;
  ai_operator(operator_args args) : args_(std::move(args)) {}

  auto operator()(generator<table_slice> x, operator_control_plane& ctrl) const -> generator<table_slice> {
    // Ensure OPENAI_API_KEY environment variable is set
    // TODO:
    auto client = ::ai::openai::create_client();

    auto options = json_printer_options{};
    auto printer = json_printer{options};
    for (auto&& slice : x) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto builder = series_builder{};
      for (auto event : values3(slice)) {
        std::string json_string;
        const auto printer = json_printer{json_printer_options{}};
        auto it = std::back_inserter(json_string);
        TENZIR_ASSERT(printer.print(it, event));
        auto prompt_string = "what is in this json object?\n" + json_string;
        auto result = client.generate_text({
            ::ai::openai::models::kGpt4o, // this can also be a string like "gpt-4o"
            "You are a friendly assistant!",
            prompt_string
        });
        TENZIR_ASSERT(result.is_success());
        builder.data(result.text);
      }
      auto responses = builder.finish_assert_one_array();
      auto output_slice = assign(args_.response_field, responses, slice, ctrl.diagnostics());
      co_yield output_slice;
    }
  }

  auto name() const -> std::string override {
    return "ai";
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, ai_operator& x) -> bool {
    return f.object(x).fields();
  }

  private:
  operator_args args_;
};

class plugin final : public virtual operator_plugin2<ai_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    operator_args args;
    auto parser = argument_parser2::operator_("ai")
      .positional("prompt", args.prompt)
      .named("response_field", args.response_field)
      //.named("compiled_rules", args.compiled_rules)
      ;
    parser.parse(inv, ctx).ignore();
    return std::make_unique<ai_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::ai

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ai::plugin)
