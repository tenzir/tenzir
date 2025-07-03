//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/view3.hpp>

#include <ai/openai.h>
#include <ai/types/client.h>
#include <ai/types/generate_options.h>

namespace tenzir::plugins::ai {

namespace {

struct ai_args {
  std::string prompt;
  ast::field_path response_field;
};

class ai_operator final : public crtp_operator<ai_operator> {
public:
  ai_operator() = default;

  ai_operator(ai_args args) : args_(std::move(args)) {
  }

  auto operator()(generator<table_slice> x, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto client = ::ai::openai::create_client();
    const auto printer = json_printer{{}};
    for (auto&& slice : x) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto builder = series_builder{};
      for (auto event : values3(slice)) {
        auto buf = std::string{};
        auto it = std::back_inserter(buf);
        TENZIR_ASSERT(printer.print(it, event));
        auto prompt_string = "what is in this json object?\n" + buf;
        auto result = client.generate_text({
          ::ai::openai::models::kGpt4o,
          "You are a friendly assistant!",
          prompt_string,
        });
        TENZIR_ASSERT(result.is_success());
        builder.data(result.text);
      }
      auto responses = builder.finish_assert_one_array();
      auto output_slice
        = assign(args_.response_field, responses, slice, ctrl.diagnostics());
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
  ai_args args_;
};

class ai final : public virtual operator_plugin2<ai_operator> {
public:
  auto name() const -> std::string override {
    return "tql2.ai";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = ai_args{};
    auto p = argument_parser2::operator_("ai");
    p.positional("prompt", args.prompt);
    p.named("response_field", args.response_field);
    TRY(p.parse(inv, ctx));
    return std::make_unique<ai_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::ai

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ai::ai)
