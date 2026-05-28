//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/async/scope.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/concept/printable/tenzir/json_printer_options.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/openai.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/view3.hpp>

#include <caf/none.hpp>
#include <fmt/format.h>

#include <chrono>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::ai_prompt {
namespace {

constexpr auto default_endpoint = std::string_view{"http://127.0.0.1:11434/v1"};

auto default_result_field() -> ast::field_path {
  auto expr = ast::expression{
    ast::root_field{ast::identifier{"ai", location::unknown}}};
  expr = ast::expression{ast::field_access{
    std::move(expr),
    location::unknown,
    false,
    ast::identifier{"prompt", location::unknown},
  }};
  auto result = ast::field_path::try_from(std::move(expr));
  TENZIR_ASSERT(result);
  return std::move(*result);
}

struct PromptArgs {
  located<std::string> model;
  Option<located<secret>> endpoint;
  Option<located<std::string>> system;
  Option<ast::expression> data;
  ast::field_path into = default_result_field();
  Option<located<secret>> api_key;
  located<double> temperature{0.0, location::unknown};
  Option<located<uint64_t>> max_tokens;
  located<duration> timeout{std::chrono::seconds{30}, location::unknown};
  located<uint64_t> concurrency{1, location::unknown};
  Option<located<tenzir::data>> tls;
  location operator_location = location::unknown;
};

struct RowResult {
  Option<std::string> input = None{};
  Option<openai::ResponsesResult> response = None{};
  Option<std::string> error = None{};
};

auto data_expr(PromptArgs const& args) -> ast::expression {
  if (args.data) {
    return *args.data;
  }
  return ast::expression{ast::this_{args.operator_location}};
}

auto print_json(data_view3 value) -> Result<std::string, std::string> {
  static auto const options = json_printer_options{
    .style = no_style(),
    .oneline = true,
  };
  static auto const printer = json_printer{options};
  auto result = std::string{};
  auto out = std::back_inserter(result);
  if (not printer.print(out, value)) {
    return Err{std::string{"failed to serialize data as JSON"}};
  }
  return result;
}

auto make_input_data(PromptArgs const& args, table_slice const& input,
                     diagnostic_handler& dh) -> std::vector<RowResult> {
  auto results = std::vector<RowResult>{};
  results.resize(detail::narrow<size_t>(input.rows()));
  auto values = eval(data_expr(args), input, dh);
  auto row = size_t{};
  for (auto value : values.values3()) {
    auto printed = print_json(value);
    if (printed.is_err()) {
      results[row].error = std::move(printed).unwrap_err();
    } else {
      results[row].input = std::move(printed).unwrap();
    }
    ++row;
  }
  TENZIR_ASSERT(row == results.size());
  return results;
}

auto append_usage(record_ref& row, Option<openai::TokenUsage> const& usage)
  -> void {
  if (not usage) {
    row.field("usage", caf::none);
    return;
  }
  auto usage_record = row.field("usage").record();
  if (usage->input_tokens) {
    usage_record.field("input_tokens", *usage->input_tokens);
  } else {
    usage_record.field("input_tokens", caf::none);
  }
  if (usage->output_tokens) {
    usage_record.field("output_tokens", *usage->output_tokens);
  } else {
    usage_record.field("output_tokens", caf::none);
  }
  if (usage->total_tokens) {
    usage_record.field("total_tokens", *usage->total_tokens);
  } else {
    usage_record.field("total_tokens", caf::none);
  }
}

auto append_response(series_builder& builder,
                     openai::ResponsesResult const& response) -> void {
  auto row = builder.record();
  row.field("text", response.text);
  if (auto parsed = from_json(response.text)) {
    row.field("value", *parsed);
  } else {
    row.field("value", caf::none);
  }
  if (response.model) {
    row.field("model", *response.model);
  } else {
    row.field("model", caf::none);
  }
  if (response.status) {
    row.field("status", *response.status);
  } else {
    row.field("status", caf::none);
  }
  append_usage(row, response.usage);
  row.field("latency", response.latency);
}

class Prompt final : public Operator<table_slice, table_slice> {
public:
  explicit Prompt(PromptArgs args) : args_{std::move(args)} {
  }

  Prompt(Prompt const&) = delete;
  auto operator=(Prompt const&) -> Prompt& = delete;
  Prompt(Prompt&&) noexcept = default;
  auto operator=(Prompt&&) noexcept -> Prompt& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    if (args_.model.inner.empty()) {
      diagnostic::error("`model` must not be empty")
        .primary(args_.model)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (args_.concurrency.inner == 0) {
      diagnostic::error("`concurrency` must be greater than zero")
        .primary(args_.concurrency)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (args_.timeout.inner < duration::zero()) {
      diagnostic::error("`timeout` must not be negative")
        .primary(args_.timeout)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto endpoint = std::string{default_endpoint};
    auto requests = std::vector<secret_request>{};
    auto endpoint_location = args_.operator_location;
    if (args_.endpoint) {
      endpoint_location = args_.endpoint->source;
      requests.push_back(
        make_secret_request("endpoint", *args_.endpoint, endpoint, ctx.dh()));
    }
    auto api_key = std::string{};
    if (args_.api_key) {
      requests.push_back(
        make_secret_request("api_key", *args_.api_key, api_key, ctx.dh()));
    }
    if (not requests.empty()) {
      auto resolved = co_await ctx.resolve_secrets(std::move(requests));
      if (resolved.is_error()) {
        done_ = true;
        co_return;
      }
    }
    if (endpoint.empty()) {
      diagnostic::error("`endpoint` must not be empty")
        .primary(endpoint_location)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
      args_.timeout.inner);
    auto config = http::make_http_pool_config(
      args_.tls, endpoint, endpoint_location, ctx.dh(), timeout,
      std::addressof(ctx.actor_system().config()));
    if (config.is_error()) {
      done_ = true;
      co_return;
    }
    auto responses_url = openai::make_responses_url(endpoint);
    if (responses_url.is_err()) {
      diagnostic::error("{}", std::move(responses_url).unwrap_err())
        .primary(endpoint_location)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto headers = std::vector<http::Header>{};
    if (not api_key.empty()) {
      headers.push_back({"Authorization", fmt::format("Bearer {}", api_key)});
    }
    try {
      auto pool
        = HttpPool::make(ctx.io_executor(), std::move(responses_url).unwrap(),
                         std::move(*config));
      client_.emplace(std::in_place, std::move(pool), std::move(headers));
    } catch (std::exception const& e) {
      diagnostic::error("failed to initialize HTTP client: {}", e.what())
        .primary(endpoint_location)
        .emit(ctx);
      done_ = true;
      co_return;
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_ or input.rows() == 0) {
      co_return;
    }
    TENZIR_ASSERT(client_);
    auto rows = make_input_data(args_, input, ctx.dh());
    auto permits = Semaphore{detail::narrow<size_t>(args_.concurrency.inner)};
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      for (auto i = size_t{}; i < rows.size(); ++i) {
        if (not rows[i].input) {
          continue;
        }
        scope.spawn([&, i]() -> Task<void> {
          auto permit = co_await permits.acquire();
          auto request = openai::ResponsesRequest{
            .model = args_.model.inner,
            .instructions = args_.system.map([](auto const& x) {
              return x.inner;
            }),
            .input = std::move(*rows[i].input),
            .temperature = args_.temperature.inner,
            .max_output_tokens = args_.max_tokens.map([](auto const& x) {
              return x.inner;
            }),
          };
          auto response = co_await (*client_)->create(std::move(request));
          permit.release();
          if (response.is_err()) {
            rows[i].error = std::move(response).unwrap_err();
            co_return;
          }
          rows[i].response = std::move(response).unwrap();
          rows[i].error = None{};
        });
      }
      co_return;
    });
    auto builder = series_builder{};
    for (auto& row : rows) {
      if (row.response) {
        append_response(builder, *row.response);
        continue;
      }
      builder.null();
      if (row.error) {
        auto diag = diagnostic::warning("AI request failed: {}", *row.error)
                      .primary(args_.operator_location);
        if (not args_.endpoint) {
          diag = std::move(diag)
                   .note("endpoint: {}/responses", default_endpoint)
                   .hint("the default endpoint targets Ollama; check that "
                         "Ollama is running and the model name is valid, or "
                         "set `endpoint=...`");
        }
        std::move(diag).emit(ctx);
      }
    }
    auto slice_start = size_t{};
    for (auto&& part : builder.finish()) {
      auto slice_end = slice_start + detail::narrow<size_t>(part.length());
      auto output = assign(args_.into, std::move(part),
                           subslice(input, slice_start, slice_end), ctx.dh());
      co_await push(std::move(output));
      slice_start = slice_end;
    }
    TENZIR_ASSERT(slice_start == detail::narrow<size_t>(input.rows()));
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  PromptArgs args_;
  Option<Box<openai::ResponsesClient>> client_ = None{};
  bool done_ = false;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "ai::prompt";
  }

  auto describe() const -> Description override {
    auto d = Describer<PromptArgs, Prompt>{};
    d.named("model", &PromptArgs::model);
    d.named("endpoint", &PromptArgs::endpoint, "string");
    d.named("system", &PromptArgs::system);
    d.named("data", &PromptArgs::data, "any");
    d.named_optional("into", &PromptArgs::into);
    d.named("api_key", &PromptArgs::api_key, "string");
    d.named_optional("temperature", &PromptArgs::temperature);
    d.named("max_tokens", &PromptArgs::max_tokens);
    d.named_optional("timeout", &PromptArgs::timeout);
    d.named_optional("concurrency", &PromptArgs::concurrency);
    d.named("tls", &PromptArgs::tls, "record");
    d.operator_location(&PromptArgs::operator_location);
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::ai_prompt

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ai_prompt::plugin)
