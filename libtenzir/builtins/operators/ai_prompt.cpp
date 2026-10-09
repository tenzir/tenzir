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
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/eval_util.hpp>
#include <tenzir/nova_json_printer.hpp>
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
#include <cmath>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::ai_prompt {
namespace {

constexpr auto default_endpoint = std::string_view{"http://127.0.0.1:11434/v1"};

struct PromptArgs {
  located<std::string> model;
  Option<located<secret>> endpoint;
  Option<located<std::string>> system;
  Option<ast::expression> data;
  ast::field_path into = [] {
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
  }();
  Option<located<secret>> api_key;
  Option<located<double>> temperature;
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

auto make_input_data(PromptArgs const& args, table_slice const& input,
                     diagnostic_handler& dh) -> std::vector<RowResult> {
  auto results = std::vector<RowResult>{};
  results.resize(detail::narrow<size_t>(input.rows()));
  auto expr = args.data ? *args.data
                        : ast::expression{ast::this_{args.operator_location}};
  auto values = eval(std::move(expr), input, dh);
  static auto const options = json_printer_options{
    .style = no_style(),
    .oneline = true,
  };
  static auto const printer = json_printer{options};
  auto row = size_t{};
  for (auto value : values.values3()) {
    auto result = std::string{};
    auto out = std::back_inserter(result);
    if (not printer.print(out, value)) {
      results[row].error = std::string{"failed to serialize data as JSON"};
    } else {
      results[row].input = std::move(result);
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
  if (response.model) {
    row.field("model", *response.model);
  } else {
    row.field("model", caf::none);
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
    auto config
      = http::make_http_pool_config(args_.tls, endpoint, endpoint_location,
                                    ctx.dh(), timeout,
                                    ctx.actor_system().config());
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
    auto pool = HttpPool::make(
      ctx.io_executor(), std::move(responses_url).unwrap(), std::move(*config));
    client_.emplace(std::in_place, std::move(pool), std::move(headers));
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_) {
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
            .temperature = args_.temperature.map([](auto const& x) {
              return x.inner;
            }),
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

auto append_usage(nova::ArrayBuilder<nova::Record>::RecordBuilder row,
                  Option<openai::TokenUsage> const& usage) -> void {
  if (not usage) {
    row.field("usage").null();
    return;
  }
  auto usage_record = row.field("usage").record();
  if (usage->input_tokens) {
    usage_record.field("input_tokens").data(*usage->input_tokens);
  } else {
    usage_record.field("input_tokens").null();
  }
  if (usage->output_tokens) {
    usage_record.field("output_tokens").data(*usage->output_tokens);
  } else {
    usage_record.field("output_tokens").null();
  }
  if (usage->total_tokens) {
    usage_record.field("total_tokens").data(*usage->total_tokens);
  } else {
    usage_record.field("total_tokens").null();
  }
}

auto append_response(nova::ArrayBuilder<nova::Data>& builder,
                     openai::ResponsesResult const& response) -> void {
  auto row = builder.record();
  row.field("text").data(response.text);
  if (response.model) {
    row.field("model").data(*response.model);
  } else {
    row.field("model").null();
  }
  append_usage(row, response.usage);
  row.field("latency").data(response.latency);
}

class PromptNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit PromptNova(PromptArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (args_.model.inner.empty()) {
      diagnostic::error("`model` must not be empty")
        .primary(args_.model)
        .emit(ctx);
      co_return;
    }
    if (args_.concurrency.inner == 0) {
      diagnostic::error("`concurrency` must be greater than zero")
        .primary(args_.concurrency)
        .emit(ctx);
      co_return;
    }
    if (args_.timeout.inner < duration::zero()) {
      diagnostic::error("`timeout` must not be negative")
        .primary(args_.timeout)
        .emit(ctx);
      co_return;
    }
    auto expr = args_.data
                  ? std::move(*args_.data)
                  : ast::expression{ast::this_{args_.operator_location}};
    auto evaluator = co_await nova::Evaluator::make(std::move(expr), ctx);
    if (not evaluator) {
      co_return;
    }
    evaluator_.emplace(std::move(*evaluator));
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
        co_return;
      }
    }
    if (endpoint.empty()) {
      diagnostic::error("`endpoint` must not be empty")
        .primary(endpoint_location)
        .emit(ctx);
      co_return;
    }
    auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
      args_.timeout.inner);
    auto config
      = http::make_http_pool_config(args_.tls, endpoint, endpoint_location,
                                    ctx.dh(), timeout,
                                    ctx.actor_system().config());
    if (config.is_error()) {
      co_return;
    }
    auto responses_url = openai::make_responses_url(endpoint);
    if (responses_url.is_err()) {
      diagnostic::error("{}", std::move(responses_url).unwrap_err())
        .primary(endpoint_location)
        .emit(ctx);
      co_return;
    }
    auto headers = std::vector<http::Header>{};
    if (not api_key.empty()) {
      headers.push_back({"Authorization", fmt::format("Bearer {}", api_key)});
    }
    auto pool = HttpPool::make(
      ctx.io_executor(), std::move(responses_url).unwrap(), std::move(*config));
    client_.emplace(std::in_place, std::move(pool), std::move(headers));
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not client_ or not evaluator_ or not input.mask.any()) {
      co_return;
    }
    auto values = evaluator_->eval(input, nova::EvalCtx{ctx.dh()});
    auto printer = nova::json_printer{{
      .style = no_style(),
      .oneline = true,
    }};
    auto rows = std::vector<RowResult>{};
    rows.reserve(detail::narrow<size_t>(input.active_count()));
    for (auto row : nova::storage::true_bits(input.mask)) {
      printer.print(values.get(row));
      auto bytes = printer.bytes();
      rows.push_back(RowResult{
        .input = std::string{reinterpret_cast<char const*>(bytes.data()),
                             bytes.size()},
      });
    }
    auto permits = Semaphore{detail::narrow<size_t>(args_.concurrency.inner)};
    // All requests finish before returning, leaving no pending checkpoint state.
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      for (auto i = size_t{}; i < rows.size(); ++i) {
        scope.spawn([&, i]() -> Task<void> {
          auto permit = co_await permits.acquire();
          auto request = openai::ResponsesRequest{
            .model = args_.model.inner,
            .instructions = args_.system.map([](auto const& x) {
              return x.inner;
            }),
            .input = std::move(*rows[i].input),
            .temperature = args_.temperature.map([](auto const& x) {
              return x.inner;
            }),
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
        });
      }
      co_return;
    });
    auto builder = nova::ArrayBuilder<nova::Data>{};
    auto i = size_t{};
    for (auto row : nova::storage::true_bits(input.mask)) {
      builder.skip_n(row - builder.length());
      auto const& result = rows[i++];
      if (result.response) {
        append_response(builder, *result.response);
        continue;
      }
      builder.null();
      if (result.error) {
        auto diag = diagnostic::warning("AI request failed: {}", *result.error)
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
    builder.skip_n(input.length() - builder.length());
    auto value = nova::MaskedArray<nova::Array<nova::Data>>{builder.finish(),
                                                            input.mask};
    if (args_.into.path().empty()) {
      input.data = nova::records_or_empty(std::move(value), input.length(),
                                          args_.into.get_location(), ctx.dh());
    } else {
      input.data = nova::assign_nested_field(
        std::move(input.data), args_.into.path(), std::move(value), ctx.dh());
    }
    co_await push(std::move(input));
  }

private:
  PromptArgs args_;
  Option<nova::Evaluator> evaluator_;
  Option<Box<openai::ResponsesClient>> client_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "ai_prompt";
  }

  auto describe() const -> Description override {
    auto d = Describer<PromptArgs, Prompt, PromptNova>{};
    d.named("model", &PromptArgs::model);
    d.named("endpoint", &PromptArgs::endpoint, "string");
    d.named("system", &PromptArgs::system);
    d.named("data", &PromptArgs::data, "any");
    d.named_optional("into", &PromptArgs::into);
    d.named("api_key", &PromptArgs::api_key, "string");
    auto temperature = d.named("temperature", &PromptArgs::temperature);
    d.named("max_tokens", &PromptArgs::max_tokens);
    d.named_optional("timeout", &PromptArgs::timeout);
    d.named_optional("concurrency", &PromptArgs::concurrency);
    d.named("tls", &PromptArgs::tls, "record");
    d.operator_location(&PromptArgs::operator_location);
    d.validate([temperature](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(temperature)) {
        if (not std::isfinite(value->inner) or value->inner < 0.0
            or value->inner > 2.0) {
          diagnostic::error("`temperature` must be between 0 and 2")
            .primary(*value)
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::ai_prompt

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ai_prompt::plugin)
