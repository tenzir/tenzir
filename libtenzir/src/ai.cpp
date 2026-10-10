//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/ai.hpp>
#include <tenzir/async/scope.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/concept/printable/tenzir/json_printer_options.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/nova_json_printer.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/view3.hpp>

#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <fmt/format.h>

#include <array>
#include <chrono>
#include <exception>

namespace tenzir::ai {
namespace {

auto make_path(std::span<std::string_view const> names, bool optional)
  -> ast::field_path {
  TENZIR_ASSERT(not names.empty());
  auto expr = ast::expression{ast::root_field{
    ast::identifier{std::string{names.front()}, location::unknown},
    optional,
  }};
  for (auto name : names.subspan(1)) {
    expr = ast::expression{ast::field_access{
      std::move(expr),
      location::unknown,
      optional,
      ast::identifier{std::string{name}, location::unknown},
    }};
  }
  auto result = ast::field_path::try_from(std::move(expr));
  TENZIR_ASSERT(result);
  return std::move(*result);
}

/// Creates the HTTP pool. The pool throws for URLs that it cannot handle, and
/// its message contains the URL, so the endpoint stays out of the diagnostic.
auto make_pool(OpCtx& ctx, std::string url, HttpPoolConfig config,
               location source) -> failure_or<Box<HttpPool>> {
  try {
    return HttpPool::make(ctx.io_executor(), std::move(url), std::move(config));
  } catch (std::exception const&) {
    diagnostic::error("`endpoint` is not a valid URL").primary(source).emit(ctx);
    return failure::promise();
  }
}

} // namespace

auto default_into(std::string_view name) -> ast::field_path {
  auto names = std::array{result_field, name};
  return make_path(names, false);
}

auto check_arguments(Option<located<std::string>> const& model,
                     Option<located<uint64_t>> const& concurrency,
                     Option<located<duration>> const& timeout,
                     diagnostic_handler& dh) -> void {
  if (model and model->inner.empty()) {
    diagnostic::error("`model` must not be empty").primary(*model).emit(dh);
  }
  if (concurrency and concurrency->inner == 0) {
    diagnostic::error("`concurrency` must be greater than zero")
      .primary(*concurrency)
      .emit(dh);
  }
  if (timeout and timeout->inner < duration::zero()) {
    diagnostic::error("`timeout` must not be negative")
      .primary(*timeout)
      .emit(dh);
  }
}

auto make_endpoint_url(std::string endpoint, std::string_view resource,
                       CompleteRoute complete)
  -> Result<std::string, std::string> {
  auto parsed = boost::urls::parse_uri(endpoint);
  if (not parsed) {
    return Err{
      fmt::format("failed to parse endpoint: {}", parsed.error().message())};
  }
  auto url = boost::urls::url{*parsed};
  if (url.scheme() != "http" and url.scheme() != "https") {
    return Err{std::string{"endpoint must use HTTP or HTTPS"}};
  }
  if (url.host().empty()) {
    return Err{std::string{"endpoint must include a host"}};
  }
  if (url.has_port() and url.port_number() == 0) {
    return Err{std::string{"endpoint must use a port between 1 and 65535"}};
  }
  auto path = std::string{url.path()};
  while (path.ends_with('/')) {
    path.pop_back();
  }
  auto suffix = fmt::format("/{}", resource);
  if (not path.ends_with(suffix) and not(complete and complete(path))) {
    path += suffix;
  }
  url.set_path(path);
  return std::string{url.buffer()};
}

auto connect(located<secret> endpoint, Option<located<secret>> api_key,
             std::string_view resource, duration timeout,
             Option<located<data>> const& tls, OpCtx& ctx,
             CompleteRoute complete) -> Task<failure_or<Connection>> {
  auto resolved_endpoint = std::string{};
  auto resolved_api_key = std::string{};
  auto requests = std::vector<secret_request>{};
  requests.push_back(
    make_secret_request("endpoint", endpoint, resolved_endpoint, ctx.dh()));
  if (api_key) {
    requests.push_back(
      make_secret_request("api_key", *api_key, resolved_api_key, ctx.dh()));
  }
  CO_TRY(co_await ctx.resolve_secrets(std::move(requests)));
  if (resolved_endpoint.empty()) {
    diagnostic::error("`endpoint` must not be empty")
      .primary(endpoint.source)
      .emit(ctx);
    co_return failure::promise();
  }
  auto config = http::make_http_pool_config(
    tls, resolved_endpoint, endpoint.source, ctx.dh(),
    std::chrono::duration_cast<std::chrono::milliseconds>(timeout),
    ctx.actor_system().config());
  if (config.is_error()) {
    co_return failure::promise();
  }
  auto url
    = make_endpoint_url(std::move(resolved_endpoint), resource, complete);
  if (url.is_err()) {
    diagnostic::error("{}", std::move(url).unwrap_err())
      .primary(endpoint.source)
      .emit(ctx);
    co_return failure::promise();
  }
  auto path = std::string{};
  if (auto parsed = boost::urls::parse_uri(url.unwrap())) {
    path = std::string{parsed->path()};
  }
  CO_TRY(auto pool, make_pool(ctx, std::move(url).unwrap(), std::move(*config),
                              endpoint.source));
  auto headers = std::vector<http::Header>{};
  if (not resolved_api_key.empty()) {
    headers.push_back(
      {"Authorization", fmt::format("Bearer {}", resolved_api_key)});
  }
  co_return Connection{
    .pool = std::move(pool),
    .headers = std::move(headers),
    .path = std::move(path),
  };
}

auto default_input_exclusions(ast::field_path const& into)
  -> std::vector<ast::field_path> {
  auto result = std::vector<ast::field_path>{};
  auto ai = std::array{result_field};
  result.push_back(make_path(ai, true));
  auto path = into.path();
  if (not path.empty() and path.front().id.name != result_field) {
    auto names = std::vector<std::string_view>{};
    for (auto const& segment : path) {
      names.push_back(segment.id.name);
    }
    result.push_back(make_path(names, true));
  }
  return result;
}

auto InputPrinter::make(Option<ast::expression> expr,
                        ast::field_path const& into, OpCtx& ctx)
  -> Task<failure_or<InputPrinter>> {
  auto result = InputPrinter{};
  if (expr) {
    CO_TRY(auto evaluator,
           co_await nova::Evaluator::make(std::move(*expr), ctx));
    result.evaluator_.emplace(std::move(evaluator));
  } else {
    result.exclusions_ = default_input_exclusions(into);
    result.drop_tree_ = nova::DropTree::make(result.exclusions_);
  }
  co_return result;
}

auto InputPrinter::print(nova::Events const& events, diagnostic_handler& dh)
  -> std::vector<std::string> {
  auto values
    = evaluator_
        ? evaluator_->eval(events, nova::EvalCtx{dh})
        : nova::Array<nova::Data>{drop_tree_.apply(events.data, events.mask)};
  auto printer = nova::json_printer{{
    .style = no_style(),
    .oneline = true,
  }};
  auto result = std::vector<std::string>{};
  result.reserve(detail::narrow<size_t>(events.active_count()));
  for (auto row : nova::storage::true_bits(events.mask)) {
    printer.print(values.get(row));
    auto bytes = printer.bytes();
    result.emplace_back(reinterpret_cast<char const*>(bytes.data()),
                        bytes.size());
  }
  return result;
}

auto request_all(size_t count, uint64_t concurrency,
                 std::function<Task<void>(size_t)> request) -> Task<void> {
  auto permits = Semaphore{detail::narrow<size_t>(concurrency)};
  // All requests finish before returning, leaving no pending checkpoint state.
  co_await async_scope([&](AsyncScope& scope) -> Task<void> {
    for (auto i = size_t{}; i < count; ++i) {
      // Acquiring the permit before spawning bounds the number of tasks, not
      // only the number of requests in flight.
      auto permit = co_await permits.acquire();
      scope.spawn([&, i, permit = std::move(permit)]() mutable -> Task<void> {
        co_await request(i);
        permit.release();
      });
    }
  });
}

auto request_failed(std::string_view error, location source)
  -> diagnostic_builder {
  return diagnostic::warning("AI request failed: {}", error).primary(source);
}

auto assign_results(nova::Events& events, ast::field_path const& into,
                    nova::MaskedArray<nova::Array<nova::Data>> results,
                    diagnostic_handler& dh) -> void {
  if (into.path().empty()) {
    events.data = nova::records_or_empty(std::move(results), events.length(),
                                         into.get_location(), dh);
    return;
  }
  events.data = nova::assign_nested_field(std::move(events.data), into.path(),
                                          std::move(results), dh);
}

auto print_inputs(Option<ast::expression> const& expr,
                  ast::field_path const& into, location self,
                  table_slice const& input, diagnostic_handler& dh)
  -> std::vector<Option<std::string>> {
  auto values = expr
                  ? eval(*expr, input, dh)
                  : eval(ast::expression{ast::this_{self}},
                         drop(input, default_input_exclusions(into), dh), dh);
  static auto const printer = json_printer{json_printer_options{
    .style = no_style(),
    .oneline = true,
  }};
  auto result = std::vector<Option<std::string>>{};
  result.reserve(detail::narrow<size_t>(input.rows()));
  for (auto value : values.values3()) {
    auto json = std::string{};
    auto out = std::back_inserter(json);
    if (printer.print(out, value)) {
      result.emplace_back(std::move(json));
    } else {
      result.emplace_back(None{});
    }
  }
  TENZIR_ASSERT(result.size() == detail::narrow<size_t>(input.rows()));
  return result;
}

auto assign_results(table_slice const& input, ast::field_path const& into,
                    series_builder results, diagnostic_handler& dh)
  -> std::vector<table_slice> {
  auto output = std::vector<table_slice>{};
  auto begin = size_t{};
  for (auto&& part : results.finish()) {
    auto end = begin + detail::narrow<size_t>(part.length());
    output.push_back(
      assign(into, std::move(part), subslice(input, begin, end), dh));
    begin = end;
  }
  TENZIR_ASSERT(begin == detail::narrow<size_t>(input.rows()));
  return output;
}

} // namespace tenzir::ai
