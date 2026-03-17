//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/async.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <boost/url/parse.hpp>
#include <folly/coro/Sleep.h>

#include <deque>
#include <unordered_map>
#include <utility>

namespace tenzir::plugins {
namespace {

using namespace std::literals;

auto to_chrono(duration d) -> std::chrono::milliseconds {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d);
}

struct ExecutorHttpRequest {
  std::string url;
  std::unordered_map<std::string, std::string> headers;
  bool is_pagination = false;
};

auto queue_executor_request(
  std::deque<ExecutorHttpRequest>& queue,
  std::unordered_map<std::string, std::string> const& headers,
  std::string next_url, bool tls_enabled, location const& op,
  diagnostic_handler& dh, severity diag_severity, std::string_view note = {})
  -> bool {
  http::normalize_http_url(next_url, tls_enabled);
  auto parsed = boost::urls::parse_uri_reference(next_url);
  if (not parsed) {
    if (diag_severity == severity::warning) {
      if (note.empty()) {
        diagnostic::warning("failed to parse uri: {}", next_url)
          .primary(op)
          .emit(dh);
      } else {
        diagnostic::warning("failed to parse uri: {}", next_url)
          .primary(op)
          .note("{}", note)
          .emit(dh);
      }
    } else {
      if (note.empty()) {
        diagnostic::error("failed to parse uri: {}", next_url)
          .primary(op)
          .emit(dh);
      } else {
        diagnostic::error("failed to parse uri: {}", next_url)
          .primary(op)
          .note("{}", note)
          .emit(dh);
      }
    }
    return false;
  }
  queue.push_back({std::move(next_url), headers, true});
  return true;
}

struct ToHttpArgs {
  location op = location::unknown;
  ast::expression url;
  std::optional<ast::expression> method;
  std::optional<ast::expression> body;
  std::optional<ast::expression> headers;
  std::optional<located<std::string>> encode;
  std::optional<located<std::string>> paginate;
  located<duration> paginate_delay{0s, location::unknown};
  located<uint64_t> parallel{1, location::unknown};
  std::optional<located<data>> tls;
  located<duration> connection_timeout{5s, location::unknown};
  located<uint64_t> max_retry_count{0, location::unknown};
  located<duration> retry_delay{1s, location::unknown};

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (encode) {
      if (encode->inner != "json" and encode->inner != "form") {
        diagnostic::error("unsupported encoding: `{}`", encode->inner)
          .primary(encode->source)
          .hint("must be `json` or `form`")
          .emit(dh);
        return failure::promise();
      }
      if (encode->inner == "form" and not body) {
        diagnostic::error("`encode=\"form\"` requires an explicit `body`")
          .primary(encode->source)
          .emit(dh);
        return failure::promise();
      }
    }
    if (retry_delay.inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(retry_delay)
        .emit(dh);
      return failure::promise();
    }
    if (paginate_delay.inner < duration::zero()) {
      diagnostic::error("`paginate_delay` must be a positive duration")
        .primary(paginate_delay)
        .emit(dh);
      return failure::promise();
    }
    if (connection_timeout.inner < duration::zero()) {
      diagnostic::error("`connection_timeout` must be a positive duration")
        .primary(connection_timeout)
        .emit(dh);
      return failure::promise();
    }
    if (parallel.inner == 0) {
      diagnostic::error("`parallel` must be not be zero")
        .primary(parallel)
        .emit(dh);
      return failure::promise();
    }
    if (paginate and paginate->inner != "link") {
      diagnostic::error("unsupported pagination mode: `{}`", paginate->inner)
        .primary(paginate->source)
        .hint("`paginate` must be `\"link\"`")
        .emit(dh);
      return failure::promise();
    }
    auto tls_opts = tls ? tls_options{*tls, {.tls_default = false}}
                        : tls_options{{.tls_default = false}};
    TRY(tls_opts.validate(dh));
    return {};
  }

  auto make_method(std::string_view method_name) const
    -> std::optional<std::string> {
    if (method_name.empty()) {
      // `to_http` always sends a body: either the explicit `body=` value or
      // the serialized input event. A missing/null per-row method should
      // therefore keep the operator's webhook-style POST default.
      return std::string{"POST"};
    }
    return http::normalize_http_method(method_name);
  }
};

class ToHttp final : public Operator<table_slice, void> {
public:
  explicit ToHttp(ToHttpArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (aborted_) {
      co_return;
    }
    if (input.rows() == 0) {
      co_return;
    }
    auto& dh = ctx.dh();
    auto urls = std::vector<std::string>{};
    urls.reserve(input.rows());
    auto requests = std::vector<secret_request>{};
    auto url_warned = false;
    auto url_ms = eval(args_.url, input, dh);
    for (auto const& part : url_ms.parts()) {
      if (part.type.kind().is<string_type>()) {
        for (auto value : part.values<string_type>()) {
          if (value) {
            urls.emplace_back(*value);
          } else {
            url_warned = true;
            urls.emplace_back();
          }
        }
        continue;
      }
      if (part.type.kind().is<secret_type>()) {
        for (auto const& value : part.values<secret_type>()) {
          if (value) {
            requests.emplace_back(make_secret_request(
              "url", materialize(*value), args_.url.get_location(),
              urls.emplace_back(), dh));
          } else {
            url_warned = true;
            urls.emplace_back();
          }
        }
        continue;
      }
      diagnostic::warning("expected `string`, got `{}`", part.type.kind())
        .primary(args_.url)
        .note("skipping request")
        .emit(dh);
      urls.insert(urls.end(), part.length(), {});
    }
    if (url_warned) {
      diagnostic::warning("`url` must not be null")
        .primary(args_.url)
        .note("skipping request")
        .emit(dh);
    }
    auto headers = std::vector<HeaderEvaluation>{};
    if (not co_await evaluate_headers(input, requests, headers, ctx)) {
      aborted_ = true;
      co_return;
    }
    if (not requests.empty()) {
      auto resolved = co_await ctx.resolve_secrets(std::move(requests));
      if (not resolved) {
        aborted_ = true;
        co_return;
      }
    }
    TENZIR_ASSERT(urls.size() == input.rows());
    TENZIR_ASSERT(headers.size() == input.rows());
    auto methods = eval_optional_string(args_.method, input, dh);
    auto bodies = eval_body(input, dh);
    auto row_index = size_t{};
    auto prepared_requests = std::vector<PreparedRequest>{};
    prepared_requests.reserve(input.rows());
    for (auto row : input.values()) {
      auto request_data = materialize(row);
      auto url = std::move(urls[row_index]);
      auto row_headers = std::move(headers[row_index]);
      auto method_name = methods.next().value();
      auto [body_view, insert_content_type] = bodies.next().value();
      auto body = std::string{};
      if (args_.body) {
        body = std::string{body_view};
      } else {
        auto printer = json_printer{{}};
        auto it = std::back_inserter(body);
        printer.print(it, request_data);
        insert_content_type = true;
      }
      ++row_index;
      if (url.empty()) {
        diagnostic::warning("`url` must not be empty")
          .primary(args_.url)
          .note("skipping request")
          .emit(dh);
        continue;
      }
      auto method = args_.make_method(method_name);
      if (not method) {
        auto const method_location = args_.method ? args_.method->get_location()
                                                  : args_.url.get_location();
        diagnostic::warning("invalid http method: `{}`", method_name)
          .primary(method_location)
          .note("skipping request")
          .emit(dh);
        continue;
      }
      if (insert_content_type and not row_headers.has_content_type) {
        row_headers.values.emplace(
          "Content-Type", args_.encode and args_.encode->inner == "form"
                            ? "application/x-www-form-urlencoded"
                            : "application/json");
      }
      if (not row_headers.has_accept_header) {
        row_headers.values.emplace("Accept", "application/json, */*;q=0.5");
      }
      auto tls_default = http::infer_tls_default(url);
      auto tls_opts = args_.tls
                        ? tls_options{*args_.tls, {.tls_default = tls_default}}
                        : tls_options{{.tls_default = tls_default}};
      auto tls_enabled = tls_opts.get_tls(nullptr).inner;
      http::normalize_http_url(url, tls_enabled);
      if (auto valid = tls_opts.validate(url, args_.url.get_location(), dh);
          not valid) {
        continue;
      }
      auto ssl_result = tls_opts.make_folly_ssl_context(dh);
      if (not ssl_result) {
        continue;
      }
      prepared_requests.push_back(PreparedRequest{
        .url = std::move(url),
        .method = std::move(*method),
        .body = std::move(body),
        .headers = std::move(row_headers.values),
        .ssl_context = std::move(*ssl_result),
        .tls_enabled = tls_enabled,
      });
    }
    auto in_flight = std::deque<AsyncHandle<void>>{};
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      for (auto& request : prepared_requests) {
        while (in_flight.size() >= args_.parallel.inner) {
          co_await in_flight.front().join();
          in_flight.pop_front();
        }
        in_flight.push_back(scope.spawn(
          [this, request = std::move(request)]() mutable -> Task<void> {
            co_await run_request_chain(std::move(request));
          }));
      }
      while (not in_flight.empty()) {
        co_await in_flight.front().join();
        in_flight.pop_front();
      }
    });
    co_return;
  }

private:
  struct PreparedRequest {
    std::string url;
    std::string method;
    std::string body;
    std::unordered_map<std::string, std::string> headers;
    std::shared_ptr<folly::SSLContext> ssl_context;
    bool tls_enabled = false;
  };

  struct HeaderEvaluation {
    std::unordered_map<std::string, std::string> values;
    bool has_content_type = false;
    bool has_accept_header = false;
  };

  auto run_request_chain(PreparedRequest request) const -> Task<void> {
    auto pending = std::deque<ExecutorHttpRequest>{
      ExecutorHttpRequest{
        .url = request.url,
        .headers = request.headers,
        .is_pagination = false,
      },
    };
    auto dh = null_diagnostic_handler{};
    while (not pending.empty()) {
      auto current_request = std::move(pending.front());
      pending.pop_front();
      if (current_request.is_pagination
          and args_.paginate_delay.inner > duration::zero()) {
        co_await folly::coro::sleep(
          std::chrono::duration_cast<folly::HighResDuration>(
            to_chrono(args_.paginate_delay.inner)));
      }
      auto response_result = co_await perform_request(
        current_request, request.method, request.body, request.ssl_context);
      if (response_result.is_err()) {
        std::ignore = std::move(response_result).unwrap_err();
        break;
      }
      auto response = std::move(response_result).unwrap();
      auto const code = response.status_code;
      auto const ok = code >= 200 and code <= 399;
      if (ok and args_.paginate and args_.paginate->inner == "link") {
        auto paginate_source = std::optional<location>{args_.paginate->source};
        if (auto next_url = http::next_url_from_link_headers(
              response, current_request.url, paginate_source, dh)) {
          std::ignore
            = queue_executor_request(pending, current_request.headers,
                                     std::move(*next_url), request.tls_enabled,
                                     args_.op, dh, severity::warning,
                                     "skipping request");
        }
      }
      if (not ok) {
        break;
      }
    }
    co_return;
  }

  auto
  perform_request(ExecutorHttpRequest const& request, std::string_view method,
                  std::string const& body,
                  std::shared_ptr<folly::SSLContext> const& ssl_context) const
    -> Task<http::HttpResult<http::ResponseData>> {
    auto config = http::ClientRequestConfig{
      .url = request.url,
      .method = std::string{method},
      .body = body,
      .headers = request.headers,
      .connect_timeout = to_chrono(args_.connection_timeout.inner),
      .ssl_context = ssl_context,
    };
    co_return co_await http::send_request_with_retries(
      std::move(config), args_.max_retry_count.inner,
      to_chrono(args_.retry_delay.inner));
  }

  auto evaluate_headers(table_slice const& input,
                        std::vector<secret_request>& requests,
                        std::vector<HeaderEvaluation>& headers, OpCtx& ctx)
    -> Task<bool> {
    auto& dh = ctx.dh();
    headers.clear();
    headers.reserve(input.rows());
    if (not args_.headers) {
      headers.resize(input.rows());
      co_return true;
    }
    auto header_warned = false;
    auto location = args_.headers->get_location();
    auto header_ms = eval(*args_.headers, input, dh);
    for (auto const& part : header_ms.parts()) {
      if (part.type.kind().is_not<record_type>()) {
        headers.insert(headers.end(), part.length(), HeaderEvaluation{});
        diagnostic::warning("expected `record`, got `{}`", part.type.kind())
          .primary(*args_.headers)
          .note("skipping headers")
          .emit(dh);
        continue;
      }
      for (auto const& value : part.values<record_type>()) {
        auto& row_headers = headers.emplace_back();
        if (not value) {
          diagnostic::warning("expected `record`, got `null`")
            .primary(*args_.headers)
            .note("skipping headers")
            .emit(dh);
          continue;
        }
        for (auto const& [name, header_value] : *value) {
          auto const has_body = true;
          row_headers.has_content_type
            |= has_body and detail::ascii_icase_equal(name, "content-type");
          row_headers.has_accept_header
            |= detail::ascii_icase_equal(name, "accept");
          match(
            header_value,
            [&](std::string_view x) {
              row_headers.values.emplace(name, x);
            },
            [&](secret_view x) {
              auto key = std::string{name};
              requests.emplace_back(make_secret_request(
                key, materialize(x), location, row_headers.values[key], dh));
            },
            [&](auto const&) {
              if (not header_warned) {
                header_warned = true;
                diagnostic::warning(
                  "`headers` must be `{{ string: string|secret }}`")
                  .primary(*args_.headers)
                  .note("skipping invalid header values")
                  .emit(dh);
              }
            });
        }
      }
    }
    co_return true;
  }

  auto eval_body(table_slice const& slice, diagnostic_handler& dh) const
    -> generator<std::pair<std::string_view, bool>> {
    if (not args_.body) {
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        co_yield {};
      }
      co_return;
    }
    auto ms = eval(args_.body.value(), slice, dh);
    for (auto const& part : ms.parts()) {
      if (part.type.kind().is<null_type>()) {
        for (auto i = int64_t{}; i < part.length(); ++i) {
          co_yield {};
        }
        continue;
      }
      if (part.type.kind().is<blob_type>()) {
        for (auto value : part.values<blob_type>()) {
          if (not value) {
            co_yield {};
            continue;
          }
          co_yield {
            {reinterpret_cast<char const*>(value->data()), value->size()},
            false,
          };
        }
        continue;
      }
      if (part.type.kind().is<string_type>()) {
        for (auto value : part.values<string_type>()) {
          if (not value) {
            co_yield {};
            continue;
          }
          co_yield {value.value(), false};
        }
        continue;
      }
      if (part.type.kind().is<record_type>()) {
        auto buffer = std::string{};
        auto const form = args_.encode and args_.encode->inner == "form";
        for (auto value : part.values<record_type>()) {
          if (not value) {
            co_yield {};
            continue;
          }
          if (form) {
            co_yield {curl::escape(flatten(materialize(value.value()))), true};
            continue;
          }
          auto printer = json_printer{{}};
          auto it = std::back_inserter(buffer);
          printer.print(it, value.value());
          co_yield {buffer, true};
          buffer.clear();
        }
        continue;
      }
      diagnostic::warning("expected `blob`, `record` or `string`, got `{}`",
                          part.type.kind())
        .primary(args_.body.value())
        .emit(dh);
      for (auto i = int64_t{}; i < part.length(); ++i) {
        co_yield {};
      }
    }
  }

  static auto
  eval_optional_string(std::optional<ast::expression> const& expr,
                       table_slice const& slice, diagnostic_handler& dh)
    -> generator<std::string_view> {
    if (not expr) {
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        co_yield {};
      }
      co_return;
    }
    auto ms = eval(*expr, slice, dh);
    for (auto const& part : ms.parts()) {
      if (part.type.kind().is<null_type>()) {
        for (auto i = int64_t{}; i < part.length(); ++i) {
          co_yield {};
        }
        continue;
      }
      if (part.type.kind().is<string_type>()) {
        for (auto value : part.values<string_type>()) {
          co_yield value.value_or("");
        }
        continue;
      }
      diagnostic::warning("expected `string`, got `{}`", part.type.kind())
        .primary(*expr)
        .emit(dh);
      for (auto i = int64_t{}; i < part.length(); ++i) {
        co_yield {};
      }
    }
  }

  ToHttpArgs args_;
  bool aborted_ = false;
};

auto make_to_http_description() -> Description {
  auto d = Describer<ToHttpArgs, ToHttp>{};
  d.operator_location(&ToHttpArgs::op);
  auto url = d.positional("url", &ToHttpArgs::url, "string");
  auto method = d.named("method", &ToHttpArgs::method, "string");
  auto body = d.named("body", &ToHttpArgs::body, "record|string|blob");
  auto headers = d.named("headers", &ToHttpArgs::headers, "record");
  auto encode = d.named("encode", &ToHttpArgs::encode);
  auto paginate = d.named("paginate", &ToHttpArgs::paginate);
  auto paginate_delay
    = d.named_optional("paginate_delay", &ToHttpArgs::paginate_delay);
  auto parallel = d.named_optional("parallel", &ToHttpArgs::parallel);
  auto tls = d.named("tls", &ToHttpArgs::tls);
  auto connection_timeout
    = d.named_optional("connection_timeout", &ToHttpArgs::connection_timeout);
  auto max_retry_count
    = d.named_optional("max_retry_count", &ToHttpArgs::max_retry_count);
  auto retry_delay = d.named_optional("retry_delay", &ToHttpArgs::retry_delay);
  d.validate([=](DescribeCtx& ctx) -> Empty {
    auto args = ToHttpArgs{};
    args.op = ctx.get_location(url).value_or(location::unknown);
    if (auto x = ctx.get(url)) {
      args.url = *x;
    }
    if (auto x = ctx.get(method)) {
      args.method = *x;
    }
    if (auto x = ctx.get(body)) {
      args.body = *x;
    }
    if (auto x = ctx.get(headers)) {
      args.headers = *x;
    }
    if (auto x = ctx.get(encode)) {
      args.encode = *x;
    }
    if (auto x = ctx.get(paginate)) {
      args.paginate = *x;
    }
    if (auto x = ctx.get(paginate_delay)) {
      args.paginate_delay = *x;
    }
    if (auto x = ctx.get(parallel)) {
      args.parallel = *x;
    }
    if (auto x = ctx.get(tls)) {
      args.tls = *x;
    }
    if (auto x = ctx.get(connection_timeout)) {
      args.connection_timeout = *x;
    }
    if (auto x = ctx.get(max_retry_count)) {
      args.max_retry_count = *x;
    }
    if (auto x = ctx.get(retry_delay)) {
      args.retry_delay = *x;
    }
    std::ignore = args.validate(ctx);
    return {};
  });
  return d.without_optimize();
}

} // namespace

struct ToHttpPlugin final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "tql2.to_http";
  }

  auto describe() const -> Description override {
    return make_to_http_description();
  }
};

} // namespace tenzir::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ToHttpPlugin)
