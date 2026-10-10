//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async.hpp>
#include <tenzir/box.hpp>
#include <tenzir/data.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/location.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/eval_util.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/option.hpp>
#include <tenzir/result.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/time.hpp>
#include <tenzir/tql2/ast.hpp>

#include <functional>
#include <string>
#include <string_view>
#include <vector>

/// Building blocks of the AI operators, such as `ai_prompt` and `ai_decide`.
/// The protocol clients live in `tenzir/ai/`.
namespace tenzir::ai {

// -- arguments ----------------------------------------------------------------

/// The top-level field that holds AI operator results by default.
inline constexpr auto result_field = std::string_view{"ai"};

/// Returns the default result field of an AI operator, `ai.<name>`.
auto default_into(std::string_view name) -> ast::field_path;

/// Checks the arguments that all AI operators share. Skips omitted arguments,
/// so that a validator can pass what it has.
auto check_arguments(Option<located<std::string>> const& model,
                     Option<located<uint64_t>> const& concurrency,
                     Option<located<duration>> const& timeout,
                     diagnostic_handler& dh) -> void;

// -- connection ---------------------------------------------------------------

/// Decides whether the path of an endpoint already addresses its resource,
/// for providers whose URL does not end in the resource name.
using CompleteRoute = bool (*)(std::string_view path);

/// Appends `/<resource>` to a base endpoint unless it already ends with it or
/// `complete` accepts its path. Fails unless the endpoint is an HTTP or HTTPS
/// URL with a host and a valid port. Error messages never contain the endpoint.
auto make_endpoint_url(std::string endpoint, std::string_view resource,
                       CompleteRoute complete = nullptr)
  -> Result<std::string, std::string>;

/// What an AI operator needs to send requests to its endpoint.
struct Connection {
  Box<HttpPool> pool;
  std::vector<http::Header> headers;
  /// The path that requests go to, after appending the resource.
  std::string path;
};

/// Resolves the endpoint and the API key, appends `resource` to the endpoint
/// unless `complete` accepts its path, and creates an HTTP pool. The endpoint
/// can be a secret, so diagnostics never reveal it.
auto connect(located<secret> endpoint, Option<located<secret>> api_key,
             std::string_view resource, duration timeout,
             Option<located<data>> const& tls, OpCtx& ctx,
             CompleteRoute complete = nullptr) -> Task<failure_or<Connection>>;

// -- inputs -------------------------------------------------------------------

/// Returns the fields that an AI operator removes from the event when it sends
/// the whole event by default: the `ai` field and the operator's own `into`
/// field. This keeps earlier AI results out of later requests. All returned
/// paths are optional, so removing them never warns.
auto default_input_exclusions(ast::field_path const& into)
  -> std::vector<ast::field_path>;

/// Serializes the input of every active event as compact JSON.
class InputPrinter {
public:
  /// Prepares to print `expr`, or the event without the default exclusions
  /// for `into` if there is no expression.
  static auto make(Option<ast::expression> expr, ast::field_path const& into,
                   OpCtx& ctx) -> Task<failure_or<InputPrinter>>;

  /// Returns one JSON text per active row of `events`.
  auto print(nova::Events const& events, diagnostic_handler& dh)
    -> std::vector<std::string>;

private:
  InputPrinter() = default;

  Option<nova::Evaluator> evaluator_ = None{};
  /// The drop tree aliases these paths. Moving the vector keeps its elements
  /// in place, so the aliases stay valid when the printer moves.
  std::vector<ast::field_path> exclusions_;
  nova::DropTree drop_tree_;
};

// -- requests -----------------------------------------------------------------

/// Runs `request(i)` for every `i` in `[0, count)`, at most `concurrency` at a
/// time, and returns once all of them finished. At most `concurrency` tasks
/// exist at any time, regardless of `count`.
auto request_all(size_t count, uint64_t concurrency,
                 std::function<Task<void>(size_t)> request) -> Task<void>;

/// Returns the warning for a failed request.
auto request_failed(std::string_view error, location source)
  -> diagnostic_builder;

// -- results ------------------------------------------------------------------

/// Writes `value` into `field`, or `null` if there is none. Works with the
/// builders of both event representations.
template <class Field, class T>
auto set_optional(Field field, Option<T> const& value) -> void {
  if (not value) {
    field.null();
  } else if constexpr (std::same_as<T, std::string>) {
    field.data(std::string_view{*value});
  } else {
    field.data(*value);
  }
}

/// Builds one result per active row of `events`. `append(builder, i)` appends
/// the result of the `i`-th active row.
template <class F>
auto build_results(nova::Events const& events, F&& append)
  -> nova::MaskedArray<nova::Array<nova::Data>> {
  auto builder = nova::ArrayBuilder<nova::Data>{};
  auto i = size_t{};
  for (auto row : nova::storage::true_bits(events.mask)) {
    builder.skip_n(row - builder.length());
    std::invoke(append, builder, i++);
  }
  builder.skip_n(events.length() - builder.length());
  return {builder.finish(), events.mask};
}

/// Writes the results into the `into` field of `events`.
auto assign_results(nova::Events& events, ast::field_path const& into,
                    nova::MaskedArray<nova::Array<nova::Data>> results,
                    diagnostic_handler& dh) -> void;

// -- legacy event representation ----------------------------------------------

/// Serializes the input of every event as compact JSON, or returns none for
/// events that fail to serialize. See `InputPrinter` for the arguments.
auto print_inputs(Option<ast::expression> const& expr,
                  ast::field_path const& into, location self,
                  table_slice const& input, diagnostic_handler& dh)
  -> std::vector<Option<std::string>>;

/// Writes one result per event from `results` into the `into` field.
auto assign_results(table_slice const& input, ast::field_path const& into,
                    series_builder results, diagnostic_handler& dh)
  -> std::vector<table_slice>;

} // namespace tenzir::ai
