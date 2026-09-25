//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval.hpp"

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async/dns.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/eval_util.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/view.hpp>

#include <arpa/inet.h>
#include <arrow/type.h>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

#include <ares.h>
#include <array>
#include <chrono>
#include <memory>
#include <netdb.h>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::dns_lookup {

namespace {

struct ares_init_raii {
  ares_init_raii() {
    const auto res = ares_library_init(ARES_LIB_INIT_ALL);
    if (res != ARES_SUCCESS) {
      TENZIR_WARN("failed to init libares: {}", res);
    }
  }

  ares_init_raii(const ares_init_raii&) = delete;

  ~ares_init_raii() {
    ares_library_cleanup();
  }
};

const static auto ares_init = ares_init_raii{};

class ares_channel_wrapper {
public:
  ares_channel_wrapper() {
    status_ = static_cast<ares_status_t>(ares_library_initialized());
    if (status_ != ARES_SUCCESS) {
      return;
    }
    auto options = ares_options{};
    options.timeout = 5000; // 5 second timeout
    options.tries = 2;
    options.evsys = ARES_EVSYS_DEFAULT;
    const auto optmask
      = ARES_OPT_TIMEOUTMS | ARES_OPT_TRIES | ARES_OPT_EVENT_THREAD;
    status_ = static_cast<ares_status_t>(
      ares_init_options(&channel_, &options, optmask));
    if (status_ != ARES_SUCCESS) {
      channel_ = nullptr;
    }
  }

  ~ares_channel_wrapper() {
    if (channel_) {
      ares_destroy(channel_);
    }
  }

  ares_channel_wrapper(const ares_channel_wrapper&) = delete;
  ares_channel_wrapper& operator=(const ares_channel_wrapper&) = delete;

  auto get() const -> ares_channel_t* {
    return channel_;
  }

  auto valid() const -> bool {
    return channel_ != nullptr;
  }

  auto status() const -> ares_status_t {
    return status_;
  }

private:
  ares_channel_t* channel_ = nullptr;
  ares_status_t status_;
};

auto default_result_field() -> ast::field_path {
  auto result = ast::field_path::try_from(
    ast::root_field{ast::identifier{"dns_lookup", location::unknown}});
  TENZIR_ASSERT(result);
  return std::move(*result);
}

auto append_forward_result(series_builder& builder,
                           const ForwardDnsResult& result) -> void {
  if (result.is_err()) {
    builder.null();
    return;
  }
  const auto* resolved = try_as<ForwardDnsResolved>(&result.unwrap());
  if (not resolved or resolved->answers.empty()) {
    builder.null();
    return;
  }
  auto answers = builder.list();
  for (const auto& answer : resolved->answers) {
    auto row = answers.record();
    row.field("address", answer.address);
    row.field("type", answer.type);
    row.field("ttl", std::chrono::duration_cast<duration>(answer.ttl));
  }
}

auto append_reverse_result(series_builder& builder,
                           const ReverseDnsResult& result) -> void {
  if (result.is_err()) {
    builder.null();
    return;
  }
  const auto* resolved = try_as<ReverseDnsResolved>(&result.unwrap());
  if (not resolved or resolved->hostname.empty()) {
    builder.null();
    return;
  }
  builder.record().field("hostname", resolved->hostname);
}

auto append_forward_result(nova::ArrayBuilder<nova::Data>& builder,
                           const ForwardDnsResult& result) -> void {
  if (result.is_err()) {
    builder.null();
    return;
  }
  const auto* resolved = try_as<ForwardDnsResolved>(&result.unwrap());
  if (not resolved or resolved->answers.empty()) {
    builder.null();
    return;
  }
  auto answers = builder.list();
  for (const auto& answer : resolved->answers) {
    auto row = answers.record();
    row.field("address").data(answer.address);
    row.field("type").data(answer.type);
    row.field("ttl").data(std::chrono::duration_cast<duration>(answer.ttl));
  }
}

auto append_reverse_result(nova::ArrayBuilder<nova::Data>& builder,
                           const ReverseDnsResult& result) -> void {
  if (result.is_err()) {
    builder.null();
    return;
  }
  const auto* resolved = try_as<ReverseDnsResolved>(&result.unwrap());
  if (not resolved or resolved->hostname.empty()) {
    builder.null();
    return;
  }
  builder.record().field("hostname").data(resolved->hostname);
}

auto type_name(nova::RowView<nova::Data> value) -> std::string_view {
  return match(value, []<nova::data_type Tag>(nova::RowView<Tag>) {
    return std::string_view{nova::Type<Tag>::static_name};
  });
}

struct DnsLookupArgs {
  ast::expression field;
  ast::field_path result = default_result_field();
  location operator_location = location::unknown;
};

class DnsLookup final : public Operator<table_slice, table_slice> {
public:
  explicit DnsLookup(DnsLookupArgs args) : args_{std::move(args)} {
  }

  DnsLookup(DnsLookup const&) = delete;
  auto operator=(DnsLookup const&) -> DnsLookup& = delete;
  DnsLookup(DnsLookup&&) noexcept = default;
  auto operator=(DnsLookup&&) noexcept -> DnsLookup& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    auto error = forward_dns_.startup_error();
    if (not error) {
      error = reverse_dns_.startup_error();
    }
    if (error) {
      diagnostic::error("failed to initialize DNS resolver")
        .primary(args_.operator_location, "reason: {}", error->error)
        .emit(ctx);
      startup_failed_ = true;
    }
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (startup_failed_) {
      co_return;
    }
    auto fields = eval(args_.field, input, ctx.dh());
    auto slice_start = int64_t{0};
    for (auto& field : fields) {
      auto result_series = series{};
      if (is<ip_type>(field.type)) {
        result_series = co_await resolve_reverse(field);
      } else if (is<string_type>(field.type)) {
        result_series = co_await resolve_forward(field);
      } else {
        if (auto kind = field.type.kind();
            kind != type_kind{tag_v<null_type>}) {
          diagnostic::warning("expected `ip` or `string`")
            .primary(args_.field, "got {}", kind)
            .emit(ctx.dh());
        }
        result_series = series::null(null_type{}, field.length());
      }
      const auto slice_end = slice_start + result_series.length();
      auto output = assign(args_.result, std::move(result_series),
                           subslice(input, slice_start, slice_end), ctx.dh());
      slice_start = slice_end;
      co_await push(std::move(output));
    }
  }

  auto state() -> OperatorState override {
    return startup_failed_ ? OperatorState::done : OperatorState::normal;
  }

private:
  auto resolve_forward(const series& input) -> Task<series> {
    auto results = std::vector<Option<Arc<ForwardDnsResult>>>{};
    results.resize(detail::narrow<size_t>(input.length()));
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      auto row = size_t{0};
      for (auto hostname : input.values3<string_type>()) {
        const auto current_row = row++;
        if (not hostname) {
          continue;
        }
        scope.spawn(
          [this, &results, current_row,
           hostname = std::string{*hostname}]() mutable -> Task<void> {
            results[current_row]
              = co_await forward_dns_.resolve(std::move(hostname));
          });
      }
      co_return;
    });
    auto builder = series_builder{};
    for (const auto& result : results) {
      if (not result) {
        builder.null();
        continue;
      }
      append_forward_result(builder, **result);
    }
    co_return builder.finish_assert_one_array();
  }

  auto resolve_reverse(const series& input) -> Task<series> {
    auto results = std::vector<Option<Arc<ReverseDnsResult>>>{};
    results.resize(detail::narrow<size_t>(input.length()));
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      auto row = size_t{0};
      for (auto address : input.values3<ip_type>()) {
        const auto current_row = row++;
        if (not address) {
          continue;
        }
        scope.spawn([this, &results, current_row,
                     address = *address]() mutable -> Task<void> {
          results[current_row] = co_await reverse_dns_.resolve(address);
        });
      }
      co_return;
    });
    auto builder = series_builder{};
    for (const auto& result : results) {
      if (not result) {
        builder.null();
        continue;
      }
      append_reverse_result(builder, **result);
    }
    co_return builder.finish_assert_one_array();
  }

  DnsLookupArgs args_;
  ForwardDnsResolver forward_dns_;
  ReverseDnsResolver reverse_dns_;
  bool startup_failed_ = false;
};

class DnsLookupNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit DnsLookupNova(DnsLookupArgs args) : args_{std::move(args)} {
    field_location_ = args_.field.get_location();
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto error = forward_dns_.startup_error();
    if (not error) {
      error = reverse_dns_.startup_error();
    }
    if (error) {
      diagnostic::error("failed to initialize DNS resolver")
        .primary(args_.operator_location, "reason: {}", error->error)
        .emit(ctx);
      co_return;
    }
    auto evaluator = nova::Evaluator::make(
      std::move(args_.field), nova::InstantiateCtx{ctx.dh(), ctx.reg()});
    if (evaluator) {
      evaluator_.emplace(std::move(*evaluator));
    }
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not evaluator_) {
      co_return;
    }
    auto fields = evaluator_->eval(input, nova::EvalCtx{ctx.dh()});
    auto forward_results = std::vector<Option<Arc<ForwardDnsResult>>>{};
    auto reverse_results = std::vector<Option<Arc<ReverseDnsResult>>>{};
    forward_results.resize(detail::narrow<size_t>(input.length()));
    reverse_results.resize(detail::narrow<size_t>(input.length()));
    auto wrong_type = std::string_view{};
    co_await async_scope([&](AsyncScope& scope) -> Task<void> {
      nova::storage::for_each_true(input.mask, [&](auto row) {
        auto value = fields.get(row);
        if (auto hostname = try_as<nova::RowView<nova::String>>(value)) {
          scope.spawn(
            [this, &forward_results, row,
             hostname = std::string{**hostname}]() mutable -> Task<void> {
              forward_results[row]
                = co_await forward_dns_.resolve(std::move(hostname));
            });
        } else if (auto address = try_as<nova::RowView<nova::Ip>>(value)) {
          scope.spawn([this, &reverse_results, row,
                       address = **address]() mutable -> Task<void> {
            reverse_results[row] = co_await reverse_dns_.resolve(address);
          });
        } else if (not is<nova::RowView<nova::Null>>(value)) {
          wrong_type = type_name(value);
        }
      });
      co_return;
    });
    if (not wrong_type.empty()) {
      diagnostic::warning("expected `ip` or `string`")
        .primary(field_location_, "got {}", wrong_type)
        .emit(ctx.dh());
    }
    auto result = nova::ArrayBuilder<nova::Data>{};
    nova::storage::for_each_true(input.mask, [&](auto row) {
      result.skip_n(row - result.length());
      if (forward_results[row]) {
        append_forward_result(result, **forward_results[row]);
      } else if (reverse_results[row]) {
        append_reverse_result(result, **reverse_results[row]);
      } else {
        result.null();
      }
    });
    result.skip_n(input.length() - result.length());
    input.data
      = nova::assign_nested_field(std::move(input.data), args_.result.path(),
                                  {result.finish(), input.mask}, ctx.dh());
    co_await push(std::move(input));
  }

private:
  DnsLookupArgs args_;
  Option<nova::Evaluator> evaluator_;
  location field_location_ = location::unknown;
  ForwardDnsResolver forward_dns_;
  ReverseDnsResolver reverse_dns_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "dns_lookup";
  }

  auto describe() const -> Description override {
    auto d = Describer<DnsLookupArgs, DnsLookup, DnsLookupNova>{};
    // Replicas resolve independent slices with private resolver caches. This
    // may duplicate requests between instances, but does not change results or
    // exceed a pipeline-wide request bound: lookups are already unconstrained.
    d.parallelizable();
    auto field = d.positional("field", &DnsLookupArgs::field, "string|ip");
    auto result = d.named_optional("result", &DnsLookupArgs::result);
    d.operator_location(&DnsLookupArgs::operator_location);
    return d.optimize(
      [=](DescribeCtx& ctx, ir::OptimizeRequest req) -> Optimization {
        auto result_path = ctx.get(result).value_or(default_result_field());
        // The result field is produced here; all other fields pass through.
        auto projection = std::move(req.projection);
        if (projection) {
          std::erase_if(*projection, [&](const ast::field_path& path) {
            return ir::is_field_path_prefix(result_path, path);
          });
          // A nested assignment observes its existing parent and warns when a
          // scalar must be replaced by an implicit record. Retain the target
          // so projection cannot hide that diagnostic.
          if (result_path.path().size() > 1) {
            ir::add_to_projection(projection, result_path);
          }
        }
        // The lookup input is computed from the event.
        if (auto expr = ctx.get(field)) {
          ir::add_refs_to_projection(projection, *expr);
        } else {
          projection = None{};
        }
        auto touched_fields = std::vector<ast::field_path>{};
        touched_fields.push_back(std::move(result_path));
        auto touched = ast::ExprRefs{.field_paths = std::move(touched_fields)};
        auto [f_upstream, f_self]
          = ir::split_filter_by_dependents(std::move(req.filter), touched);
        // `dns_lookup` is 1:1, so the first N outputs stem from exactly the
        // first N inputs and a carried limit passes through when no predicate
        // stays behind. Skipped DNS requests are acceptable: effects may
        // change under optimization.
        auto limit = f_self.empty() ? req.limit : Option<uint64_t>{};
        return {
          .order = req.order,
          .filter_upstream = std::move(f_upstream),
          .filter_self = std::move(f_self),
          .limit_upstream = limit,
          .projection_upstream = std::move(projection),
        };
      });
  }
};

} // namespace

} // namespace tenzir::plugins::dns_lookup

TENZIR_REGISTER_PLUGIN(tenzir::plugins::dns_lookup::plugin)
