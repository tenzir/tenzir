//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/numeric/integral.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::taste {

namespace {

class taste_operator final
  : public schematic_operator<taste_operator, uint64_t> {
public:
  taste_operator() = default;

  explicit taste_operator(uint64_t limit) : limit_{limit} {
  }

  auto initialize(const type&, operator_control_plane&) const
    -> caf::expected<state_type> override {
    return limit_;
  }

  auto process(table_slice slice, state_type& remaining) const
    -> table_slice override {
    auto result = head(slice, remaining);
    remaining -= result.rows();
    return result;
  }

  auto name() const -> std::string override {
    return "taste";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    // Note: The `unordered` means that we do not necessarily return the first
    // `limit_` events.
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, taste_operator& x) -> bool {
    return f.apply(x.limit_);
  }

private:
  uint64_t limit_;
};

struct TasteArgs {
  uint64_t limit = 10;
};

class Taste : public Operator<table_slice, table_slice> {
public:
  explicit Taste(TasteArgs args) : limit_{args.limit} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<bool> override {
    auto it = schemas_.find(input.schema());
    if (it == schemas_.end()) {
      it = schemas_.emplace(input.schema(), limit_).first;
    }
    auto remaining = it->second;
    if (remaining != 0) {
      auto result = head(std::move(input), remaining);
      it->second -= result.rows();
      co_return (co_await push(std::move(result))).is_err();
    }
    co_return false;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("schemas_", schemas_);
  }

private:
  std::unordered_map<type, uint64_t> schemas_;
  uint64_t limit_;
};

class plugin final : public virtual operator_plugin<taste_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"taste", "https://docs.tenzir.com/"
                                           "operators/taste"};
    auto count = std::optional<uint64_t>{};
    parser.add(count, "<limit>");
    parser.parse(p);
    return std::make_unique<taste_operator>(count.value_or(10));
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto count = std::optional<uint64_t>{};
    argument_parser2::operator_("taste")
      .positional("count", count)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<taste_operator>(count.value_or(10));
  }

  auto describe() const -> Description override {
    auto d = Describer<TasteArgs, Taste>{};
    auto limit = d.optional_positional("limit", &TasteArgs::limit);
    d.validate([limit](DescribeCtx& ctx) -> Empty {
      TRY(auto value, ctx.get(limit));
      if (value == 0) {
        diagnostic::error("`limit` must not be zero")
          .primary(ctx.get_location(limit).value())
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::taste

TENZIR_REGISTER_PLUGIN(tenzir::plugins::taste::plugin)
