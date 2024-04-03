//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::source {

namespace {

using namespace tql2;

class source_use final : public crtp_operator<source_use> {
public:
  source_use() = default;

  explicit source_use(std::vector<tenzir::record> events)
    : events_{std::move(events)} {
  }

  auto name() const -> std::string override {
    return "tql2.source";
  }

  auto operator()() const -> generator<table_slice> {
    auto sb = series_builder{};
    for (auto& event : events_) {
      sb.data(event);
    }
    for (auto& slice : sb.finish_as_table_slice("tenzir.source")) {
      co_yield std::move(slice);
    }
  }

  auto optimize(tenzir::expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, source_use& x) -> bool {
    return f.apply(x.events_);
  }

private:
  std::vector<tenzir::record> events_;
};

class plugin final : public tql2::operator_plugin<source_use> {
public:
  auto make_operator(ast::entity self, std::vector<ast::expression> args,
                     tql2::context& ctx) const -> operator_ptr override {
    auto usage = "source [{...}, ...]";
    auto docs = "https://docs.tenzir.com/operators/source";
    if (args.size() != 1) {
      diagnostic::error("expected exactly one argument")
        .primary(self.get_location())
        .usage(usage)
        .docs(docs)
        .emit(ctx.dh());
    }
    if (args.empty()) {
      return nullptr;
    }
    // TODO: We want to const-eval instead.
    auto events = std::vector<tenzir::record>{};
    args[0].match(
      [&](ast::list& x) {
        for (auto& y : x.items) {
          auto item = evaluate(y, ctx);
          if (not item) {
            continue;
          }
          auto rec = caf::get_if<tenzir::record>(&*item);
          if (not rec) {
            diagnostic::error("expected a record")
              .primary(y.get_location())
              .usage(usage)
              .docs(docs)
              .emit(ctx.dh());
            continue;
          }
          events.push_back(std::move(*rec));
        }
      },
      [&](auto&) {
        diagnostic::error("expected a list")
          .primary(args[0].get_location())
          .usage(usage)
          .docs(docs)
          .emit(ctx.dh());
      });
    return std::make_unique<source_use>(std::move(events));
  }
};

} // namespace

} // namespace tenzir::plugins::source

TENZIR_REGISTER_PLUGIN(tenzir::plugins::source::plugin)
