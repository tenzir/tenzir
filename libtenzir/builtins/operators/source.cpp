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

class source_operator final : public crtp_operator<source_operator> {
public:
  source_operator() = default;

  explicit source_operator(std::vector<record> events)
    : events_{std::move(events)} {
  }

  auto name() const -> std::string override {
    return "tql2.source";
  }

  auto operator()() const -> generator<table_slice> {
    auto sb = series_builder{};
#if 1
    for (auto& event : events_) {
      sb.data(event);
    }
    auto slices = sb.finish_as_table_slice("tenzir.source");
#else
    for (auto i = 0; i < 1'000'000; ++i) {
      for (auto& event : events_) {
        sb.data(event);
      }
    }
    auto slices = sb.finish_as_table_slice("tenzir.source");
    std::cerr << "--- DONE ---\n" << std::flush;
#endif
    for (auto& slice : slices) {
      co_yield std::move(slice);
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, source_operator& x) -> bool {
    return f.apply(x.events_);
  }

private:
  std::vector<record> events_;
};

class plugin final : public tql2::operator_plugin<source_operator> {
public:
  auto make(invocation inv, session ctx) const -> operator_ptr override {
    auto usage = "source {...} | [...]";
    auto docs = "https://docs.tenzir.com/operators/source";
    if (inv.args.size() != 1) {
      diagnostic::error("expected exactly one argument")
        .primary(inv.self.get_location())
        .usage(usage)
        .docs(docs)
        .emit(ctx);
    }
    if (inv.args.empty()) {
      return nullptr;
    }
    // TODO: We want to const-eval instead.
    // TODO: And we want to const-eval when the operator is instantiated.
    // For example: `every 1s { source { ts: now() } }`
    auto events = std::vector<record>{};
    inv.args[0].match(
      [&](ast::list& x) {
        for (auto& y : x.items) {
          auto item = const_eval(y, ctx);
          if (not item) {
            continue;
          }
          auto rec = caf::get_if<record>(&*item);
          if (not rec) {
            diagnostic::error("expected a record")
              .primary(y.get_location())
              .usage(usage)
              .docs(docs)
              .emit(ctx);
            continue;
          }
          events.push_back(std::move(*rec));
        }
      },
      [&](ast::record& x) {
        // TODO
        auto event = const_eval(ast::expression{std::move(x)}, ctx);
        if (not event) {
          return;
        }
        events.push_back(caf::get<record>(*event));
      },
      [&](auto&) {
        diagnostic::error("expected a record or a list of records")
          .primary(inv.args[0].get_location())
          .usage(usage)
          .docs(docs)
          .emit(ctx);
      });
    return std::make_unique<source_operator>(std::move(events));
  }
};

} // namespace

} // namespace tenzir::plugins::source

TENZIR_REGISTER_PLUGIN(tenzir::plugins::source::plugin)
