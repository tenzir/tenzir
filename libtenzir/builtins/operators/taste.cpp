//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>

#include <unordered_map>

namespace tenzir::plugins::taste {

namespace {

struct TasteArgs {
  uint64_t limit = 10;
};

class Taste : public Operator<table_slice, table_slice> {
public:
  explicit Taste(TasteArgs args) : limit_{args.limit} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto it = schemas_.find(input.schema());
    if (it == schemas_.end()) {
      it = schemas_.emplace(input.schema(), limit_).first;
    }
    auto remaining = it->second;
    if (remaining != 0) {
      auto result = head(std::move(input), remaining);
      it->second -= result.rows();
      co_await push(std::move(result));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    serde("schemas_", schemas_);
  }

private:
  std::unordered_map<type, uint64_t> schemas_;
  uint64_t limit_;
};

class TastePlugin : public OperatorPlugin {
  auto name() const -> std::string override {
    return "taste";
  }

  auto describe() const -> Description override {
    auto d = Describer<TasteArgs, Taste>{};
    auto limit = d.optional_positional("limit", &TasteArgs::limit);
    d.validate([limit](ValidateCtx& ctx) -> Empty {
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

TENZIR_REGISTER_PLUGIN(tenzir::plugins::taste::TastePlugin)
