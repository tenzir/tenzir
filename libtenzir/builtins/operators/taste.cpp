//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

struct TasteArgs {
  uint64_t limit = 10;
};

class Taste : public Operator<table_slice, table_slice> {
public:
  explicit Taste(TasteArgs args) : limit_{args.limit} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx&)
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

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "taste";
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
    return d.unordered();
  }
};

} // namespace

} // namespace tenzir::plugins::taste

TENZIR_REGISTER_PLUGIN(tenzir::plugins::taste::plugin)
