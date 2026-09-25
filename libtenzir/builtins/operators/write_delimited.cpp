//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/blob.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/try.hpp>
#include <tenzir/view3.hpp>

#include <set>

#include "write_bytes.hpp"

namespace tenzir::plugins::write_delimited {

namespace {

struct WriteDelimitedArgs {
  ast::expression value;
  located<data> separator;
};

auto make_separator_blob(located<data> const& separator) -> blob {
  return match(
    separator.inner,
    [](std::string const& str) {
      return blob{as_bytes(str)};
    },
    [](blob const& bytes) {
      return bytes;
    },
    [](auto const&) -> blob {
      TENZIR_UNREACHABLE();
    });
}

template <class Input>
class WriteDelimited final : public Operator<Input, chunk_ptr> {
public:
  explicit WriteDelimited(WriteDelimitedArgs args)
    : args_{std::move(args)}, separator_{make_separator_blob(args_.separator)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if constexpr (std::same_as<Input, nova::Events>) {
      auto evaluator = co_await nova::Evaluator::make(args_.value, ctx);
      if (evaluator) {
        evaluator_ = std::move(*evaluator);
      }
    }
    co_return;
  }

  auto process(Input input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if constexpr (std::same_as<Input, table_slice>) {
      co_await process_arrow(std::move(input), push, ctx);
    } else {
      if (not evaluator_) {
        co_return;
      }
      auto values = evaluator_->eval(input, nova::EvalCtx{ctx.dh()});
      auto buffer = blob{};
      auto warned_null = false;
      auto warned_types = std::set<std::string_view>{};
      for (auto row : nova::storage::true_bits(input.mask)) {
        auto value = values.get(row);
        auto append = [&](auto bytes) {
          buffer.insert(buffer.end(), bytes.begin(), bytes.end());
          buffer.insert(buffer.end(), separator_.begin(), separator_.end());
        };
        match(
          value,
          [&](nova::RowView<nova::Null>) {
            if (not std::exchange(warned_null, true)) {
              diagnostic::warning("dropped null value")
                .primary(args_.value)
                .emit(ctx);
            }
          },
          [&](nova::RowView<nova::String> text) {
            append(as_bytes(*text));
          },
          [&](nova::RowView<nova::Blob> bytes) {
            append(*bytes);
          },
          [&](auto const&) {
            auto kind = write_bytes::kind(value);
            if (warned_types.insert(kind).second) {
              diagnostic::warning("expected `string` or `blob`, but got `{}`",
                                  kind)
                .primary(args_.value)
                .emit(ctx);
            }
          });
      }
      if (not buffer.empty()) {
        co_await push(chunk::make(std::move(buffer)));
      }
    }
  }

  auto process_arrow(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> {
    auto evaluated = eval(args_.value, input, ctx.dh());
    auto buffer = blob{};
    auto append_separator = [&] {
      buffer.insert(buffer.end(), separator_.begin(), separator_.end());
    };
    auto warn_about_nulls = [&](arrow::Array const& array) {
      if (array.null_count() > 0) {
        diagnostic::warning("dropped null value").primary(args_.value).emit(ctx);
      }
    };
    for (auto const& selected : evaluated.parts()) {
      match(
        selected,
        [&](basic_series<string_type> const& part) {
          warn_about_nulls(*part.array);
          for (auto value : part.values3()) {
            if (value) {
              auto bytes = as_bytes(*value);
              buffer.insert(buffer.end(), bytes.begin(), bytes.end());
              append_separator();
            }
          }
        },
        [&](basic_series<blob_type> const& part) {
          warn_about_nulls(*part.array);
          for (auto value : part.values3()) {
            if (value) {
              buffer.insert(buffer.end(), value->begin(), value->end());
              append_separator();
            }
          }
        },
        [&](basic_series<null_type> const& part) {
          warn_about_nulls(*part.array);
        },
        [&](auto const& part) {
          diagnostic::warning("expected `string` or `blob`, but got `{}`",
                              type{part.type}.kind())
            .primary(args_.value)
            .emit(ctx);
        });
    }
    if (not buffer.empty()) {
      co_await push(chunk::make(std::move(buffer)));
    }
  }

  auto finalize(Push<chunk_ptr>&, OpCtx&) -> Task<FinalizeBehavior> override {
    co_return FinalizeBehavior::done;
  }

private:
  WriteDelimitedArgs args_;
  blob separator_;
  Option<nova::Evaluator> evaluator_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_delimited";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteDelimitedArgs, WriteDelimited<table_slice>,
                       WriteDelimited<nova::Events>>{};
    d.positional("value", &WriteDelimitedArgs::value, "any");
    auto separator = d.positional("separator", &WriteDelimitedArgs::separator,
                                  "string|blob");
    d.validate([separator](DescribeCtx& ctx) -> Empty {
      TRY(auto sep, ctx.get(separator));
      if (not is<std::string>(sep.inner) and not is<blob>(sep.inner)) {
        diagnostic::error("expected `string` or `blob`, but got `{}`",
                          type_kind_of_data(sep.inner))
          .primary(sep.source)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::write_delimited

TENZIR_REGISTER_PLUGIN(tenzir::plugins::write_delimited::plugin)
