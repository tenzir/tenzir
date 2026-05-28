//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/blob.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/try.hpp>
#include <tenzir/view3.hpp>

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

auto append_delimited_part(series const& selected, ast::expression const& value,
                           blob const& separator, blob& buffer, OpCtx& ctx)
  -> bool {
  auto append_separator = [&] {
    buffer.insert(buffer.end(), separator.begin(), separator.end());
  };
  return match(
    selected,
    [&](basic_series<string_type> const& part) {
      for (auto value : part.values3()) {
        if (not value) {
          continue;
        }
        auto bytes = as_bytes(*value);
        buffer.insert(buffer.end(), bytes.begin(), bytes.end());
        append_separator();
      }
      return true;
    },
    [&](basic_series<blob_type> const& part) {
      for (auto value : part.values3()) {
        if (not value) {
          continue;
        }
        buffer.insert(buffer.end(), value->begin(), value->end());
        append_separator();
      }
      return true;
    },
    [](basic_series<null_type> const&) {
      return true;
    },
    [&](auto const& part) {
      diagnostic::error("expected `string`, `blob`, or `null`, but got `{}`",
                        type{part.type}.kind())
        .primary(value)
        .emit(ctx);
      return false;
    });
}

class WriteDelimited final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteDelimited(WriteDelimitedArgs args)
    : args_{std::move(args)}, separator_{make_separator_blob(args_.separator)} {
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
    auto evaluated = eval(args_.value, input, ctx.dh());
    auto buffer = blob{};
    for (auto const& selected : evaluated.parts()) {
      if (not append_delimited_part(selected, args_.value, separator_, buffer,
                                    ctx)) {
        co_return;
      }
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
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.write_delimited";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteDelimitedArgs, WriteDelimited>{};
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
