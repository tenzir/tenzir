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
    if (evaluated.parts().size() != 1) {
      diagnostic::error("expected exactly one series, but got {}",
                        evaluated.parts().size())
        .primary(args_.value)
        .emit(ctx);
      co_return;
    }
    auto const& selected = evaluated.part(0);
    if (selected.type.kind().is<null_type>()) {
      co_return;
    }
    auto buffer = blob{};
    auto append_separator = [&] {
      buffer.insert(buffer.end(), separator_.begin(), separator_.end());
    };
    if (selected.type.kind().is<string_type>()) {
      for (auto value : selected.values3<string_type>()) {
        if (not value) {
          continue;
        }
        auto bytes = as_bytes(*value);
        buffer.insert(buffer.end(), bytes.begin(), bytes.end());
        append_separator();
      }
    } else if (selected.type.kind().is<blob_type>()) {
      for (auto value : selected.values3<blob_type>()) {
        if (not value) {
          continue;
        }
        buffer.insert(buffer.end(), value->begin(), value->end());
        append_separator();
      }
    } else {
      diagnostic::error("expected `string`, `blob`, or `null`, but got `{}`",
                        selected.type.kind())
        .primary(args_.value)
        .emit(ctx);
      co_return;
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
