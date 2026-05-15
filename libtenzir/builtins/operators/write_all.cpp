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
#include <tenzir/view3.hpp>

#include <string_view>

namespace tenzir::plugins::write_all {

namespace {

struct WriteAllArgs {
  ast::field_path field;
};

auto emit_resolve_error(resolve_error const& err, diagnostic_handler& dh)
  -> void {
  err.reason.match([&](resolve_error::field_not_found const&) {},
                   [&](resolve_error::field_not_found_no_error const&) {},
                   [&](resolve_error::field_of_non_record const& reason) {
                     diagnostic::error("type `{}` has no field `{}`",
                                       reason.type.kind(), err.ident.name)
                       .primary(err.ident)
                       .emit(dh);
                   });
}

class WriteAll final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteAll(WriteAllArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice slice, Push<chunk_ptr>&, OpCtx& ctx)
    -> Task<void> override {
    if (slice.rows() == 0) {
      co_return;
    }
    auto resolved = resolve(args_.field, slice);
    if (auto* error = std::get_if<resolve_error>(&resolved)) {
      emit_resolve_error(*error, ctx);
      co_return;
    }
    auto const& selected = std::get<series>(resolved);
    if (selected.type.kind().is<null_type>()) {
      co_return;
    }
    if (selected.type.kind().is<string_type>()) {
      for (auto value : selected.values3<string_type>()) {
        if (not value) {
          continue;
        }
        auto bytes = as_bytes(*value);
        buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
      }
      co_return;
    }
    if (selected.type.kind().is<blob_type>()) {
      for (auto value : selected.values3<blob_type>()) {
        if (not value) {
          continue;
        }
        buffer_.insert(buffer_.end(), value->begin(), value->end());
      }
      co_return;
    }
    diagnostic::error("field has unsupported type `{}`", selected.type.kind())
      .primary(args_.field)
      .hint("`write_all` supports `string` and `blob` fields")
      .emit(ctx);
    co_return;
  }

  auto finalize(Push<chunk_ptr>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    if (buffer_.empty()) {
      co_return FinalizeBehavior::done;
    }
    co_await push(chunk::make(std::move(buffer_)));
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
  }

private:
  WriteAllArgs args_;
  blob buffer_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.write_all";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteAllArgs, WriteAll>{};
    auto field = d.positional("field", &WriteAllArgs::field, "field");
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto value = ctx.get(field);
      if (value and value->path().empty()) {
        diagnostic::error("cannot write all of `this`")
          .primary(*value)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::write_all

TENZIR_REGISTER_PLUGIN(tenzir::plugins::write_all::plugin)
