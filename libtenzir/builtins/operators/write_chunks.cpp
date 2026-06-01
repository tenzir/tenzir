//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/view3.hpp>

namespace tenzir::plugins::write_chunks {

namespace {

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

struct WriteChunksArgs {
  Option<ast::field_path> field;
  location operator_location;
};

class WriteChunks final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteChunks(WriteChunksArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice slice, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (slice.rows() == 0) {
      co_return;
    }
    auto selected = select_field(slice, ctx);
    if (not selected) {
      co_return;
    }
    if (selected->type.kind().is<null_type>()) {
      co_return;
    }
    if (selected->type.kind().is<blob_type>()) {
      for (auto value : selected->values3<blob_type>()) {
        if (not value) {
          continue;
        }
        co_await push(chunk::copy(*value));
      }
      co_return;
    }
    diagnostic::error("field has unsupported type `{}`", selected->type.kind())
      .primary(args_.operator_location)
      .hint("`write_chunks` expects a field of type `blob`")
      .emit(ctx);
    co_return;
  }

private:
  auto select_field(const table_slice& slice, OpCtx& ctx) -> Option<series> {
    if (args_.field) {
      auto resolved = resolve(*args_.field, slice);
      if (auto* error = std::get_if<resolve_error>(&resolved)) {
        emit_resolve_error(*error, ctx);
        return None;
      }
      return std::get<series>(std::move(resolved));
    }
    // Default: look up the "bytes" field.
    auto batch = to_record_batch(slice);
    auto column = batch->GetColumnByName("data");
    if (not column) {
      diagnostic::warning("expected a field named `data`")
        .primary(args_.operator_location)
        .emit(ctx);
      return None;
    }
    auto rt = try_as<record_type>(slice.schema());
    if (not rt) {
      return None;
    }
    auto ty = rt->field("data");
    if (not ty) {
      return None;
    }
    return series{*ty, column};
  }

  WriteChunksArgs args_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.write_chunks";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteChunksArgs, WriteChunks>{};
    d.optional_positional("field", &WriteChunksArgs::field, "field");
    d.operator_location(&WriteChunksArgs::operator_location);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::write_chunks

TENZIR_REGISTER_PLUGIN(tenzir::plugins::write_chunks::plugin)
