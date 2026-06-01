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
#include <tenzir/view3.hpp>

namespace tenzir::plugins::write_chunks {

namespace {

struct WriteChunksArgs {
  location operator_location;
};

class WriteChunks final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteChunks(WriteChunksArgs args) : args_{args} {
  }

  auto process(table_slice slice, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (slice.rows() == 0) {
      co_return;
    }
    auto batch = to_record_batch(slice);
    auto column = batch->GetColumnByName("bytes");
    if (not column) {
      diagnostic::warning("expected a field named `bytes`")
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    auto rt = try_as<record_type>(slice.schema());
    if (not rt) {
      co_return;
    }
    auto ty = rt->field("bytes");
    if (not ty) {
      co_return;
    }
    auto selected = series{*ty, column};
    if (selected.type.kind().is<null_type>()) {
      co_return;
    }
    if (selected.type.kind().is<blob_type>()) {
      for (auto value : selected.values3<blob_type>()) {
        if (not value) {
          continue;
        }
        co_await push(chunk::copy(*value));
      }
      co_return;
    }
    diagnostic::error("field `bytes` has unsupported type `{}`",
                      selected.type.kind())
      .primary(args_.operator_location)
      .hint("`write_chunks` expects the `bytes` field to have type `blob`")
      .emit(ctx);
    co_return;
  }

private:
  WriteChunksArgs args_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.write_chunks";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteChunksArgs, WriteChunks>{};
    d.operator_location(&WriteChunksArgs::operator_location);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::write_chunks

TENZIR_REGISTER_PLUGIN(tenzir::plugins::write_chunks::plugin)
