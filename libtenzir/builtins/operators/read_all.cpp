//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>

#include <arrow/util/utf8.h>

#include <string_view>

namespace tenzir::plugins::read_all {

namespace {

struct ReadAllArgs {
  bool binary = false;
  location operator_location;
};

class ReadAll final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadAll(ReadAllArgs args) : args_{args} {
  }

  auto process(chunk_ptr input, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    if (input->size() == 0) {
      co_return;
    }
    buffer_.push_back(std::move(input));
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    auto joined = join_chunks(buffer_);
    auto builder = series_builder{};
    if (joined->size() == 0) {
      co_return FinalizeBehavior::done;
    }
    if (args_.binary) {
      builder.record().field("data", as_bytes(joined));
    } else {
      if (not arrow::util::ValidateUTF8(
            reinterpret_cast<const uint8_t*>(joined->data()),
            detail::narrow<std::int64_t>(joined->size()))) {
        diagnostic::warning("got invalid UTF-8")
          .primary(args_.operator_location)
          .hint("use `binary=true` if you are reading binary data")
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      builder.record().field(
        "data", std::string_view{reinterpret_cast<const char*>(joined->data()),
                                 joined->size()});
    }
    co_await push(builder.finish_assert_one_slice());
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
  }

private:
  ReadAllArgs args_;
  std::vector<chunk_ptr> buffer_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_all";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadAllArgs, ReadAll>{};
    d.named_optional("binary", &ReadAllArgs::binary);
    d.operator_location(&ReadAllArgs::operator_location);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_all

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_all::plugin)
