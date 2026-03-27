//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin/register.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/try.hpp>

namespace tenzir::plugins::split_bytes {

namespace {

struct SplitArgs {
  located<uint64_t> size;
};

class SplitBytes final : public Operator<chunk_ptr, chunk_ptr> {
public:
  explicit SplitBytes(SplitArgs args) : args_{args} {
  }

  auto process(chunk_ptr input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    const auto sz = static_cast<size_t>(args_.size.inner);
    auto offset = size_t{0};
    while (offset < input->size()) {
      auto len = std::min(sz, input->size() - offset);
      co_await push(input->slice(offset, len));
      offset += len;
    }
  }

private:
  SplitArgs args_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "split_bytes";
  }

  auto describe() const -> Description override {
    auto d = Describer<SplitArgs, SplitBytes>{};
    auto size_arg = d.positional("size", &SplitArgs::size);
    d.validate([size_arg](DescribeCtx& ctx) -> Empty {
      TRY(auto sz, ctx.get(size_arg));
      if (sz.inner == 0) {
        diagnostic::error("size must be greater than zero")
          .primary(sz.source)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::split_bytes

TENZIR_REGISTER_PLUGIN(tenzir::plugins::split_bytes::plugin)
