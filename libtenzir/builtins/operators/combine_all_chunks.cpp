//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/chunk.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/tql2/plugin.hpp"

namespace tenzir::plugins::combine_all_chunks {
namespace {

struct CombineAllChunksArgs {};

class CombineAllChunks final : public Operator<chunk_ptr, chunk_ptr> {
public:
  explicit CombineAllChunks(CombineAllChunksArgs) {
  }

  auto process(chunk_ptr input, Push<chunk_ptr>&, OpCtx&)
    -> Task<void> override {
    if (input and input->size() != 0) {
      chunks_.push_back(std::move(input));
    }
    co_return;
  }

  auto finalize(Push<chunk_ptr>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    auto size = size_t{0};
    for (const auto& c : chunks_) {
      size += c->size();
    }
    auto buffer = std::vector<std::byte>{};
    buffer.reserve(size);
    for (const auto& c : chunks_) {
      buffer.insert(buffer.end(), c->begin(), c->end());
    }
    co_await push(chunk::make(std::move(buffer)));
    co_return FinalizeBehavior::done;
  }

private:
  std::vector<chunk_ptr> chunks_;
};

class combine_all_chunks final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "_combine_all_chunks";
  }

  auto describe() const -> Description override {
    auto d = Describer<CombineAllChunksArgs, CombineAllChunks>{};
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::combine_all_chunks

TENZIR_REGISTER_PLUGIN(tenzir::plugins::combine_all_chunks::combine_all_chunks)
