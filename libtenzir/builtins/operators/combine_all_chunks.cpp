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

class combine_all_chunks_operator final
  : public crtp_operator<combine_all_chunks_operator> {
public:
  combine_all_chunks_operator() = default;

  auto name() const -> std::string override {
    return "_combine_all_chunks";
  }

  auto operator()(generator<chunk_ptr> input, operator_control_plane&) const
    -> generator<chunk_ptr> {
    auto chunks = std::vector<chunk_ptr>{};
    for (auto&& chunk : input) {
      if (chunk and chunk->size() != 0) {
        chunks.push_back(std::move(chunk));
      }
      co_yield {};
    }
    auto size = size_t{0};
    for (const auto& c : chunks) {
      size += c->size();
    }
    auto buffer = std::vector<std::byte>{};
    buffer.reserve(size);
    for (const auto& c : chunks) {
      buffer.insert(buffer.end(), c->begin(), c->end());
    }
    co_yield chunk::make(std::move(buffer));
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, combine_all_chunks_operator& x) {
    return f.object(x).fields();
  }
};

class combine_all_chunks final
  : public virtual operator_plugin2<combine_all_chunks_operator>,
    public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation, session) const
    -> failure_or<operator_ptr> override {
    return std::make_unique<combine_all_chunks_operator>();
  }

  auto describe() const -> Description override {
    auto d = Describer<CombineAllChunksArgs, CombineAllChunks>{};
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::combine_all_chunks

TENZIR_REGISTER_PLUGIN(tenzir::plugins::combine_all_chunks::combine_all_chunks)
