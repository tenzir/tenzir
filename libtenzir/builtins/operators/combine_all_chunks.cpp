//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/plugin.hpp"
#include "tenzir/tql2/plugin.hpp"

namespace tenzir::plugins::combine_all_chunks {
namespace {

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
      if (chunk && chunk->size() != 0) {
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
  : public operator_plugin2<combine_all_chunks_operator> {
public:
  auto make(invocation, session) const -> failure_or<operator_ptr> override {
    return std::make_unique<combine_all_chunks_operator>();
  }
};

} // namespace
} // namespace tenzir::plugins::combine_all_chunks

TENZIR_REGISTER_PLUGIN(tenzir::plugins::combine_all_chunks::combine_all_chunks)
