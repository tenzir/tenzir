//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/async/signal.hpp"

namespace tenzir {

template <class Input>
OpenPipeline<Input>::OpenPipeline(Push<OperatorMsg<Input>>& push)
  : push_{push} {
}

template <class Input>
template <std::same_as<Input> In>
auto OpenPipeline<Input>::push(In input) -> Task<Result<void, In>> {
  if constexpr (std::same_as<Input, table_slice>) {
    LOGW("pushing {} rows to subpipeline", input.rows());
  } else {
    LOGW("pushing {} bytes to subpipeline", input ? input->size() : 0);
  }
  co_await push_(std::move(input));
  co_return {};
}

template <class Input>
auto OpenPipeline<Input>::close() -> Task<void> {
  co_await push_(EndOfData{});
}

template class OpenPipeline<void>;
template class OpenPipeline<chunk_ptr>;
template class OpenPipeline<table_slice>;

template auto OpenPipeline<chunk_ptr>::push(chunk_ptr)
  -> Task<Result<void, chunk_ptr>>;
template auto OpenPipeline<table_slice>::push(table_slice)
  -> Task<Result<void, table_slice>>;

} // namespace tenzir
