//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/try.hpp"

#include <concepts>

namespace tenzir {

/// Creates the spawner for a source operator that relays the events of a user
/// pipeline, such as `from_tcp { read_lines }`.
///
/// The output element type of such an operator equals the output of its user
/// pipeline, so the spawner infers that type for byte input and picks `Current`
/// for `nova::Events` and `Legacy` otherwise. Remove it together with the
/// legacy implementations, since a single implementation needs no spawner.
///
/// @tparam Args The shared argument bundle of both implementations.
/// @tparam Legacy The `Operator<void, table_slice>` implementation.
/// @tparam Current The `Operator<void, nova::Events>` implementation.
/// @param pipe_arg The handle that `Describer::pipeline` returned for the user
/// pipeline.
template <class Args, class Legacy, class Current>
auto make_stream_source_spawner(auto pipe_arg) {
  return [pipe_arg]<class Input>(
           DescribeCtx& ctx) -> failure_or<Option<SpawnWith<Args, Input>>> {
    if constexpr (not std::same_as<Input, void>) {
      return {};
    } else {
      TRY(auto pipe, ctx.get(pipe_arg));
      TRY(auto output, pipe.inner.infer_type(tag_v<chunk_ptr>, ctx));
      return match(
        output,
        [](tag<nova::Events>) -> Option<SpawnWith<Args, Input>> {
          return SpawnWith<Args, void>{
            [](Args args) -> Box<Operator<void, nova::Events>> {
              return Current{std::move(args)};
            }};
        },
        [](auto) -> Option<SpawnWith<Args, Input>> {
          return SpawnWith<Args, void>{
            [](Args args) -> Box<Operator<void, table_slice>> {
              return Legacy{std::move(args)};
            }};
        });
    }
  };
}

} // namespace tenzir
