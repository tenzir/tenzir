//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_executor.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/diagnostics.hpp"

namespace tenzir {

auto pipeline_executor(
  pipeline_executor_actor::stateful_pointer<pipeline_executor_state> self,
  pipeline, std::string, receiver_actor<diagnostic> diagnostics,
  metrics_receiver_actor, node_actor, bool, bool, std::string)
  -> pipeline_executor_actor::behavior_type {
  return {
    [self, diagnostics](atom::start) -> caf::result<void> {
      auto diag = diagnostic::error("legacy pipeline executor was removed")
                    .note("rewrite this operator to use the IR executor")
                    .done();
      auto err = diag.to_error();
      if (diagnostics) {
        self->mail(std::move(diag)).send(diagnostics);
      }
      self->quit(err);
      return err;
    },
    [](atom::pause) -> caf::result<void> {
      return {};
    },
    [](atom::resume) -> caf::result<void> {
      return {};
    },
  };
}

} // namespace tenzir
