//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/aggregation.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/nova.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"

#include <chrono>

using namespace tenzir;

namespace {

/// A context whose pipeline checkpoints.
class CheckpointingCtx final : public test::NovaOpCtx {
public:
  using NovaOpCtx::NovaOpCtx;

  auto checkpoint_settings() const
    -> Option<CheckpointSettings const&> override {
    return settings_;
  }

private:
  CheckpointSettings settings_{std::chrono::seconds{1}, false};
};

} // namespace

TEST("local aggregation requires a pipeline without checkpoints") {
  auto aggregation = ir::OptimizeAggregation{
    .columns = {},
    .schema_name = "tenzir.summarize",
    .fallback = {},
  };
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto const& reg = provider.as_session().reg();
  // Without checkpoints, a source may run the fallback in a subpipeline.
  auto plain = test::NovaOpCtx{dh, reg};
  CHECK(pushdown::can_aggregate_locally(aggregation, plain));
  CHECK(std::move(dh).collect().empty());
  // With checkpoints, the source fails like the `summarize` that it replaced,
  // instead of spawning a subpipeline that the executor cannot checkpoint.
  dh = collecting_diagnostic_handler{};
  auto checkpointing = CheckpointingCtx{dh, reg};
  CHECK(not pushdown::can_aggregate_locally(aggregation, checkpointing));
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics.front().severity, severity::error);
  CHECK_EQUAL(diagnostics.front().message,
              "`summarize` does not support checkpointing with `--nova` yet");
}
