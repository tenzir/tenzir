//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// `measure2` is the experimental `tenzir2::TableSlice` ("events2") counterpart
// to the events path of `measure`. It counts the events in each incoming slice
// and emits one `tenzir.measure.events` metric per slice.
//
// This is intentionally minimal: events only (no byte-counting `chunk_ptr`
// variant), and no `schema_id` fingerprint or schema-definition modes, since
// the `tenzir2` type system exposes neither fingerprints nor definitions. The
// per-schema counter is keyed on the slice name, the only stable schema
// identity available.

#include <tenzir/async.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>

#include <tenzir2/table_slice.hpp>
#include <tenzir2/type_system/array/builder.hpp>

#include <cstdint>
#include <string>
#include <unordered_map>

namespace tenzir::plugins::measure2 {

namespace {

/// Arguments for `measure2`.
struct Measure2Args {
  bool cumulative = false;
};

/// Runtime operator: counts the events of each `tenzir2::TableSlice` and emits
/// a `tenzir.measure.events` metric slice.
class Measure2 final
  : public Operator<tenzir2::TableSlice, tenzir2::TableSlice> {
public:
  Measure2() = default;

  explicit Measure2(Measure2Args args) : args_{args} {
  }

  auto process(tenzir2::TableSlice input, Push<tenzir2::TableSlice>& push,
               OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    const auto rows = static_cast<uint64_t>(input.data_.length());
    if (rows == 0) {
      co_return;
    }
    auto& events = counters_[input.name_];
    events = args_.cumulative ? events + rows : rows;
    auto builder = tenzir2::array_builder_<tenzir2::record>{};
    auto row = builder.record();
    row.field("timestamp").data(tenzir2::clock::now());
    row.field("events").data(uint64_t{events});
    row.field("schema").data(input.name_);
    co_await push(make_slice(builder.finish()));
  }

  auto snapshot(Serde& serde) -> void override {
    serde("counters", counters_);
  }

private:
  auto make_slice(tenzir2::array_<tenzir2::record> data) const
    -> tenzir2::TableSlice {
    return tenzir2::TableSlice{
      std::string{"tenzir.measure.events"},
      tenzir2::clock::now(),
      tenzir2::ProvenanceToken{},
      std::move(data),
    };
  }

  Measure2Args args_;
  std::unordered_map<std::string, uint64_t> counters_;
};

class measure2_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "measure2";
  }

  auto describe() const -> Description override {
    auto d = Describer<Measure2Args, Measure2>{};
    d.named("cumulative", &Measure2Args::cumulative);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::measure2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::measure2::measure2_plugin)
