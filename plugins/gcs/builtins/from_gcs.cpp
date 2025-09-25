//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/from_file_base.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/filesystem/gcsfs.h>
#include <caf/actor_from_state.hpp>

#include <memory>

namespace tenzir::plugins::gcs {
namespace {

struct from_gcs_args final {
  from_file_args base_args;
  std::optional<location> anonymous;

  friend auto inspect(auto& f, from_gcs_args& x) -> bool {
    return f.object(x).fields(f.field("base_args", x.base_args),
                              f.field("anonymous", x.anonymous));
  }
};

class from_gcs_operator final : public crtp_operator<from_gcs_operator> {
public:
  from_gcs_operator() = default;

  explicit from_gcs_operator(from_gcs_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto uri = arrow::util::Uri{};
    auto reqs = std::vector{
      make_uri_request(args_.base_args.url, "gs://", uri, ctrl.diagnostics()),
    };
    co_yield ctrl.resolve_secrets_must_yield(std::move(reqs));
    auto path = std::string{};
    auto opts = arrow::fs::GcsOptions::FromUri(uri, &path);
    if (not opts.ok()) {
      diagnostic::error("failed to create Arrow GCS options: {}",
                        opts.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
      co_return;
    }
    if (args_.anonymous) {
      *opts = arrow::fs::GcsOptions::Anonymous();
    }
    auto fs = arrow::fs::GcsFileSystem::Make(*opts);
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow GCS filesystem: {}",
                        fs.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Spawning the actor detached because some parts of the Arrow filesystem
    // API are blocking.
    auto impl = scope_linked{ctrl.self().spawn<caf::linked + caf::detached>(
      caf::actor_from_state<from_file_state>, args_.base_args, path, path,
      fs.MoveValueUnsafe(), order_,
      std::make_unique<shared_diagnostic_handler>(ctrl.shared_diagnostics()),
      std::string{ctrl.definition()}, ctrl.node(), ctrl.is_hidden(),
      ctrl.metrics_receiver(), ctrl.operator_index())};
    while (true) {
      auto result = table_slice{};
      ctrl.self()
        .mail(atom::get_v)
        .request(impl.get(), caf::infinite)
        .then(
          [&](table_slice slice) {
            result = std::move(slice);
            ctrl.set_waiting(false);
          },
          [&](caf::error error) {
            diagnostic::error(std::move(error)).emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      if (result.rows() == 0) {
        break;
      }
      co_yield std::move(result);
    }
  }

  auto name() const -> std::string override {
    return "from_gcs";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order order) const
    -> optimize_result override {
    auto copy = std::make_unique<from_gcs_operator>(*this);
    copy->order_ = order;
    return optimize_result{std::nullopt, event_order::ordered, std::move(copy)};
  }

  friend auto inspect(auto& f, from_gcs_operator& x) -> bool {
    return f.object(x).fields(f.field("args_", x.args_),
                              f.field("order_", x.order_));
  }

private:
  from_gcs_args args_;
  event_order order_{event_order::ordered};
};

class from_gcs final : public operator_plugin2<from_gcs_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_gcs_args{};
    auto p = argument_parser2::operator_(name());
    args.base_args.add_to(p);
    p.named("anonymous", args.anonymous);
    TRY(p.parse(inv, ctx));
    TRY(auto result, args.base_args.handle(ctx));
    result.prepend(std::make_unique<from_gcs_operator>(std::move(args)));
    return std::make_unique<pipeline>(std::move(result));
  }
};

} // namespace
} // namespace tenzir::plugins::gcs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::gcs::from_gcs)
