//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/to_kafka_legacy.hpp"

#include <tenzir/argument_parser2.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <variant>

namespace tenzir::plugins::kafka::legacy {

to_kafka_operator::to_kafka_operator(to_kafka_args args, record config)
  : args_{std::move(args)}, config_{std::move(config)} {
}

auto to_kafka_operator::operator()(generator<table_slice> input,
                                   operator_control_plane& ctrl) const
  -> generator<std::monostate> {
  auto& dh = ctrl.diagnostics();
  // Resolve secrets if explicit credentials or role are provided.
  auto resolved_creds = std::optional<tenzir::resolved_aws_credentials>{};
  if (args_.aws
      and (args_.aws->has_explicit_credentials() or args_.aws->role)) {
    resolved_creds.emplace();
    auto requests = args_.aws->make_secret_requests(*resolved_creds, dh);
    co_yield ctrl.resolve_secrets_must_yield(std::move(requests));
  }
  // Use top-level aws_region if provided, otherwise fall back to aws_iam.
  if (args_.aws_region) {
    if (not resolved_creds) {
      resolved_creds.emplace();
    }
    resolved_creds->region = args_.aws_region->inner;
  }
  co_yield {};
  auto config = configuration::make(config_, args_.aws, resolved_creds, dh);
  if (not config) {
    diagnostic::error(std::move(config).error()).primary(args_.op).emit(dh);
    co_return;
  }
  co_yield ctrl.resolve_secrets_must_yield(
    configure_or_request(args_.options, *config, ctrl.diagnostics()));
  auto p = producer::make(std::move(*config));
  if (not p) {
    diagnostic::error(std::move(p).error()).primary(args_.op).emit(dh);
    co_return;
  }
  const auto guard = detail::scope_guard([&] noexcept {
    TENZIR_DEBUG("[to_kafka] waiting 10 seconds to flush pending messages");
    if (const auto err = p->flush(10s); err.valid()) {
      TENZIR_WARN(err);
    }
    const auto num_messages = p->queue_size();
    if (num_messages > 0) {
      TENZIR_ERROR("[to_kafka] {} messages were not delivered", num_messages);
    }
  });
  const auto key = args_.key ? args_.key->inner : "";
  const auto timestamp = args_.timestamp ? args_.timestamp->inner : time{};
  for (const auto& slice : input) {
    if (slice.rows() == 0) {
      co_yield {};
      continue;
    }
    const auto& ms = eval(args_.message, slice, dh);
    for (const auto& s : ms) {
      match(
        *s.array,
        [&](const concepts::one_of<arrow::BinaryArray, arrow::StringArray> auto&
              array) {
          for (auto i = int64_t{}; i < array.length(); ++i) {
            if (array.IsNull(i)) {
              diagnostic::warning("expected `string` or `blob`, got `null`")
                .primary(args_.message)
                .emit(dh);
              continue;
            }
            if (auto e = p->produce(args_.topic, as_bytes(array.Value(i)), key,
                                    timestamp);
                e.valid()) {
              diagnostic::error(std::move(e)).primary(args_.op).emit(dh);
            }
          }
        },
        [&](const auto&) {
          diagnostic::warning("expected `string` or `blob`, got `{}`",
                              s.type.kind())
            .primary(args_.message)
            .emit(dh);
        });
    }
    p->poll(0ms);
  }
}

auto to_kafka_operator::name() const -> std::string {
  return "to_kafka";
}

auto to_kafka_operator::detached() const -> bool {
  return true;
}

auto to_kafka_operator::optimize(const expression&, event_order) const
  -> optimize_result {
  return do_not_optimize(*this);
}

auto make_to_kafka(operator_factory_plugin::invocation inv, session ctx,
                   const record& defaults) -> failure_or<operator_ptr> {
  auto args = to_kafka_args{};
  TRY(resolve_entities(args.message, ctx));
  auto iam_opts = std::optional<located<record>>{};
  TRY(argument_parser2::operator_("to_kafka")
        .positional("topic", args.topic)
        .named_optional("message", args.message, "blob|string")
        .named("key", args.key)
        .named("timestamp", args.timestamp)
        .named("aws_region", args.aws_region)
        .named("aws_iam", iam_opts)
        .named_optional("options", args.options)
        .parse(inv, ctx));
  if (iam_opts) {
    TRY(check_sasl_mechanism(args.options, ctx));
    TRY(check_sasl_mechanism(located{defaults, iam_opts->source}, ctx));
    args.options.inner["sasl.mechanism"] = "OAUTHBEARER";
    TRY(args.aws,
        tenzir::aws_iam_options::from_record(std::move(iam_opts).value(), ctx));
    // Region is required for Kafka MSK authentication.
    // Use top-level aws_region if provided, otherwise require aws_iam.region.
    if (not args.aws_region and not args.aws->region) {
      diagnostic::error("`aws_region` is required for Kafka MSK authentication")
        .primary(args.aws->loc)
        .emit(ctx);
      return failure::promise();
    }
  }
  TRY(validate_options(args.options, ctx));
  return std::make_unique<to_kafka_operator>(std::move(args), defaults);
}

} // namespace tenzir::plugins::kafka::legacy
