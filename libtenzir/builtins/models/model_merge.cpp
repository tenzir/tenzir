//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/model.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <fmt/format.h>

namespace tenzir::plugins::model_merge {

namespace {

class instance final : public aggregation_instance {
public:
  explicit instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(table_slice const& input, session ctx) -> void override {
    if (failed_) {
      return;
    }
    for (auto& arg : eval(expr_, input, ctx)) {
      if (is<null_type>(arg.type)) {
        continue;
      }
      auto const records = arg.as<record_type>();
      if (not records) {
        fail(fmt::format("expected a model record, got `{}`", arg.type.kind()),
             ctx);
        return;
      }
      for (auto value : values3(*records->array)) {
        if (not value) {
          continue;
        }
        if (auto result = merge(*value); not result) {
          fail(std::move(result.unwrap_err()), ctx);
          return;
        }
      }
    }
  }

  auto get() const -> data override {
    if (failed_ or not state_) {
      return {};
    }
    return (*state_)->get();
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto result = flatbuffers::Offset<fbs::Data>{};
    if (state_ and not failed_) {
      result = pack(fbb, (*state_)->get_for_checkpoint());
    }
    auto const fb = fbs::aggregation::CreateModelMerge(fbb, failed_, result);
    fbb.Finish(fb);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    auto const fb
      = flatbuffer<fbs::aggregation::ModelMerge>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN("failed to restore `model_merge` aggregation instance: "
                  "invalid FlatBuffer");
      return false;
    }
    reset();
    if ((*fb)->failed()) {
      failed_ = true;
      return true;
    }
    if (not(*fb)->result()) {
      return true;
    }
    auto restored = data{};
    if (auto error = unpack(*(*fb)->result(), restored); error) {
      TENZIR_WARN("failed to restore `model_merge` aggregation instance: {}",
                  error);
      return false;
    }
    auto const* record = try_as<tenzir::record>(&restored);
    if (not record) {
      TENZIR_WARN("failed to restore `model_merge` aggregation instance: "
                  "persisted result is not a record");
      return false;
    }
    auto envelope_result = parse_model_envelope(*record);
    if (not envelope_result) {
      TENZIR_WARN("failed to restore `model_merge` aggregation instance: {}",
                  envelope_result.unwrap_err());
      return false;
    }
    auto envelope = std::move(envelope_result).unwrap();
    auto provider_result = find_model_plugin(envelope);
    if (not provider_result) {
      TENZIR_WARN("failed to restore `model_merge` aggregation instance: {}",
                  provider_result.unwrap_err());
      return false;
    }
    auto state_result
      = provider_result.unwrap()->make_model_merge_state(*record);
    if (not state_result) {
      TENZIR_WARN("failed to restore `model_merge` aggregation instance: "
                  "malformed `{}` model: {}",
                  envelope.model, state_result.unwrap_err());
      return false;
    }
    state_.emplace(std::move(state_result).unwrap());
    model_ = std::string{envelope.model};
    version_ = envelope.version;
    return true;
  }

  auto reset() -> void override {
    // Deliberately keep `warned_`: it deduplicates diagnostics over the
    // lifetime of the instance, not per row.
    state_.reset();
    model_.clear();
    version_ = 0;
    failed_ = false;
  }

private:
  auto merge(record_view3 model) -> Result<void, std::string> {
    auto envelope_result = parse_model_envelope(model);
    if (not envelope_result) {
      return Err{fmt::format("malformed model record: {}",
                             envelope_result.unwrap_err())};
    }
    auto envelope = std::move(envelope_result).unwrap();
    if (not state_) {
      auto plugin_result = find_model_plugin(envelope);
      if (not plugin_result) {
        return Err{std::move(plugin_result).unwrap_err()};
      }
      auto state_result = plugin_result.unwrap()->make_model_merge_state(model);
      if (not state_result) {
        return Err{fmt::format("malformed `{}` model: {}", envelope.model,
                               state_result.unwrap_err())};
      }
      state_.emplace(std::move(state_result).unwrap());
      model_ = std::string{envelope.model};
      version_ = envelope.version;
      return {};
    }
    if (envelope.model != model_ or envelope.version != version_) {
      return Err{fmt::format(
        "incompatible models: expected `{}` version {}, got `{}` version {}",
        model_, version_, envelope.model, envelope.version)};
    }
    auto result = (*state_)->merge(model);
    if (not result) {
      return Err{fmt::format("cannot merge `{}` models: {}", model_,
                             result.unwrap_err())};
    }
    return {};
  }

  auto fail(std::string message, session ctx) -> void {
    failed_ = true;
    state_.reset();
    if (not warned_) {
      warned_ = true;
      diagnostic::warning("`model_merge` failed: {}", message)
        .primary(expr_.get_location().subloc(0, 1))
        .emit(ctx);
    }
  }

  ast::expression expr_;
  Option<Box<model_merge_state>> state_;
  std::string model_;
  uint64_t version_ = 0;
  bool failed_ = false;
  bool warned_ = false;
};

class plugin final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "model_merge";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("model", expr, "record")
          .parse(inv, ctx));
    return std::make_unique<instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::model_merge

TENZIR_REGISTER_PLUGIN(tenzir::plugins::model_merge::plugin)
