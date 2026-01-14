//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/execution_node.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/connect_to_node.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/base58.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/fanout_counter.hpp"
#include "tenzir/detail/hex_encode.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/detail/weak_handle.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ecc.hpp"
#include "tenzir/metric_handler.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline_buffer_stats.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/secret_store.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/table_slice.hpp"

#include <arrow/config.h>
#include <arrow/util/byte_size.h>
#include <caf/actor_addr.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/anon_mail.hpp>
#include <caf/exit_reason.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <string_view>

namespace tenzir {

namespace {

template <class F>
void loop_at(caf::scheduled_actor* self, caf::actor_clock::time_point start,
             caf::timespan delay, F&& f) {
  auto run = [self, start, delay, f = std::forward<F>(f)]() mutable {
    std::invoke(f);
    loop_at(self, start + delay, delay, std::move(f));
  };
  self->delay_until_fn(start + delay, std::move(run));
}

template <class F>
void loop(caf::scheduled_actor* self, caf::timespan delay, F&& f) {
  loop_at(self, self->clock().now() + delay, delay, std::forward<F>(f));
}

using namespace std::chrono_literals;
using namespace si_literals;

template <class Element = void>
struct exec_node_defaults {
  /// Defines how much free capacity must be in the inbound buffer of the
  /// execution node before it requests further data.
  inline static constexpr uint64_t min_elements = 1;

  /// Defines the upper bound for the inbound buffer of the execution node.
  inline static constexpr uint64_t max_elements = 0;

  /// Defines how many batches may be buffered at most. This is an additional
  /// upper bound to the number of buffered elements that protects against a
  /// high memory usage from having too many small batches.
  inline static constexpr uint64_t max_batches = 10;
};

template <>
struct exec_node_defaults<table_slice> : exec_node_defaults<> {
  /// Defines how much free capacity must be in the inbound buffer of the
  /// execution node before it requests further data.
  inline static constexpr uint64_t min_elements = 8_Ki;

  /// Defines the upper bound for the inbound buffer of the execution node.
  inline static constexpr uint64_t max_elements = 254_Ki;
};

template <>
struct exec_node_defaults<chunk_ptr> : exec_node_defaults<> {
  /// Defines how much free capacity must be in the inbound buffer of the
  /// execution node before it requests further data.
  inline static constexpr uint64_t min_elements = 128_Ki;

  /// Defines the upper bound for the inbound buffer of the execution node.
  inline static constexpr uint64_t max_elements = 4_Mi;
};

} // namespace

namespace {

template <class... Duration>
  requires(std::is_same_v<Duration, duration> && ...)
auto make_timer_guard(Duration&... elapsed) {
  return detail::scope_guard(
    [&, start_time = std::chrono::steady_clock::now()]() noexcept {
      const auto delta = std::chrono::steady_clock::now() - start_time;
      ((void)(elapsed += delta, true), ...);
    });
}

// Return an underestimate for the total number of referenced bytes for a
// vector of table slices, excluding the schema and disregarding any overlap
// or custom information from extension types.
auto approx_bytes(const table_slice& events) -> uint64_t {
  return events.approx_bytes();
}

auto approx_bytes(const chunk_ptr& bytes) -> uint64_t {
  return bytes ? bytes->size() : 0;
}

template <class Input, class Output>
struct exec_node_state;

template <class Input, class Output>
struct exec_node_diagnostic_handler final : public diagnostic_handler {
  exec_node_diagnostic_handler(exec_node_state<Input, Output>& state,
                               receiver_actor<diagnostic> handle)
    : state{state}, handle{std::move(handle)} {
    loop_at(state.self, state.self->clock().now(),
            defaults::diagnostic_deduplication_interval, [this] {
              deduplicator_.clear();
            });
  }

  void emit(diagnostic diag) override {
    if (not censor.is_noop()) {
      diag.message = censor.censor(std::move(diag.message));
      for (auto& annotation : diag.annotations) {
        annotation.text = censor.censor(std::move(annotation.text));
      }
      for (auto& note : diag.notes) {
        note.message = censor.censor(std::move(note.message));
      }
    }
    TENZIR_TRACE("{} {} emits diagnostic: {:?}", *state.self, state.op->name(),
                 diag);
    switch (state.op->strictness()) {
      case strictness_level::strict:
        if (diag.severity == severity::warning) {
          diag.severity = severity::error;
        }
        break;
      case strictness_level::normal:
        break;
    }
    if (diag.severity == severity::error) {
      throw std::move(diag);
    }
    if (deduplicator_.insert(diag)) {
      state.self->mail(std::move(diag)).send(handle);
    }
  }

  void add_to_censor(ecc::cleansing_blob v) {
    censor.secrets.emplace(std::move(v));
  }

private:
  exec_node_state<Input, Output>& state;
  receiver_actor<diagnostic> handle = {};
  diagnostic_deduplicator deduplicator_;
  secret_censor censor;
};

template <class Input, class Output>
struct exec_node_control_plane final : public operator_control_plane {
  exec_node_control_plane(exec_node_state<Input, Output>& state,
                          receiver_actor<diagnostic> diagnostic_handler,
                          metrics_receiver_actor metric_receiver,
                          uint64_t op_index, bool has_terminal, bool is_hidden)
    : state{state},
      diagnostic_handler{
        std::make_unique<exec_node_diagnostic_handler<Input, Output>>(
          state, std::move(diagnostic_handler))},
      metrics_receiver_{std::move(metric_receiver)},
      operator_index_{op_index},
      has_terminal_{has_terminal},
      is_hidden_{is_hidden} {
  }

  auto self() noexcept -> exec_node_actor::base& override {
    return *state.self;
  }

  auto definition() const noexcept -> std::string_view override {
    return state.definition;
  }

  auto run_id() const noexcept -> uuid override {
    return state.run_id;
  }

  auto node() noexcept -> node_actor override {
    return state.weak_node.lock();
  }

  auto operator_index() const noexcept -> uint64_t override {
    return operator_index_;
  }

  auto diagnostics() noexcept -> diagnostic_handler& override {
    return *diagnostic_handler;
  }

  auto metrics(type t) noexcept -> metric_handler override {
    return metric_handler{
      metrics_receiver_,
      operator_index_,
      t,
    };
  }

  auto metrics_receiver() const noexcept -> metrics_receiver_actor override {
    return metrics_receiver_;
  }

  auto no_location_overrides() const noexcept -> bool override {
    return caf::get_or(content(state.self->config()),
                       "tenzir.no-location-overrides", false);
  }

  auto has_terminal() const noexcept -> bool override {
    return has_terminal_;
  }

  auto is_hidden() const noexcept -> bool override {
    return is_hidden_;
  }

  auto pipeline_id() const noexcept -> std::string_view override {
    return state.pipeline_id;
  }

  auto set_waiting(bool value) noexcept -> void override {
    state.waiting = value;
    if (not state.waiting) {
      state.schedule_run(false);
    }
  }

  struct located_resolved_secret {
    location loc;
    ecc::cleansing_blob value;

    located_resolved_secret(location loc) : loc{loc} {
    }
  };

  using request_map_t
    = std::unordered_map<std::string, located_resolved_secret>;

  struct secret_finisher {
    class secret secret;
    secret_request_callback callback;
    location loc;

    static auto apply_transformation(ecc::cleansing_blob blob,
                                     fbs::data::SecretTransformations operation,
                                     diagnostic_handler& dh, location loc)
      -> failure_or<ecc::cleansing_blob> {
#define X_ENCODE(OPERATION, FUNCTION)                                          \
  case OPERATION: {                                                            \
    const auto encoded = FUNCTION(std::string_view{                            \
      reinterpret_cast<const char*>(blob.data()),                              \
      reinterpret_cast<const char*>(blob.data() + blob.size()),                \
    });                                                                        \
    const auto enc_bytes = as_bytes(encoded);                                  \
    blob.assign(enc_bytes.begin(), enc_bytes.end());                           \
    return blob;                                                               \
  }

#define X_DECODE(OPERATION, FUNCTION)                                          \
  case OPERATION: {                                                            \
    const auto decoded = FUNCTION(std::string_view{                            \
      reinterpret_cast<const char*>(blob.data()),                              \
      reinterpret_cast<const char*>(blob.data() + blob.size()),                \
    });                                                                        \
    if (not decoded) {                                                         \
      diagnostic::error("failed to `" #OPERATION "` secret value")             \
        .primary(loc)                                                          \
        .emit(dh);                                                             \
      return failure::promise();                                               \
    }                                                                          \
    const auto dec_bytes = as_bytes(*decoded);                                 \
    blob.assign(dec_bytes.begin(), dec_bytes.end());                           \
    return blob;                                                               \
  }
      switch (operation) {
        using enum fbs::data::SecretTransformations;
        X_ENCODE(encode_base64, detail::base64::encode)
        X_DECODE(decode_base64, detail::base64::try_decode)
        X_ENCODE(encode_url, curl::escape)
        X_DECODE(decode_url, curl::try_unescape)
        X_ENCODE(encode_base58, detail::base58::encode)
        X_DECODE(decode_base58, detail::base58::decode)
        X_ENCODE(encode_hex, detail::hex::encode)
        X_DECODE(decode_hex, detail::hex::decode)
      }
#undef X_ENCODE
#undef X_DECODE
      TENZIR_UNREACHABLE();
    }

    auto finish(const request_map_t& requested,
                exec_node_diagnostic_handler<Input, Output>& dh) const
      -> failure_or<void> {
      // For every element in the original secret
      bool all_literal = true;
      using ret_t = failure_or<ecc::cleansing_blob>;
      const auto f = detail::overload{
        [](const fbs::data::SecretLiteral& l) -> ret_t {
          const auto& v = detail::secrets::deref(l.value());
          const auto v_bytes = as_bytes(v);
          return ecc::cleansing_blob{v_bytes.begin(), v_bytes.end()};
        },
        [&requested](const fbs::data::SecretName& l) -> ret_t {
          const auto it
            = requested.find(detail::secrets::deref(l.value()).str());
          TENZIR_ASSERT(it != requested.end());
          return it->second.value;
        },
        [](this const auto& self,
           const fbs::data::SecretConcatenation& concat) -> ret_t {
          auto res = ecc::cleansing_blob{};
          for (auto* p : detail::secrets::deref(concat.secrets())) {
            TRY(auto part, match(detail::secrets::deref(p), self));
            res.insert(res.end(), part.begin(), part.end());
          }
          return res;
        },
        [&dh, loc
              = this->loc](this const auto& self,
                           const fbs::data::SecretTransformed& trafo) -> ret_t {
          TRY(auto nested, match(detail::secrets::deref(trafo.secret()), self));
          return apply_transformation(std::move(nested), trafo.transformation(),
                                      dh, loc);
        },
      };
      TRY(auto res, match(secret, f));
      // Finally, we invoke the callback
      return callback(resolved_secret_value{std::move(res), all_literal});
    }
  };

  auto resolve_secrets_must_yield(std::vector<secret_request> requests,
                                  final_callback_t final_callback)
    -> secret_resolution_sentinel override {
    auto requested_secrets = std::make_shared<request_map_t>();
    auto finishers = std::vector<secret_finisher>{};
    for (auto& req : requests) {
      const auto collect = detail::overload{
        [](const fbs::data::SecretLiteral&) -> void {
          ; /* noop */
        },
        [&requested_secrets,
         loc = req.location](const fbs::data::SecretName& n) -> void {
          requested_secrets->try_emplace(
            detail::secrets::deref(n.value()).str(), loc);
        },
        [](this const auto& self,
           const fbs::data::SecretConcatenation& concat) -> void {
          for (const auto* p : detail::secrets::deref(concat.secrets())) {
            match(detail::secrets::deref(p), self);
          }
        },
        [](this const auto& self,
           const fbs::data::SecretTransformed& trafo) -> void {
          match(detail::secrets::deref(trafo.secret()), self);
        },
      };
      match(req.secret, collect);
      finishers.emplace_back(std::move(req.secret), std::move(req.callback),
                             req.location);
    }
    if (requested_secrets->empty()) {
      /// Finish all secrets via the respective finisher.
      auto success = true;
      for (const auto& f : finishers) {
        success &= static_cast<bool>(
          f.finish(*requested_secrets, *diagnostic_handler));
      }
      success &= static_cast<bool>(final_callback(true));
      // We want to avoid re-scheduling in the error case, so we set_waiting
      if (not success) {
        set_waiting(true);
      }
      return secret_resolution_sentinel{};
    }
    auto node_callback = [this, finishers = std::move(finishers),
                          final_callback = std::move(final_callback),
                          requested_secrets = std::move(requested_secrets)](
                           caf::expected<node_actor> maybe_actor) {
      if (not maybe_actor) {
        diagnostic::error("no Tenzir Node to resolve secrets")
          .primary(finishers.front().loc)
          .emit(diagnostics());
        std::ignore = final_callback(false);
        return;
      }
      auto fan = detail::make_fanout_counter_with_error<diagnostic>(
        requested_secrets->size(),
        [this, requested_secrets, finishers = std::move(finishers),
         final_callback]() {
          /// Finish all secrets via the respective finisher.
          bool success = true;
          for (const auto& f : finishers) {
            success &= static_cast<bool>(
              f.finish(*requested_secrets, *diagnostic_handler));
          }
          success &= static_cast<bool>(final_callback(success));
          /// We do not want to re-schedule ourselves in the error case.
          if (success) {
            set_waiting(false);
          }
        },
        [this, final_callback](std::span<diagnostic> diags) {
          TENZIR_ASSERT(std::ranges::any_of(
                          diags,
                          [](const auto& severity) {
                            return severity == severity::error;
                          },
                          &diagnostic::severity),
                        "failed secret resolution must have produced at least "
                        "one error");
          for (auto& d : diags) {
            diagnostics().emit(std::move(d));
          }
          /// We do not want to re-schedule ourselves in the error case.
          std::ignore = final_callback(false);
        });
      for (auto& [name, out] : *requested_secrets) {
        auto key_pair = ecc::generate_keypair();
        TENZIR_ASSERT(key_pair);
        auto public_key = key_pair->public_key;
        state.self->mail(atom::resolve_v, name, std::move(public_key))
          .request(*maybe_actor, caf::infinite)
          .then(
            [fan, keys = *key_pair, name, &out](secret_resolution_result res) {
              match(
                res,
                [&](const encrypted_secret_value& v) {
                  auto decrypted = ecc::decrypt(v.value, keys);
                  if (not decrypted) {
                    fan->receive_error(
                      diagnostic::error("failed to decrypt secret: {}",
                                        decrypted.error())
                        .primary(out.loc)
                        .note("secret `{}` failed", name)
                        .done());
                    return;
                  }
                  out.value = std::move(*decrypted);
                  fan->receive_success();
                  return;
                },
                [&](const secret_resolution_error& e) {
                  fan->receive_error(
                    diagnostic::error("could not get secret value: {}",
                                      e.message)
                      .primary(out.loc)
                      .note("secret `{}` failed", name)
                      .done());
                });
            },
            [fan, loc = out.loc](caf::error e) {
              fan->receive_error(
                diagnostic::error(std::move(e)).primary(loc).done());
            });
      }
    };
    set_waiting(true);
    auto node = this->node();
    if (node) {
      node_callback(node);
      return {};
    }
    connect_to_node(state.self, std::move(node_callback));
    return {};
  }

  exec_node_state<Input, Output>& state;
  std::unique_ptr<exec_node_diagnostic_handler<Input, Output>> diagnostic_handler
    = {};
  metrics_receiver_actor metrics_receiver_ = {};
  uint64_t operator_index_ = {};
  bool has_terminal_ = {};
  bool is_hidden_ = {};
};

template <class Input, class Output>
struct exec_node_state {
  exec_node_state(exec_node_actor::pointer self, operator_ptr op,
                  std::string definition, const node_actor& node,
                  const receiver_actor<diagnostic>& diagnostic_handler,
                  const metrics_receiver_actor& metrics_receiver, int index,
                  bool has_terminal, bool is_hidden, uuid run_id,
                  std::string pipeline_id)
    : self{self},
      definition{std::move(definition)},
      run_id{run_id},
      pipeline_id{std::move(pipeline_id)},
      op{std::move(op)},
      metrics_receiver{metrics_receiver} {
    // Initialize buffer stats for non-source operators of non-hidden pipelines
    if constexpr (not std::is_same_v<Input, std::monostate>) {
      if (not is_hidden and not this->pipeline_id.empty()) {
        buffer_stats = pipeline_buffer_registry::instance().get_or_create(
          this->pipeline_id);
      }
    }
    auto read_config = [&]<class T>(std::string_view config, T min, T fallback,
                                    bool element_specific) -> T {
      static_assert(caf::detail::tl_contains_v<data::types, T>);
      auto result
        = caf::get_or(content(self->system().config()),
                      fmt::format("tenzir.demand.{}", config), fallback);
      if (element_specific) {
        result = caf::get_or(content(self->system().config()),
                             fmt::format("tenzir.demand.{}.{}", config,
                                         operator_type_name<Input>()),
                             result);
      }
      return std::max(min, result);
    };
    const auto demand_settings = this->op->demand();
    min_elements
      = demand_settings.min_elements
          ? *demand_settings.min_elements
          : read_config("min-elements", uint64_t{1}, min_elements, true);
    max_elements
      = demand_settings.max_elements
          ? *demand_settings.max_elements
          : read_config("max-elements", min_elements, max_elements, true);
    max_batches
      = demand_settings.max_batches
          ? *demand_settings.max_batches
          : read_config("max-batches", uint64_t{1}, max_batches, false);
    min_backoff
      = demand_settings.min_backoff
          ? *demand_settings.min_backoff
          : read_config("min-backoff", duration{std::chrono::milliseconds{1}},
                        min_backoff, false);
    max_backoff
      = demand_settings.max_backoff
          ? *demand_settings.max_backoff
          : read_config("min-backoff", min_backoff, max_backoff, false);
    backoff_rate = demand_settings.backoff_rate
                     ? *demand_settings.backoff_rate
                     : read_config("backoff-rate", 1.0, backoff_rate, false);
    auto time_starting_guard
      = make_timer_guard(metrics.time_scheduled, metrics.time_starting);
    metrics.operator_index = index;
    metrics.operator_name = this->op->name();
    metrics.inbound_measurement.unit = operator_type_name<Input>();
    metrics.outbound_measurement.unit = operator_type_name<Output>();
    // We make an exception here for transformations, which are always considered
    // internal as they cannot transport data outside of the pipeline.
    metrics.internal = this->op->internal()
                       and (std::is_same_v<Input, std::monostate>
                            or std::is_same_v<Output, std::monostate>);
    ctrl = std::make_unique<exec_node_control_plane<Input, Output>>(
      *this, diagnostic_handler, metrics_receiver, index, has_terminal,
      is_hidden);
    // The node actor must be set when the operator is not a source.
    TENZIR_ASSERT(node or (this->op->location() != operator_location::remote));
    weak_node = node;
  }

  auto make_behavior() -> exec_node_actor::behavior_type {
    if (self->getf(caf::scheduled_actor::is_detached_flag)) {
      const auto name = fmt::format("tnz.{}", this->op->name());
      caf::detail::set_thread_name(name.c_str());
    }
    self->set_exception_handler(
      [this](const std::exception_ptr& exception) -> caf::error {
        auto error = std::invoke([&] {
          try {
            std::rethrow_exception(exception);
          } catch (diagnostic& diag) {
            return std::move(diag).to_error();
          } catch (panic_exception& panic) {
            auto has_node
              = self->system().registry().get("tenzir.node") != nullptr;
            auto diagnostic = to_diagnostic(panic);
            if (has_node) {
              auto buffer = std::stringstream{};
              buffer << "internal error in operator\n";
              auto printer = make_diagnostic_printer(
                std::nullopt, color_diagnostics::no, buffer);
              printer->emit(diagnostic);
              auto string = std::move(buffer).str();
              if (not string.empty() and string.back() == '\n') {
                string.pop_back();
              }
              TENZIR_ERROR(string);
            }
            return std::move(diagnostic).to_error();
          } catch (const std::exception& err) {
            return diagnostic::error("{}", err.what())
              .note("unhandled exception in {} {}", *self, op->name())
              .to_error();
          } catch (...) {
            return diagnostic::error("unhandled exception in {} {}", *self,
                                     op->name())
              .to_error();
          }
        });
        if (start_rp.pending()) {
          start_rp.deliver(std::move(error));
          return ec::silent;
        }
        return error;
      });
    return {
      [this](atom::start,
             std::vector<caf::actor>& all_previous) -> caf::result<void> {
        auto time_scheduled_guard
          = make_timer_guard(metrics.time_scheduled, metrics.time_starting);
        return start(std::move(all_previous));
      },
      [this](atom::pause) -> caf::result<void> {
        auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
        return pause();
      },
      [this](atom::resume) -> caf::result<void> {
        auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
        return resume();
      },
      [this](diagnostic& diag) -> caf::result<void> {
        auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
        ctrl->diagnostics().emit(std::move(diag));
        return {};
      },
      [this](atom::push, table_slice& events) -> caf::result<void> {
        auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
        if constexpr (std::is_same_v<Input, table_slice>) {
          return push(std::move(events));
        } else {
          return caf::make_error(
            ec::logic_error,
            fmt::format("{} does not accept events as input", *self));
        }
      },
      [this](atom::push, chunk_ptr& bytes) -> caf::result<void> {
        auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
        if constexpr (std::is_same_v<Input, chunk_ptr>) {
          return push(std::move(bytes));
        } else {
          return caf::make_error(
            ec::logic_error,
            fmt::format("{} does not accept bytes as input", *self));
        }
      },
      [this](atom::pull, exec_node_sink_actor& sink, uint64_t elements,
             uint64_t batches) -> caf::result<void> {
        auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
        if constexpr (not std::is_same_v<Output, std::monostate>) {
          return pull(std::move(sink), elements, batches);
        } else {
          return caf::make_error(
            ec::logic_error,
            fmt::format("{} is a sink and must not be pulled from", *self));
        }
      },
      [this](const caf::exit_msg& msg) -> caf::result<void> {
        auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
        handle_exit_msg(msg);
        return {};
      },
    };
  }

  static constexpr auto name = "exec-node";

  /// A pointer to the parent actor.
  exec_node_actor::pointer self = {};

  /// The definition of this pipeline.
  std::string definition;

  /// A unique identifier for the current run.
  uuid run_id = {};

  /// The pipeline's unique identifier for buffer metrics.
  std::string pipeline_id;

  /// Shared stats for tracking buffered data across all exec nodes in pipeline.
  std::shared_ptr<pipeline_buffer_stats> buffer_stats;

  /// Buffer limits derived from the configuration.
  uint64_t min_elements = exec_node_defaults<Input>::min_elements;
  uint64_t max_elements = exec_node_defaults<Input>::max_elements;
  uint64_t max_batches = exec_node_defaults<Input>::max_batches;

  /// The operator owned by this execution node.
  operator_ptr op = {};

  /// The instance created by the operator. Must be created at most once.
  std::optional<generator<Output>> instance = {};
  /// The output of the operator that was generated at the start of the
  /// execution node. We do not have demand at that time, so we can't directly
  /// send it to its downstream operator.
  std::optional<Output> start_output;

  /// State required for keeping and sending metrics.
  std::chrono::steady_clock::time_point start_time
    = std::chrono::steady_clock::now();
  metrics_receiver_actor metrics_receiver = {};
  operator_metric metrics = {};

  /// Whether this execution node is paused, and when it was.
  std::optional<std::chrono::steady_clock::time_point> paused_at = {};

  /// Whether this execution node is currently waiting for a response.
  bool waiting = {};

  /// A handle to the previous execution node.
  exec_node_actor previous = {};

  /// Whether the previous execution node exited.
  caf::actor_addr prev_addr = nullptr;

  /// The inbound buffer.
  std::deque<Input> inbound_buffer = {};
  uint64_t inbound_buffer_elements = {};

  /// The currently open demand.
  struct demand {
    demand(caf::typed_response_promise<void> rp, exec_node_sink_actor sink,
           uint64_t remaining_elements, uint64_t remaining_batches)
      : rp{std::move(rp)},
        sink{std::move(sink)},
        remaining_elements{remaining_elements},
        remaining_batches{remaining_batches} {
    }

    caf::typed_response_promise<void> rp = {};
    exec_node_sink_actor sink = {};
    uint64_t remaining_elements = {};
    uint64_t remaining_batches = {};
  };
  std::optional<struct demand> demand = {};
  bool issue_demand_inflight = {};

  caf::typed_response_promise<void> start_rp = {};

  /// Exponential backoff for scheduling.
  duration min_backoff = std::chrono::milliseconds{10};
  duration max_backoff = std::chrono::seconds{1};
  double backoff_rate = 2.0;
  duration backoff = duration::zero();
  caf::disposable backoff_disposable = {};
  std::optional<std::chrono::steady_clock::time_point> idle_since = {};

  /// A pointer to te operator control plane passed to this operator during
  /// execution, which acts as an escape hatch to this actor.
  std::unique_ptr<exec_node_control_plane<Input, Output>> ctrl = {};

  /// A weak handle to the node actor.
  detail::weak_handle<node_actor> weak_node = {};

  /// Whether the next run of the internal run loop for this execution node has
  /// already been scheduled.
  bool run_scheduled = {};

  /// Tracks whether the current run has produced an output and consumed an
  /// input, respectively.
  bool consumed_input = false;
  bool produced_output = false;

  ~exec_node_state() noexcept {
    TENZIR_DEBUG("{} {} shut down", *self, op->name());
    // Clean up buffer stats for any remaining items in inbound_buffer.
    if constexpr (not std::is_same_v<Input, std::monostate>) {
      if (buffer_stats and not inbound_buffer.empty()) {
        auto total_bytes = uint64_t{0};
        auto total_events = uint64_t{0};
        for (const auto& item : inbound_buffer) {
          total_bytes += approx_bytes(item);
          if constexpr (std::is_same_v<Input, table_slice>) {
            total_events += item.rows();
          }
        }
        buffer_stats->bytes.fetch_sub(total_bytes, std::memory_order_relaxed);
        if constexpr (std::is_same_v<Input, table_slice>) {
          buffer_stats->events.fetch_sub(total_events,
                                         std::memory_order_relaxed);
        }
      }
    }
    emit_generic_op_metrics();
    instance.reset();
    ctrl.reset();
    if (demand and demand->rp.pending()) {
      demand->rp.deliver();
    }
    if (start_rp.pending()) {
      // TODO: This should probably never happen, as it means that we do not
      // deliver a diagnostic.
      TENZIR_WARN("reached pending `start_rp` in exec node destructor");
      start_rp.deliver(ec::silent);
    }
  }

  auto emit_generic_op_metrics() -> void {
    const auto now = std::chrono::steady_clock::now();
    auto metrics_copy = metrics;
    if (paused_at) {
      metrics_copy.time_paused
        += std::chrono::duration_cast<duration>(now - *paused_at);
    }
    metrics_copy.time_total
      = std::chrono::duration_cast<duration>(now - start_time);
    metrics_copy.time_running
      = metrics_copy.time_total - metrics_copy.time_paused;
    caf::anon_mail(std::move(metrics_copy)).send(metrics_receiver);
  }

  auto start(std::vector<caf::actor> all_previous) -> caf::result<void> {
    TENZIR_DEBUG("{} {} received start request", *self, op->name());
    loop(self, defaults::metrics_interval, [this] {
      auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
      emit_generic_op_metrics();
    });
    if (instance.has_value()) {
      return caf::make_error(ec::logic_error,
                             fmt::format("{} was already started", *self));
    }
    if constexpr (std::is_same_v<Input, std::monostate>) {
      if (not all_previous.empty()) {
        return caf::make_error(ec::logic_error,
                               fmt::format("{} runs a source operator and must "
                                           "not have a previous exec-node",
                                           *self));
      }
    } else {
      // The previous exec-node must be set when the operator is not a source.
      if (all_previous.empty()) {
        return caf::make_error(
          ec::logic_error, fmt::format("{} runs a transformation/sink operator "
                                       "and must have a previous exec-node",
                                       *self));
      }
      previous
        = caf::actor_cast<exec_node_actor>(std::move(all_previous.back()));
      prev_addr = previous->address();
      all_previous.pop_back();
      self->link_to(previous);
    }
    // Instantiate the operator with its input type.
    {
      auto time_scheduled_guard = make_timer_guard(metrics.time_processing);
      auto output_generator = op->instantiate(make_input_adapter(), *ctrl);
      if (not output_generator) {
        TENZIR_DEBUG("{} {} failed to instantiate operator: {}", *self,
                     op->name(), output_generator.error());
        return diagnostic::error(output_generator.error())
          .note("{} {} failed to instantiate operator", *self, op->name())
          .to_error();
      }
      if (not std::holds_alternative<generator<Output>>(*output_generator)) {
        return caf::make_error(
          ec::logic_error, fmt::format("{} expected {}, but got {}", *self,
                                       operator_type_name<Output>(),
                                       operator_type_name(*output_generator)));
      }
      instance = std::get<generator<Output>>(std::move(*output_generator));
      start_output = instance->next();
      if (self->getf(caf::abstract_actor::is_shutting_down_flag)) {
        return {};
      }
      // Emit metrics once to get started.
      emit_generic_op_metrics();
      if (not start_output) {
        TENZIR_TRACE("{} {} finished without yielding", *self, op->name());
        if (previous) {
          // If a transformation or sink operator finishes without yielding,
          // preceding operators effectively dangle because they are set up but
          // never receive any demand. We need to explicitly shut them down to
          // avoid a hang.
          self->send_exit(previous, caf::exit_reason::unreachable);
        }
        self->quit();
        return {};
      }
    }
    if constexpr (detail::are_same_v<std::monostate, Input, Output>) {
      schedule_run(false);
      return {};
    }
    if constexpr (std::is_same_v<Output, std::monostate>) {
      start_rp = self->make_response_promise<void>();
      self->mail(atom::start_v, std::move(all_previous))
        .request(previous, caf::infinite)
        .then(
          [this]() {
            auto time_starting_guard
              = make_timer_guard(metrics.time_scheduled, metrics.time_starting);
            TENZIR_TRACE("{} {} schedules run after successful startup of all "
                         "operators",
                         *self, op->name());
            schedule_run(false);
            start_rp.deliver();
          },
          [this](const caf::error& error) {
            auto time_starting_guard
              = make_timer_guard(metrics.time_scheduled, metrics.time_starting);
            TENZIR_DEBUG("{} {} forwards error during startup: {}", *self,
                         op->name(), error);
            start_rp.deliver(error);
          });
      return start_rp;
    }
    if constexpr (not std::is_same_v<Input, std::monostate>) {
      TENZIR_DEBUG("{} {} delegates start to {}", *self, op->name(), previous);
      return self->mail(atom::start_v, std::move(all_previous))
        .delegate(previous);
    }
    return {};
  }

  auto pause() -> caf::result<void> {
    if (paused_at) {
      return {};
    }
    TENZIR_DEBUG("{} {} pauses execution", *self, op->name());
    paused_at = std::chrono::steady_clock::now();
    return {};
  }

  auto resume() -> caf::result<void> {
    if (not paused_at) {
      return {};
    }
    TENZIR_DEBUG("{} {} resumes execution", *self, op->name());
    metrics.time_paused += std::chrono::duration_cast<duration>(
      std::chrono::steady_clock::now() - *paused_at);
    paused_at.reset();
    schedule_run(false);
    return {};
  }

  auto has_active_demand() const -> bool {
    // We pretend that the sink always has demand.
    if constexpr (std::is_same_v<Output, std::monostate>) {
      return true;
    }
    return demand and demand->remaining_batches > 0
           and demand->remaining_elements > 0;
  }

  auto advance_generator() -> void {
    auto time_processing_guard = make_timer_guard(metrics.time_processing);
    TENZIR_ASSERT(instance);
    if constexpr (std::is_same_v<Output, std::monostate>) {
      // We never issue demand to the sink, so we cannot be at the end of the
      // generator here.
      TENZIR_TRACE("{} {} processes", *self, op->name());
      auto output = instance->next();
      if (not output) {
        TENZIR_DEBUG("{} {} completes processing", *self, op->name());
        if (demand and demand->rp.pending()) {
          demand->rp.deliver();
        }
        self->quit();
        return;
      }
      if (self->getf(caf::abstract_actor::is_shutting_down_flag)) {
        return;
      }
      return;
    } else {
      if (not has_active_demand()) {
        return;
      }
      TENZIR_TRACE("{} {} processes", *self, op->name());
      // We just checked that we are not exhausted.
      auto output = instance->next();
      if (not output) {
        TENZIR_DEBUG("{} {} completes processing", *self, op->name());
        if (demand and demand->rp.pending()) {
          demand->rp.deliver();
        }
        self->quit();
        return;
      }
      const auto output_size = size(*output);
      if (self->getf(caf::abstract_actor::is_shutting_down_flag)) {
        return;
      }
      if (output_size == 0) {
        if (not idle_since) {
          idle_since = std::chrono::steady_clock::now();
        }
        return;
      }
      send_output(std::move(*output));
    }
  }

  void send_output(Output output)
    requires(not std::same_as<Output, std::monostate>)
  {
    auto output_size = size(output);
    TENZIR_ASSERT(output_size > 0);
    idle_since.reset();
    produced_output = true;
    metrics.outbound_measurement.num_elements += output_size;
    metrics.outbound_measurement.num_batches += 1;
    metrics.outbound_measurement.num_approx_bytes += approx_bytes(output);
    TENZIR_TRACE("{} {} produced and pushes {} elements", *self, op->name(),
                 output_size);
    // We already checked that there is active demand.
    demand->remaining_batches -= 1;
    demand->remaining_elements
      -= std::min(output_size, demand->remaining_elements);
    // We have to remember whether this is the push that finishes the demand
    // because there can be multiple pushes in flight in parallel.
    auto finished = not has_active_demand();
    self->mail(atom::push_v, std::move(output))
      .request(demand->sink, caf::infinite)
      .then(
        [this, output_size, finished]() {
          auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
          TENZIR_TRACE("{} {} pushed {} elements", *self, op->name(),
                       output_size);
          TENZIR_ASSERT(demand);
          if (finished) {
            TENZIR_ASSERT(demand->rp.pending());
            demand->rp.deliver();
            demand.reset();
          }
          schedule_run(false);
        },
        [this, output_size](const caf::error& err) {
          TENZIR_DEBUG("{} {} failed to push {} elements", *self, op->name(),
                       output_size);
          auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
          if (err == caf::sec::request_receiver_down) {
            if (demand and demand->rp.pending()) {
              demand->rp.deliver();
            }
            self->quit();
            return;
          }
          diagnostic::error(err)
            .note("{} {} failed to push to next execution node", *self,
                  op->name())
            .emit(ctrl->diagnostics());
        });
  }

  auto make_input_adapter() -> std::monostate
    requires std::is_same_v<Input, std::monostate>
  {
    return {};
  }

  auto make_input_adapter() -> generator<Input>
    requires(not std::is_same_v<Input, std::monostate>)
  {
    while (previous or not inbound_buffer.empty()) {
      if (inbound_buffer.empty()) {
        co_yield {};
        continue;
      }
      consumed_input = true;
      auto input = std::move(inbound_buffer.front());
      inbound_buffer.pop_front();
      const auto input_size = size(input);
      inbound_buffer_elements -= input_size;
      // Update buffer stats for metrics
      if (buffer_stats) {
        buffer_stats->bytes.fetch_sub(approx_bytes(input),
                                      std::memory_order_relaxed);
        if constexpr (std::is_same_v<Input, table_slice>) {
          buffer_stats->events.fetch_sub(input.rows(),
                                         std::memory_order_relaxed);
        }
      }
      TENZIR_TRACE("{} {} uses {} elements", *self, op->name(), input_size);
      co_yield std::move(input);
    }
    TENZIR_DEBUG("{} {} reached end of input", *self, op->name());
  }

  auto schedule_run(bool use_backoff) -> void {
    // Edge case: If a run with backoff is currently scheduled, but we now want
    // a run without backoff, we can replace the scheduled run with a new one.
    if (not backoff_disposable.disposed() and not use_backoff) {
      backoff_disposable.dispose();
      run_scheduled = false;
    }
    // Check whether we're already scheduled to run, or are no longer allowed to
    // rum.
    if (run_scheduled) {
      return;
    }
    const auto remaining_until_idle
      = idle_since
          ? op->idle_after() - (std::chrono::steady_clock::now() - *idle_since)
          : duration::zero();
    const auto is_idle = remaining_until_idle <= duration::zero();
    if (not use_backoff or not is_idle) {
      backoff = duration::zero();
    } else if (backoff == duration::zero()) {
      backoff = min_backoff;
    } else {
      backoff
        = std::min(std::chrono::duration_cast<duration>(backoff_rate * backoff),
                   max_backoff);
    }
    TENZIR_TRACE("{} {} schedules run with a delay of {}", *self, op->name(),
                 data{backoff});
    run_scheduled = true;
    if (use_backoff) {
      backoff_disposable = self->run_delayed_weak(backoff, [this] {
        run_scheduled = false;
        run();
      });
      return;
    }
    self->schedule_fn([this] {
      run_scheduled = false;
      run();
    });
  }

  auto issue_demand() -> void {
    if (not previous or issue_demand_inflight) {
      return;
    }
    if (inbound_buffer_elements + min_elements > max_elements) {
      return;
    }
    auto elements = max_elements - inbound_buffer_elements;
    if (inbound_buffer.size() >= max_batches) {
      return;
    }
    auto batches = max_batches - inbound_buffer.size();
    TENZIR_TRACE("{} {} issues demand for up to {} elements or {} batches",
                 *self, op->name(), elements, batches);
    issue_demand_inflight = true;
    self
      ->mail(atom::pull_v, static_cast<exec_node_sink_actor>(self), elements,
             batches)
      .request(previous, caf::infinite)
      .then(
        [this] {
          auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
          TENZIR_TRACE("{} {} had its demand fulfilled", *self, op->name());
          TENZIR_ASSERT(issue_demand_inflight);
          issue_demand_inflight = false;
          schedule_run(false);
        },
        [this](const caf::error& err) {
          auto time_scheduled_guard = make_timer_guard(metrics.time_scheduled);
          TENZIR_DEBUG("{} {} failed to get its demand fulfilled: {}", *self,
                       op->name(), err);
          TENZIR_ASSERT(issue_demand_inflight);
          issue_demand_inflight = false;
          if (err.valid() and err != caf::sec::request_receiver_down
              and err != caf::exit_reason::remote_link_unreachable) {
            diagnostic::error(err)
              .note("{} {} failed to pull from previous execution node", *self,
                    op->name())
              .emit(ctrl->diagnostics());
          } else {
            // TODO: We seem to assume that this error is recoverable, but I'm
            // not sure whether that makes sense here.
            schedule_run(true);
          }
        });
  }

  auto run() -> void {
    if (waiting or paused_at or not instance) {
      return;
    }
    TENZIR_TRACE("{} {} enters run loop", *self, op->name());
    // If the inbound buffer is below its capacity, we must issue demand
    // upstream.
    issue_demand();
    // Advance the operator's generator.
    advance_generator();
    // We are only allowed to run the generator again if that is possible, it's
    // not waiting and there is active demand.
    auto may_continue
      = not instance->exhausted() and not waiting and has_active_demand();
    if (may_continue) {
      // If we may continue, we have to decide whether we are in a situation
      // where there definitely is work to be done or where we are just polling
      // the operator.
      auto can_definitely_do_work =
        // If we have unconsumed input, there is definitely
        // something for the operator to do.
        not inbound_buffer.empty()
        // When the previous execution node exits (or we are a source in the
        // first place), then we want to continue directly because we want to
        // communicate to the operator that it's done. If it's still doing
        // polling as part of it's exit routine, then this is incorrect, but we
        // are okay with that here.
        or not previous
        // If we just got output we assume that there is more. This does not
        // work for simple 1:1 transformations, but is needed for things that
        // produce multiple batches or independent on input.
        or produced_output;
      if (can_definitely_do_work) {
        schedule_run(false);
      } else {
        schedule_run(true);
      }
    } else {
      TENZIR_TRACE("{} {} idles", *self, op->name());
    }
    metrics.num_runs += 1;
    metrics.num_runs_processing += consumed_input or produced_output ? 1 : 0;
    metrics.num_runs_processing_input += consumed_input ? 1 : 0;
    metrics.num_runs_processing_output += produced_output ? 1 : 0;
    consumed_input = false;
    produced_output = false;
  }

  auto pull(exec_node_sink_actor sink, uint64_t elements, uint64_t batches)
    -> caf::result<void>
    requires(not std::is_same_v<Output, std::monostate>)
  {
    TENZIR_TRACE("{} {} received downstream demand for {} elements or {} "
                 "batches",
                 *self, op->name(), elements, batches);
    // We only keep one demand in flight at a time. Our implementation guarantees
    // that the final push is answered before we get the next pull message.
    TENZIR_ASSERT(not demand);
    TENZIR_ASSERT(sink);
    TENZIR_ASSERT(elements > 0);
    TENZIR_ASSERT(batches > 0);
    auto& pr = demand.emplace(self->make_response_promise<void>(),
                              std::move(sink), elements, batches);
    if (start_output) {
      if (size(*start_output) > 0) {
        send_output(std::move(*start_output));
      }
      start_output.reset();
    }
    schedule_run(false);
    return pr.rp;
  }

  auto push(Input input) -> caf::result<void>
    requires(not std::is_same_v<Input, std::monostate>)
  {
    if (metrics.time_to_first_input == duration::zero()) {
      metrics.time_to_first_input
        = std::chrono::steady_clock::now() - start_time;
    }
    const auto input_size = size(input);
    TENZIR_ASSERT(input_size > 0);
    TENZIR_TRACE("{} {} received {} elements from upstream", *self, op->name(),
                 input_size);
    // The protocol ensures that the maximum number of batches is not exceeded.
    // This does not apply to the element count since we do not want to perform
    // slicing to maintain the same invariant there.
    TENZIR_ASSERT(inbound_buffer.size() <= max_batches);
    metrics.inbound_measurement.num_elements += input_size;
    metrics.inbound_measurement.num_batches += 1;
    metrics.inbound_measurement.num_approx_bytes += approx_bytes(input);
    // Update buffer stats for metrics
    if (buffer_stats) {
      buffer_stats->bytes.fetch_add(approx_bytes(input),
                                    std::memory_order_relaxed);
      if constexpr (std::is_same_v<Input, table_slice>) {
        buffer_stats->events.fetch_add(input.rows(), std::memory_order_relaxed);
      }
    }
    inbound_buffer_elements += input_size;
    inbound_buffer.push_back(std::move(input));
    schedule_run(false);
    return {};
  }

  void on_error(caf::error error) {
    if (start_rp.pending()) {
      start_rp.deliver(std::move(error));
      self->quit(ec::silent);
      return;
    }
    self->quit(std::move(error));
  }

  void handle_exit_msg(const caf::exit_msg& msg) {
    if (not instance) {
      if (msg.reason.valid()) {
        self->quit(msg.reason);
      }
    }
    if constexpr (std::is_same_v<Input, std::monostate>) {
      TENZIR_DEBUG("{} {} got exit message from the next execution node or "
                   "its executor with address {}: {}",
                   *self, op->name(), msg.source, msg.reason);
      on_error(msg.reason);
      return;
    } else {
      if (not previous and msg.source == prev_addr) {
        // Ignore duplicate exit message from the previous node.
        // For some reason, we can get multiple exit messages from the previous
        // exec node. This can cause the current operator to ungracefully quit.
        //
        // We ignore this because we should only get exit messages from the exec
        // nodes from the `linked` state.
        return;
      }
      // We got an exit message, which can mean one of four things:
      // 1. The pipeline manager quit.
      // 2. The next operator quit.
      // 3. The previous operator quit gracefully.
      // 4. The previous operator quit ungracefully.
      // In cases (1-3) we need to shut down this operator unconditionally.
      // For (4) we we need to treat the previous operator as offline.
      if (not previous or msg.source != prev_addr) {
        TENZIR_DEBUG("{} {} got exit message from the next execution node or "
                     "its executor with address {}: {}",
                     *self, op->name(), msg.source, msg.reason);
        on_error(msg.reason);
        return;
      }
      TENZIR_DEBUG("{} {} got exit message from previous execution node with "
                   "address {}: {}",
                   *self, op->name(), msg.source, msg.reason);
      if (msg.reason.valid() and msg.reason != caf::exit_reason::unreachable) {
        on_error(msg.reason);
        return;
      }
      previous = nullptr;
      schedule_run(false);
    }
  }
};

} // namespace

auto spawn_exec_node(caf::scheduled_actor* self, operator_ptr op,
                     operator_type input_type, std::string definition,
                     node_actor node,
                     receiver_actor<diagnostic> diagnostics_handler,
                     metrics_receiver_actor metrics_receiver, int index,
                     bool has_terminal, bool is_hidden, uuid run_id,
                     std::string pipeline_id)
  -> caf::expected<std::pair<exec_node_actor, operator_type>> {
  TENZIR_ASSERT(self);
  TENZIR_ASSERT(op != nullptr);
  TENZIR_ASSERT(node != nullptr or op->location() != operator_location::remote);
  TENZIR_ASSERT(diagnostics_handler != nullptr);
  TENZIR_ASSERT(metrics_receiver != nullptr);
  auto output_type = op->infer_type(input_type);
  if (not output_type) {
    return caf::make_error(ec::logic_error,
                           fmt::format("failed to spawn exec-node for '{}': {}",
                                       op->name(), output_type.error()));
  }
  auto f = [&]<caf::spawn_options SpawnOptions>() {
    return [&]<class Input, class Output>(tag<Input>,
                                          tag<Output>) -> exec_node_actor {
      using input_type
        = std::conditional_t<std::is_void_v<Input>, std::monostate, Input>;
      using output_type
        = std::conditional_t<std::is_void_v<Output>, std::monostate, Output>;
      auto result = self->spawn<SpawnOptions>(
        caf::actor_from_state<exec_node_state<input_type, output_type>>,
        std::move(op), std::move(definition), std::move(node),
        std::move(diagnostics_handler), std::move(metrics_receiver), index,
        has_terminal, is_hidden, run_id, std::move(pipeline_id));
      return result;
    };
  };
  return std::pair{
    op->detached() ? std::visit(f.template operator()<caf::detached>(),
                                input_type, *output_type)
                   : std::visit(f.template operator()<caf::no_spawn_options>(),
                                input_type, *output_type),
    *output_type,
  };
};

} // namespace tenzir
