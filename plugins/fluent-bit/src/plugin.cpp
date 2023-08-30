//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>

#include <arrow/record_batch.h>

#include <cstring>

#include <fluent-bit/fluent-bit-minimal.h>

using namespace std::chrono_literals;
using namespace tenzir::si_literals;

namespace tenzir::plugins::fluentbit {

namespace {

/* Shared state between Tenzir and Fluent Bit.
 * WARNING: keep in sync with the respective code bases.
 */
struct shared_state {
  char* buf;
  int buf_len;
  size_t buf_size;
  pthread_mutex_t lock;
};

struct operator_args {
  std::vector<std::string> args;

  template <class Inspector>
  friend auto inspect(Inspector& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("args", x.args));
  }
};

/// A RAII-style wrapper around the Fluent Bit engine.
class engine {
public:
  static auto make_source(std::string input) -> std::unique_ptr<engine> {
    auto buffer_size = 16_Mi;
    auto result = make_engine(buffer_size);
    if (!result)
      return nullptr;
    auto* ctx = make_fluentbit_context();
    if (not ctx)
      return nullptr;
    result->ctx_ = ctx;
    // Set the desired input plugin.
    auto ret = flb_input(ctx, input.c_str(), nullptr);
    if (ret < 0) {
      TENZIR_ERROR("failed to setup {} input plugin ({})", input, ret);
      return nullptr;
    }
    // Set ourselves as output plugin.
    ret = flb_output(ctx, "tenzir", nullptr);
    if (ret < 0) {
      TENZIR_ERROR("failed to setup tenzir output plugin ({})", ret);
      return nullptr;
    }
    ret = flb_start(ctx);
    if (ret < 0) {
      TENZIR_ERROR("failed to start engine ({})", ret);
      return nullptr;
    }
    return result;
  }

  static auto make_sink(std::string output) -> std::unique_ptr<engine> {
    auto buffer_size = 16_Mi;
    auto result = make_engine(buffer_size);
    if (!result)
      return nullptr;
    auto* ctx = make_fluentbit_context();
    if (not ctx)
      return nullptr;
    result->ctx_ = ctx;
    // Set ourselves as input plugin.
    auto ret = flb_input(ctx, "tenzir", result->state());
    if (ret < 0) {
      TENZIR_ERROR("failed to setup tenzir input plugin ({})", ret);
      return nullptr;
    }
    // Set the desired output plugin.
    ret = flb_output(ctx, output.c_str(), nullptr);
    if (ret < 0) {
      TENZIR_ERROR("failed to setup {} output plugin ({})", output, ret);
      return nullptr;
    }
    ret = flb_start(ctx);
    if (ret < 0) {
      TENZIR_ERROR("failed to start engine ({})", ret);
      return nullptr;
    }
    return result;
  }

  ~engine() {
    if (ctx_ != nullptr) {
      // We must release all Fluent Bit resources prior to destroying the shared
      // state.
      auto ret = flb_stop(ctx_);
      if (ret != 0)
        TENZIR_ERROR("failed to stop engine ({})", ret);
      // FIXME: destroying the library context currently yields a segfault.
      // flb_destroy(ctx_);
    }
    auto ret = pthread_mutex_destroy(&state_.lock);
    if (ret != 0)
      TENZIR_ERROR("failed to destroy mutex: {}", std::strerror(ret));
  }

  // The engine is a move-only handle type.
  engine(engine&&) = default;
  auto operator=(engine&&) -> engine& = default;
  engine(const engine&) = delete;
  auto operator=(const engine&) -> engine& = delete;

  auto state() -> shared_state* {
    return &state_;
  }

private:
  static auto make_fluentbit_context() -> flb_ctx_t* {
    auto* ctx = flb_create();
    if (ctx == nullptr)
      return nullptr;
    // TODO: make these parameters configurable.
    auto result = flb_service_set(ctx, "flush", "1", "grace", "5", nullptr);
    if (result != 0)
      return nullptr;
    return ctx;
  }

  static auto make_engine(size_t buffer_size) -> std::unique_ptr<engine> {
    return std::unique_ptr<engine>(new engine{buffer_size});
  }

  explicit engine(size_t buffer_size) {
    TENZIR_ASSERT(buffer_size > 0);
    buffer_.resize(buffer_size);
    pthread_mutex_init(&state_.lock, nullptr);
    state_ = {
      .buf = buffer_.data(),
      .buf_len = 0,
      .buf_size = buffer_.size(),
    };
  }

  flb_ctx_t* ctx_{nullptr};
  shared_state state_{};
  std::vector<char> buffer_;
};

class fluent_bit_operator final : public crtp_operator<fluent_bit_operator> {
public:
  fluent_bit_operator() = default;

  explicit fluent_bit_operator(operator_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    TENZIR_ASSERT(!args_.args.empty());
    auto engine = engine::make_source(args_.args[0]);
    if (not engine) {
      diagnostic::error("failed to create Fluent Bit engine")
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Keep the executor pumping.
    while (true) {
      auto* state = engine->state();
      pthread_mutex_lock(&state->lock);
      co_yield {}; // TODO
      pthread_mutex_unlock(&state->lock);
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    TENZIR_ASSERT(!args_.args.empty());
    auto engine = engine::make_sink(args_.args[0]);
    if (not engine) {
      diagnostic::error("failed to create Fluent Bit engine")
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto printer = tenzir::json_printer{{
      .oneline = true,
    }};
    auto json_buffer = std::vector<char>{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Print table slice as JSON.
      auto resolved_slice = resolve_enumerations(slice);
      auto array
        = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
      auto it = std::back_inserter(json_buffer);
      for (const auto& row :
           values(caf::get<record_type>(resolved_slice.schema()), *array)) {
        TENZIR_ASSERT_CHEAP(row);
        const auto ok = printer.print(it, *row);
        TENZIR_ASSERT_CHEAP(ok);
        it = fmt::format_to(it, "\n");
      }
      json_buffer.pop_back(); // Remove trailing '\n'.
      // Mutate shared state: copy generated JSON into buffer.
      auto* state = engine->state();
      pthread_mutex_lock(&state->lock);
      if (json_buffer.size() > state->buf_size - state->buf_len - 1)
        // FIXME: resize buffer instead of dying
        die("not enough buffer capa");
      std::memcpy(state->buf + state->buf_len, json_buffer.data(),
                  json_buffer.size());
      state->buf_len += json_buffer.size();
      state->buf[state->buf_len] = '\0';
      pthread_mutex_unlock(&state->lock);
      co_yield {};
    }
    // FIXME: we're keeping things alive just for testing.
    while (true) {
      co_yield {};
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      TENZIR_WARN("yielding...");
    }
  }

  auto name() const -> std::string override {
    return "fluentbit";
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, fluent_bit_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  operator_args args_;
};

class plugin final : public operator_plugin<fluent_bit_operator> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto args = operator_args{};
    // auto parser = argument_parser{
    //   name(),
    //   fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    // parser.parse(p);
    while (true) {
      auto arg = p.accept_shell_arg();
      if (arg == std::nullopt)
        break;
      args.args.push_back(std::move(arg->inner));
    }
    if (args.args.empty())
      diagnostic::error("missing fluentbit arguments").throw_();
    return std::make_unique<fluent_bit_operator>(std::move(args));
  }

  auto name() const -> std::string override {
    return "fluentbit";
  }
};

} // namespace

} // namespace tenzir::plugins::fluentbit

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::plugin)
