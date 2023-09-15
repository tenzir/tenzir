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
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/record_batch.h>

#include <cstring>
// #include <msgpack.h>

#include <fluent-bit/fluent-bit-minimal.h>

namespace tenzir::plugins::fluentbit {

namespace {

// Shared state between this operator and the Fluent Bit plugins.
// WARNING: keep in sync with the respective code bases.
struct shared_state {
  char* buf;
  int len;
  pthread_mutex_t lock;
};

/// A map of key-value pairs of Fluent Bit plugin configuration options.
using property_map = std::map<std::string, std::string>;

/// The arguments passed to the operator.
struct operator_args {
  std::string plugin;                           ///< Fluent Bit plugin name.
  std::chrono::milliseconds poll_interval{250}; ///< Engine poll interval.
  property_map service_properties;              ///< The global service options.
  property_map args;                            ///< The plugin arguments.

  template <class Inspector>
  friend auto inspect(Inspector& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("plugin", x.plugin),
              f.field("poll_interval", x.poll_interval),
              f.field("service_properties", x.service_properties),
              f.field("args", x.args));
  }
};

/// A RAII-style wrapper around the Fluent Bit engine.
class engine {
  /// Callback that the Fluent Bit `lib` output invokes per record. We use when
  /// the engine acts as source. Since we don't want to do any memory
  /// management within Fluent Bit, we just make a copy of the data into our
  /// shared buffer that we then process later with the source operator.
  static auto handle_lib_output(void* record, size_t size, void* data) -> int {
    auto str = std::string_view{reinterpret_cast<char*>(record), size};
    auto* self = reinterpret_cast<engine*>(data);
    self->append(str);
    self->append(std::string{'\n'}); // ensures valid JSONL
    flb_lib_free(record);
    return 0;
  }

public:
  /// Constructs a Fluent Bit engine for use as "source" in a pipeline.
  static auto
  make_source(const operator_args& args, const record& plugin_config)
    -> std::unique_ptr<engine> {
    auto result
      = make_engine(plugin_config, args.poll_interval, args.service_properties);
    if (not result)
      return nullptr;
    if (not result->input(args.plugin, args.args))
      return nullptr;
    auto callback = flb_lib_out_cb{
      .cb = handle_lib_output,
      .data = result.get(),
    };
    // There are two options for the `lib` output:
    // - format: "msgpack" or "json"
    // - max_records: integer representing the maximum number of records to
    //   process per single flush call.
    if (not result->output("lib", {{"format", "json"}}, &callback))
      return nullptr;
    if (not result->start())
      return nullptr;
    return result;
  }

  /// Constructs a Fluent Bit engine for use as "sink" in a pipeline.
  static auto make_sink(const operator_args& args, const record& plugin_config)
    -> std::unique_ptr<engine> {
    auto result
      = make_engine(plugin_config, args.poll_interval, args.service_properties);
    if (not result)
      return nullptr;
    if (not result->input("lib"))
      return nullptr;
    if (not result->output(args.plugin, args.args))
      return nullptr;
    if (not result->start())
      return nullptr;
    return result;
  }

  ~engine() {
    if (ctx_ != nullptr) {
      stop();
      flb_destroy(ctx_);
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

  /// Copies data into the shared buffer with the Tenzir Fluent Bit plugin.
  /// @note This function is thread-safe.
  void append(const auto& buffer) {
    pthread_mutex_lock(&state_.lock);
    TENZIR_ASSERT(state_.len >= 0);
    TENZIR_ASSERT(static_cast<size_t>(state_.len) == buffer_.size());
    // When we enter here, Fluent Bit may have futzed with our buffer and
    // partially processed it. So we must adjust our own buffer accordingly.
    // Fluent Bit assuems that the last writeable byte is at position `len` and
    // may write a NUL byte at `len + 1` to produce a NUL-terminated C-string.
    buffer_.resize(detail::narrow_cast<size_t>(state_.len));
    // Now we're ready to write new data.
    buffer_.insert(buffer_.end(), buffer.begin(), buffer.end());
    // Finally, we update the shared state to allow Fluent Bit to wield freely.
    // Fluent Bit expects that it can freely within the buffer bounds.
    state_.buf = buffer_.data();
    state_.len = detail::narrow_cast<int>(buffer_.size());
    pthread_mutex_unlock(&state_.lock);
  }

  /// Tries to consume the shared buffer with a function.
  /// @note This function is thread-safe.
  auto try_consume(auto f) -> bool {
    if (pthread_mutex_trylock(&state_.lock) != 0)
      return false;
    auto guard = caf::detail::make_scope_guard([&] {
      pthread_mutex_unlock(&state_.lock);
    });
    if (state_.len == 0)
      return false;
    auto str = std::string_view{state_.buf, static_cast<size_t>(state_.len)};
    if (f(str)) {
      buffer_.clear();
      state_.buf = nullptr;
      state_.len = 0;
    }
    return true;
  }

  /// Provides an upper bound on sleep time before stopping the engine. This is
  /// important when using the engine as sink, because pushing data into Fluent
  /// Bit is not preventing a teardown, i.e., pushed data may not be processed
  /// at all. Since there are no delivery guarantees, the best we can do is
  /// wait by sleeping.
  void max_wait_before_stop(std::chrono::milliseconds wait_time) {
    num_stop_polls_ = wait_time / poll_interval_;
  }

  /// Checks whether the Fluent Bit engine is still running.
  auto running() -> bool {
    TENZIR_ASSERT_CHEAP(ctx_ != nullptr);
    return ctx_->status == FLB_LIB_OK;
  }

  /// Pushes data into Fluent Bit.
  auto push(std::string_view data) -> bool {
    TENZIR_ASSERT_CHEAP(ctx_ != nullptr);
    TENZIR_ASSERT_CHEAP(ffd_ >= 0);
    return flb_lib_push(ctx_, ffd_, data.data(), data.size()) != 0;
  }

private:
  static auto make_engine(const record& global_properties,
                          std::chrono::milliseconds poll_interval,
                          const property_map& local_properties)
    -> std::unique_ptr<engine> {
    auto* ctx = flb_create();
    if (ctx == nullptr)
      return nullptr;
    // Start with a less noisy log level.
    if (flb_service_set(ctx, "log_level", "error", nullptr) != 0) {
      TENZIR_ERROR("failed to adjust default log_level");
      return nullptr;
    }
    for (const auto& [key, value] : global_properties) {
      auto str_value = to_string(value);
      TENZIR_DEBUG("setting global service option: {}={}", key, str_value);
      if (flb_service_set(ctx, key.c_str(), str_value.c_str(), nullptr) != 0) {
        TENZIR_ERROR("failed to set global service option: {}={}", key,
                     str_value);
        return nullptr;
      }
    }
    for (const auto& [key, value] : local_properties) {
      TENZIR_DEBUG("setting local service option: {}={}", key, value);
      if (flb_service_set(ctx, key.c_str(), value.c_str(), nullptr) != 0) {
        TENZIR_ERROR("failed to set local service option: {}={}", key, value);
        return nullptr;
      }
    }
    return std::unique_ptr<engine>(new engine{ctx, poll_interval});
  }

  explicit engine(flb_ctx_t* ctx, std::chrono::milliseconds poll_interval)
    : ctx_{ctx}, poll_interval_{poll_interval} {
    TENZIR_ASSERT(ctx != nullptr);
    pthread_mutex_init(&state_.lock, nullptr);
  }

  auto input(const std::string& plugin, const property_map& properties = {})
    -> bool {
    ffd_ = flb_input(ctx_, plugin.c_str(), &state_);
    if (ffd_ < 0) {
      TENZIR_ERROR("failed to setup {} input plugin ({})", plugin, ffd_);
      return false;
    }
    // Apply user-provided plugin properties.
    for (const auto& [key, value] : properties) {
      TENZIR_DEBUG("setting {} plugin option: {}={}", plugin, key, value);
      if (flb_input_set(ctx_, ffd_, key.c_str(), value.c_str(), nullptr) != 0) {
        TENZIR_ERROR("failed to set {} plugin option: {}={}", plugin, key,
                     value);
        return false;
      }
    }
    return true;
  }

  auto output(const std::string& plugin, const property_map& properties = {},
              struct flb_lib_out_cb* callback = nullptr) -> bool {
    auto ffd = flb_output(ctx_, plugin.c_str(), callback);
    if (ffd < 0) {
      TENZIR_ERROR("failed to setup {} output plugin ({})", plugin, ffd);
      return false;
    }
    // Apply user-provided plugin properties.
    for (const auto& [key, value] : properties) {
      TENZIR_DEBUG("setting {} plugin option: {}={}", plugin, key, value);
      if (flb_output_set(ctx_, ffd, key.c_str(), value.c_str(), nullptr) != 0) {
        TENZIR_ERROR("failed to set {} plugin option: {}={}", plugin, key,
                     value);
        return false;
      }
    }
    return true;
  }

  auto start() -> bool {
    TENZIR_ASSERT_CHEAP(ctx_ != nullptr);
    TENZIR_DEBUG("starting Fluent Bit engine");
    auto ret = flb_start(ctx_);
    if (ret == 0)
      return true;
    TENZIR_ERROR("failed to start engine ({})", ret);
    return false;
  }

  /// Stops the engine.
  auto stop() -> bool {
    TENZIR_ASSERT_CHEAP(ctx_ != nullptr);
    TENZIR_DEBUG("stopping Fluent Bit engine");
    for (size_t i = 0; ctx_->status == FLB_LIB_OK && i < num_stop_polls_; ++i) {
      TENZIR_DEBUG("sleeping while Fluent Bit context is okay");
      std::this_thread::sleep_for(poll_interval_);
    }
    auto ret = flb_stop(ctx_);
    if (ret == 0)
      return true;
    TENZIR_ERROR("failed to stop engine ({})", ret);
    return false;
  }

  flb_ctx_t* ctx_{nullptr}; ///< Fluent Bit context
  int ffd_{-1};             ///< Fluent Bit handle for pushing data
  std::chrono::milliseconds poll_interval_{}; ///< How fast we check FB
  size_t num_stop_polls_{0}; ///< Number of polls in the destructor
  shared_state state_{.buf = nullptr, .len = 0}; ///< Shared state with FB
  std::string buffer_{};                         ///< Buffer for shred state
};

class fluent_bit_operator final : public crtp_operator<fluent_bit_operator> {
public:
  fluent_bit_operator() = default;

  fluent_bit_operator(operator_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto engine = engine::make_source(args_, config_);
    if (not engine) {
      diagnostic::error("failed to create Fluent Bit engine")
        .emit(ctrl.diagnostics());
      co_return;
    }
    while (engine->running()) {
      auto result = table_slice{};
      auto parse_fluentbit = [&result](std::string_view data) {
        // TODO: create table slices from JSON. The alternative would be
        // switching the format to MsgPack, but this is perhaps v2 when things
        // are too slow.
        //
        // Speaking with Dominik, we could use simdjson directly here to get the
        // second array element of the Fluent Bit message [timestamp, {..}],
        // using something like on_demand::dom::parse. Once we're ready to
        // extract the contained object, we should go to Jannis as he's
        // currently reworking the JSON parser. What we need is a library
        // utility that bakes us a table slice given a JSON object and builder.
        result = {};
        return true;
      };
      // Poll the engine and process data that Fluent Bit already handed over.
      if (not engine->try_consume(parse_fluentbit))
        std::this_thread::sleep_for(args_.poll_interval);
      co_yield result;
    }
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto engine = engine::make_sink(args_, config_);
    if (not engine) {
      diagnostic::error("failed to create Fluent Bit engine")
        .emit(ctrl.diagnostics());
      co_return;
    }
    engine->max_wait_before_stop(std::chrono::seconds(1));
    auto event = std::string{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Print table slice as JSON.
      auto resolved_slice = resolve_enumerations(slice);
      auto array
        = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
      auto it = std::back_inserter(event);
      for (const auto& row :
           values(caf::get<record_type>(resolved_slice.schema()), *array)) {
        TENZIR_ASSERT_CHEAP(row);
        auto printer = json_printer{{
          .oneline = true,
        }};
        const auto ok = printer.print(it, *row);
        TENZIR_ASSERT_CHEAP(ok);
        // Wrap JSON object in the 2-element JSON array that Fluent Bit expects.
        auto message = fmt::format("[{}, {}]", flb_time_now(), event);
        if (not engine->push(message))
          TENZIR_ERROR("failed to push data into Fluent Bit engine");
        event.clear();
      }
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "fluent-bit";
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
  record config_;
};

class plugin final : public operator_plugin<fluent_bit_operator> {
public:
  auto initialize(const record& config, const record& /* global_config */)
    -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto args = operator_args{};
    auto arg = p.accept_shell_arg();
    if (arg == std::nullopt)
      diagnostic::error("missing fluent-bit plugin").throw_();
    auto have_options = false;
    if (arg->inner == "-X" || arg->inner == "--set") {
      have_options = true;
      arg = p.accept_shell_arg();
      if (arg == std::nullopt)
        diagnostic::error("-X|--set requires values").throw_();
      std::vector<std::pair<std::string, std::string>> kvps;
      if (not parsers::kvp_list(arg->inner, kvps))
        diagnostic::error("invalid list of key=value pairs")
          .primary(arg->source)
          .throw_();
      for (auto& [key, value] : kvps)
        args.service_properties.emplace(std::move(key), std::move(value));
    }
    // Parse the remainder: <plugin> [<key=value>...]
    if (have_options) {
      arg = p.accept_shell_arg();
      if (arg == std::nullopt)
        diagnostic::error("missing fluent-bit plugin").throw_();
    }
    args.plugin = std::move(arg->inner);
    while (true) {
      arg = p.accept_shell_arg();
      if (arg == std::nullopt)
        break;
      // Try parsing as key-value pair
      auto kvp = detail::split(arg->inner, "=");
      if (kvp.size() != 2)
        diagnostic::error("invalid key-value pair: {}", arg->inner)
          .hint("{} operator arguments have the form key=value", name())
          .throw_();
      args.args.emplace(kvp[0], kvp[1]);
    }
    return std::make_unique<fluent_bit_operator>(std::move(args), config_);
  }

  auto name() const -> std::string override {
    return "fluent-bit";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::fluentbit

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::plugin)
