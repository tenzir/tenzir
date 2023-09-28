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
#include <tenzir/series_builder.hpp>

#include <arrow/record_batch.h>

#include <cstring>

#include <fluent-bit/fluent-bit-minimal.h>

namespace tenzir::plugins::fluentbit {

// We're using the 'lib' Fluent Bit plugin for both input and output. We could
// upgrade our implementation to switch from JSON data exchange to MsgPack. For
// the 'lib' output plugin, we could already consume MsgPack. For the 'lib'
// input, we got green light from Eduardo that he would accept patch to also
// support MsgPack, as there's currently only JSON support. The proposed API
// changes was as follows:
//
//     in_ffd = flb_input(ctx, "lib", NULL);
//     // New: allow switching input format to MsgPack!
//     flb_input_set(ctx, in_ffd, "format", "msgpack", NULL);
//     // No more JSON, but raw MsgPack delivery.
//     flb_lib_push(ctx, in_ffd, msgpack_buf, msgpack_buf_len);"

namespace {

/// The name of the table slice that the source yields.
constexpr auto table_slice_name = "tenzir.fluentbit";

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
  }

  // The engine is a move-only handle type.
  engine(engine&&) = default;
  auto operator=(engine&&) -> engine& = default;
  engine(const engine&) = delete;
  auto operator=(const engine&) -> engine& = delete;

  /// Copies data into the shared buffer with the Tenzir Fluent Bit plugin.
  /// @note This function is thread-safe.
  void append(const auto& str) {
    TENZIR_ASSERT_CHEAP(not str.empty());
    auto guard = std::lock_guard{*buffer_mtx_};
    // Ideally, every callback invocation produces valid JSON adhering to the
    // Fluent Bit convention of [first, second].
    // TODO: have another look at the Fluent Bit source code and validate this
    // assumption. Until then, we perform a cheap poorman's check to ensure the
    // input conforms to the expectation.
    TENZIR_ASSERT_CHEAP(str.back() == ']');
    buffer_.emplace_back(str);
  }

  /// Tries to consume the shared buffer with a function.
  /// @note This function is thread-safe.
  auto try_consume(auto f) -> size_t {
    // NB: this would be UB iff called in the same thread as append(). But since
    // append() is called by the Fluent Bit thread, it is not UB.
    if (auto lock = std::unique_lock{*buffer_mtx_, std::try_to_lock}) {
      if (not buffer_.empty()) {
        auto result = buffer_.size();
        for (const auto& line : buffer_)
          f(std::string_view{line});
        buffer_.clear();
        return result;
      }
    }
    return 0;
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
    : ctx_{ctx},
      poll_interval_{poll_interval},
      buffer_mtx_{std::make_unique<std::mutex>()} {
    TENZIR_ASSERT(ctx != nullptr);
    // We call this function only to produce a side effect of global state
    // initialization in Fluent Bit. This smells like a bug, yes. If we didn't
    // do this, we'd crash in flb_destroy with an attempt to deallocate the
    // pointer to thread-local state that first gets initialized in flb_start.
    // To avoid the crash, we indirectly initialize this state here.
    flb_init_env();
  }

  auto input(const std::string& plugin, const property_map& properties = {})
    -> bool {
    ffd_ = flb_input(ctx_, plugin.c_str(), nullptr);
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

  /// Starts the engine.
  auto start() -> bool {
    TENZIR_ASSERT_CHEAP(ctx_ != nullptr);
    TENZIR_DEBUG("starting Fluent Bit engine");
    auto ret = flb_start(ctx_);
    if (ret == 0) {
      started_ = true;
      return true;
    }
    TENZIR_ERROR("failed to start engine ({})", ret);
    return false;
  }

  /// Stops the engine.
  auto stop() -> bool {
    TENZIR_ASSERT_CHEAP(ctx_ != nullptr);
    if (not started_) {
      TENZIR_DEBUG("discarded attempt to stop unstarted engine");
      return false;
    }
    TENZIR_DEBUG("stopping Fluent Bit engine");
    for (size_t i = 0; ctx_->status == FLB_LIB_OK && i < num_stop_polls_; ++i) {
      TENZIR_DEBUG("sleeping while Fluent Bit context is okay");
      std::this_thread::sleep_for(poll_interval_);
    }
    auto ret = flb_stop(ctx_);
    if (ret == 0) {
      started_ = false;
      return true;
    }
    TENZIR_ERROR("failed to stop engine ({})", ret);
    return false;
  }

  flb_ctx_t* ctx_{nullptr}; ///< Fluent Bit context
  bool started_{false};     ///< Engine started/stopped status.
  int ffd_{-1};             ///< Fluent Bit handle for pushing data
  std::chrono::milliseconds poll_interval_{}; ///< How fast we check FB
  size_t num_stop_polls_{0};          ///< Number of polls in the destructor
  std::vector<std::string> buffer_{}; ///< Buffer shared with Fluent Bit
  std::unique_ptr<std::mutex> buffer_mtx_{}; ///< Protects the shared buffer
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
    auto builder = series_builder{};
    // FIXME: handle all return values.
    auto parse = [&builder](std::string_view line) {
      TENZIR_ASSERT(not line.empty());
      // What we're getting here is the typical Fluent Bit array consisting of
      // the following format, as described in
      // https://docs.fluentbit.io/manual/concepts/key-concepts#event-format:
      //
      //     [[TIMESTAMP, METADATA], MESSAGE]
      //
      // where
      //
      // - TIMESTAMP is a timestamp in seconds as an integer or floating point
      //   value (not a string);
      // - METADATA is a possibly-empty object containing event metadata; and
      // - MESSAGE is an object containing the event body.
      //
      // Fluent Bit versions prior to v2.1.0 instead used
      //
      //     [TIMESTAMP, MESSAGE]
      //
      // to represent events. This format is still supported for reading input
      // event streams.
      //
      // We are parsing this into a table with the following fields:
      //
      // 1. timestamp: time (timestamp alias type)
      // 2. metadata: record (inferred)
      // 3. message: record (inferred)
      auto json = from_json(line);
      if (not json) {
        TENZIR_WARN("invalid JSON: {}", line);
        return;
      }
      const auto* outer = caf::get_if<list>(&*json);
      if (outer == nullptr) {
        TENZIR_WARN("expected array as top-level JSON, got {}", *json);
        return;
      }
      if (outer->size() != 2) {
        TENZIR_WARN("expected two-element array at top-level, got {}",
                    outer->size());
        return;
      }
      // The outer framing is established, now create a new table slice row.
      auto row = builder.record();
      const auto& first = outer->front();
      // The first element must be either:
      // - TIMESTAMP
      // - [TIMESTAMP, METADATA]
      auto handle_first = detail::overload{
        [&](const auto&) {
          TENZIR_ERROR("expected array or number, got {}", first);
        },
        [&](const double& ts) {
          auto d = std::chrono::duration_cast<duration>(double_seconds(ts));
          row.field("timestamp").data(time{d});
        },
        [&](const uint64_t& ts) {
          row.field("timestamp").data(time{std::chrono::seconds(ts)});
        },
        [&](const list& xs) {
          if (xs.size() != 2) {
            TENZIR_WARN("expected 2-element inner array, got {}", xs.size());
            return;
          }
          auto handle_nested_first = detail::overload{
            [&](const auto&) {
              TENZIR_ERROR("expected timestamp or object, got {}", xs[0]);
            },
            [&](const double& n) {
              auto d = std::chrono::duration_cast<duration>(double_seconds(n));
              row.field("timestamp").data(time{d});
            },
            [&](const uint64_t& n) {
              row.field("timestamp").data(time{std::chrono::seconds(n)});
            },
          };
          caf::visit(handle_nested_first, xs[0]);
          row.field("metadata").data(make_view(xs[1]));
        },
      };
      caf::visit(handle_first, first);
      // The second array element is always the MESSAGE.
      const auto& second = outer->back();
      // We are not always getting a JSON object here. Sometimes we get an
      // escaped string that contains a JSON object that we need to extract
      // first. Fluent Bit has a concept of *encoders* and *decoders* for this
      // purpose: https://docs.fluentbit.io/manual/pipeline/parsers/decoders.
      // Parsers can be configured with a decoder using the option
      // `decode_field json <field>`.
      //
      // While this means there are potentially infinite choices to make, in
      // reality we see hopefully mostly default configurations that cover 99%
      // of decoding needs: a nested field "log" with a string that is escaped
      // JSON. That's what we're looking for manually for now. If users come
      // with more flexible decoding requests, we need to adapt.
      auto decoded = false;
      if (const auto* rec = caf::get_if<record>(&second)) {
        if (auto log = try_get<std::string>(*rec, "log")) {
          if (*log and not(*log)->empty()) {
            if (auto log_json = from_json(**log)) {
              row.field("message").data(record{
                {"log", std::move(*log_json)},
              });
              decoded = true;
            }
          }
        }
      }
      if (not decoded)
        row.field("message").data(make_view(second));
    };
    auto last_finish = std::chrono::steady_clock::now();
    while (engine->running()) {
      const auto now = std::chrono::steady_clock::now();
      // Poll the engine and process data that Fluent Bit already handed over.
      if (engine->try_consume(parse) == 0) {
        TENZIR_DEBUG("sleeping for {}", args_.poll_interval);
        std::this_thread::sleep_for(args_.poll_interval);
      }
      auto max_slice_length
        = detail::narrow_cast<int64_t>(defaults::import::table_slice_size);
      if (builder.length() >= max_slice_length
          or last_finish + defaults::import::batch_timeout < now) {
        TENZIR_DEBUG("flushing table slice with {} rows", builder.length());
        last_finish = now;
        for (auto& slice : builder.finish_as_table_slice(table_slice_name))
          co_yield slice;
      } else {
        co_yield {};
      }
    }
    TENZIR_WARN("heejahooo");
    if (builder.length() > 0) {
      TENZIR_DEBUG("flushing last table slice with {} rows", builder.length());
      for (auto& slice : builder.finish_as_table_slice(table_slice_name))
        co_yield slice;
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
