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
#include <queue>
#include <stdexcept>

#include <fluent-bit/fluent-bit-minimal.h>

// Tag type for when we are retrieving MsgPack objects with an unknown type.
struct unknown_msgpack_type {};

template <>
struct fmt::formatter<msgpack_object_type> : fmt::formatter<std::string_view> {
  auto format(msgpack_object_type type, format_context& ctx) const {
    std::string_view name = "Unknown";
    switch (type) {
      case MSGPACK_OBJECT_NIL:
        name = "Nil";
        break;
      case MSGPACK_OBJECT_BOOLEAN:
        name = "Boolean";
        break;
      case MSGPACK_OBJECT_POSITIVE_INTEGER:
        name = "Positive Integer";
        break;
      case MSGPACK_OBJECT_NEGATIVE_INTEGER:
        name = "Negative Integer";
        break;
      case MSGPACK_OBJECT_FLOAT32:
        [[fallthrough]];
      case MSGPACK_OBJECT_FLOAT64:
        name = "Float";
        break;
      case MSGPACK_OBJECT_STR:
        name = "String";
        break;
      case MSGPACK_OBJECT_ARRAY:
        name = "Array";
        break;
      case MSGPACK_OBJECT_MAP:
        name = "Map";
        break;
      case MSGPACK_OBJECT_BIN:
        name = "Binary";
        break;
      case MSGPACK_OBJECT_EXT:
        name = "Extension";
        break;
    }
    return fmt::formatter<std::string_view>::format(name, ctx);
  }
};

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

/// Utilities wrapping the MsgPack C API.
namespace msgpack {

auto to_str(const msgpack_object& object) {
  return std::string_view{object.via.str.ptr, object.via.str.size};
}

auto to_array(const msgpack_object& object) {
  return std::span<msgpack_object>{object.via.array.ptr,
                                   size_t{object.via.array.size}};
}

auto to_map(const msgpack_object& object) {
  return std::span<msgpack_object_kv>{object.via.map.ptr,
                                      size_t{object.via.map.size}};
}

auto to_bin(const msgpack_object& object) {
  return std::span<const std::byte>{
    reinterpret_cast<const std::byte*>(object.via.bin.ptr),
    size_t{object.via.bin.size}};
}

/// A MsgPack object.
auto visit(auto f, const msgpack_object& object) {
  switch (object.type) {
    case MSGPACK_OBJECT_NIL:
      return f(std::nullopt);
    case MSGPACK_OBJECT_BOOLEAN:
      return f(object.via.boolean);
    case MSGPACK_OBJECT_POSITIVE_INTEGER:
      return f(object.via.u64);
    case MSGPACK_OBJECT_NEGATIVE_INTEGER:
      return f(object.via.i64);
    case MSGPACK_OBJECT_FLOAT32:
      [[fallthrough]];
    case MSGPACK_OBJECT_FLOAT64:
      return f(object.via.f64);
    case MSGPACK_OBJECT_STR:
      return f(to_str(object));
    case MSGPACK_OBJECT_ARRAY:
      return f(to_array(object));
    case MSGPACK_OBJECT_MAP:
      return f(to_map(object));
    case MSGPACK_OBJECT_BIN:
      return f(to_bin(object));
    case MSGPACK_OBJECT_EXT:
      return f(object.via.ext);
  }
  return f(unknown_msgpack_type{});
}

/// RAII-style wrapper around `msgpack_unpack`.
class unpacked {
public:
  unpacked() noexcept {
    msgpack_unpacked_init(&unpacked_);
  }

  unpacked(unpacked&& other) : unpacked_{other.unpacked_} {
    other.unpacked_.zone = nullptr;
  }

  auto operator=(unpacked&& other) -> unpacked& {
    unpacked_ = other.unpacked_;
    other.unpacked_.zone = nullptr;
    return *this;
  }

  unpacked(const unpacked&) = delete;
  auto operator=(const unpacked&) -> unpacked& = delete;

  ~unpacked() noexcept {
    msgpack_unpacked_destroy(&unpacked_);
  }

  // Opinionated version of `msgpack_unpack_next` that can only yield an object.
  auto unpack(std::span<const std::byte> bytes)
    -> std::optional<msgpack_object> {
    auto offset = size_t{0};
    auto result
      = msgpack_unpack_next(&unpacked_,
                            reinterpret_cast<const char*>(bytes.data()),
                            bytes.size(), &offset);
    if (result == MSGPACK_UNPACK_SUCCESS)
      return unpacked_.data;
    return std::nullopt;
  }

private:
  msgpack_unpacked unpacked_;
};

/// Reimplementation of flb_time_msgpack_to_time to meet our needs.
auto to_flb_time(const msgpack_object& object) -> std::optional<time> {
  auto f = detail::overload{
    [](auto) -> std::optional<time> {
      return std::nullopt;
    },
    [](uint64_t x) -> std::optional<time> {
      auto secs = std::chrono::seconds{x};
      return time{secs};
    },
    [](double x) -> std::optional<time> {
      auto secs = double_seconds{x};
      return time{std::chrono::duration_cast<duration>(secs)};
    },
    [](const msgpack_object_ext& ext) -> std::optional<time> {
      if (ext.type != 0 || ext.size != 8) {
        return std::nullopt;
      }
      // Fluent Bit encodes seconds and nanoseconds as two 32-bit unsigned
      // integers into the extension type pointer.
      auto u32 = uint32_t{0};
      std::memcpy(&u32, &ext.ptr[0], 4);
      auto result = time{std::chrono::seconds{ntohl(u32)}};
      std::memcpy(&u32, &ext.ptr[4], 4);
      result += std::chrono::nanoseconds{ntohl(u32)};
      return result;
    },
  };
  return visit(f, object);
}

} // namespace msgpack

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
    auto deleter = [record]() noexcept {
      flb_lib_free(record);
    };
    auto* self = reinterpret_cast<engine*>(data);
    self->append(chunk::make(record, size, deleter));
    return 0;
  }

public:
  /// Constructs a Fluent Bit engine for use as "source" in a pipeline.
  static auto
  make_source(const operator_args& args, const record& plugin_config)
    -> caf::expected<std::unique_ptr<engine>> {
    auto result
      = make_engine(plugin_config, args.poll_interval, args.service_properties);
    if (not result)
      return result;
    if (not(*result)->input(args.plugin, args.args))
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to setup Fluent Bit {} input",
                                         args.plugin));
    auto callback = flb_lib_out_cb{
      .cb = handle_lib_output,
      .data = result->get(),
    };
    // There are two options for the `lib` output:
    // - format: "msgpack" or "json"
    // - max_records: integer representing the maximum number of records to
    //   process per single flush call.
    if (not(*result)->output("lib", {{"format", "msgpack"}}, &callback))
      return caf::make_error(ec::unspecified,
                             "failed to setup Fluent Bit lib output");
    if (not(*result)->start())
      return caf::make_error(ec::unspecified,
                             "failed to start Fluent Bit engine");
    return result;
  }

  /// Constructs a Fluent Bit engine for use as "sink" in a pipeline.
  static auto make_sink(const operator_args& args, const record& plugin_config)
    -> caf::expected<std::unique_ptr<engine>> {
    auto result
      = make_engine(plugin_config, args.poll_interval, args.service_properties);
    if (not result)
      return result;
    if (not(*result)->input("lib"))
      return caf::make_error(ec::unspecified,
                             "failed to setup Fluent Bit lib input");
    if (not(*result)->output(args.plugin, args.args))
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to setup Fluent Bit {} outut",
                                         args.plugin));
    if (not(*result)->start())
      return caf::make_error(ec::unspecified,
                             "failed to start Fluent Bit engine");
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
  void append(chunk_ptr chunk) {
    auto guard = std::lock_guard{*buffer_mtx_};
    queue_.push(std::move(chunk));
  }

  /// Tries to consume the shared buffer with a function.
  /// @note This function is thread-safe.
  /// @returns the number of consumed events.
  auto try_consume(auto f) -> size_t {
    // NB: this would be UB iff called in the same thread as append(). But since
    // append() is called by the Fluent Bit thread, it is not UB.
    if (auto lock = std::unique_lock{*buffer_mtx_, std::try_to_lock}) {
      auto result = queue_.size();
      while (not queue_.empty()) {
        f(queue_.front());
        queue_.pop();
        return result;
      }
      return result;
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
    TENZIR_ASSERT(ctx_ != nullptr);
    return ctx_->status == FLB_LIB_OK;
  }

  /// Pushes data into Fluent Bit.
  auto push(std::string_view data) -> bool {
    TENZIR_ASSERT(ctx_ != nullptr);
    TENZIR_ASSERT(ffd_ >= 0);
    return flb_lib_push(ctx_, ffd_, data.data(), data.size()) != 0;
  }

private:
  static auto make_engine(const record& global_properties,
                          std::chrono::milliseconds poll_interval,
                          const property_map& local_properties)
    -> caf::expected<std::unique_ptr<engine>> {
    auto* ctx = flb_create();
    if (ctx == nullptr)
      return caf::make_error(ec::unspecified,
                             "failed to create Fluent Bit context");
    // Start with a less noisy log level.
    if (flb_service_set(ctx, "log_level", "error", nullptr) != 0)
      return caf::make_error(ec::unspecified,
                             "failed to adjust Fluent Bit log_level");
    for (const auto& [key, value] : global_properties) {
      auto str_value = to_string(value);
      TENZIR_DEBUG("setting global service option: {}={}", key, str_value);
      if (flb_service_set(ctx, key.c_str(), str_value.c_str(), nullptr) != 0)
        return caf::make_error(ec::unspecified,
                               fmt::format("failed to set global service "
                                           "option: {}={}", //
                                           key, str_value));
    }
    for (const auto& [key, value] : local_properties) {
      TENZIR_DEBUG("setting local service option: {}={}", key, value);
      if (flb_service_set(ctx, key.c_str(), value.c_str(), nullptr) != 0)
        return caf::make_error(
          ec::unspecified,
          fmt::format("failed to set local service option: {}={}", key, value));
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
    TENZIR_ASSERT(ctx_ != nullptr);
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
    TENZIR_ASSERT(ctx_ != nullptr);
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
  size_t num_stop_polls_{0};      ///< Number of polls in the destructor
  std::queue<chunk_ptr> queue_{}; ///< MsgPack chunks shared with Fluent Bit
  std::unique_ptr<std::mutex> buffer_mtx_{}; ///< Protects the shared buffer
};

auto add(auto& field, const msgpack_object& object, bool decode = false)
  -> void {
  auto f = detail::overload{
    [&](std::nullopt_t) {
      field.data(caf::none);
    },
    [&](auto x) {
      field.data(x);
    },
    [&](std::string_view x) {
      // Sometimes we get an escaped string that contains a JSON object
      // that we may need to extract first. Fluent Bit has a concept of
      // *encoders* and *decoders* for this purpose:
      // https://docs.fluentbit.io/manual/pipeline/parsers/decoders.
      // Parsers can be configured with a decoder using the option
      // `decode_field json <field>`.
      if (decode) {
        if (auto json = from_json(x)) {
          field.data(*json);
          return;
        }
      }
      field.data(x);
    },
    [&](std::span<const std::byte> xs) {
      auto blob = std::basic_string_view<std::byte>{xs.data(), xs.size()};
      field.data(blob);
    },
    [&](std::span<msgpack_object> xs) {
      auto list = field.list();
      for (const auto& x : xs)
        add(list, x, decode);
    },
    [&](std::span<msgpack_object_kv> xs) {
      auto record = field.record();
      for (const auto& kvp : xs) {
        if (kvp.key.type != MSGPACK_OBJECT_STR)
          diagnostic::warning("invalid Fluent Bit record")
            .note("failed to parse key")
            .note("got {}", kvp.key.type)
            .throw_();
        auto key = msgpack::to_str(kvp.key);
        auto field = record.field(key);
        // TODO: restrict this attempt to decode to the top-level field "log"
        // only. We currently attempt to parse *all* fields named "log" as JSON.
        add(field, kvp.val, key == "log");
      }
    },
    [&](const msgpack_object_ext& ext) {
      diagnostic::warning("unknown MsgPack type")
        .note("cannot handle MsgPack extensions")
        .note("got {}", ext.type)
        .throw_();
    },
    [&](const unknown_msgpack_type&) {
      diagnostic::warning("unknown MsgPack type")
        .note("got {}", object.type)
        .throw_();
    },
  };
  msgpack::visit(f, object);
}

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
        .hint("{}", engine.error())
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto builder = series_builder{};
    auto parse = [&ctrl, &builder](chunk_ptr chunk) {
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
      //
      auto unpacked = msgpack::unpacked{};
      auto object = unpacked.unpack(as_bytes(chunk));
      // The unpacking operation cannot fail because we are calling this
      // function within a while loop checking that msgpack_unpack_next
      // returned MSGPACK_UNPACK_SUCCESS. See out_lib_flush() in
      // plugins/out_lib/out_lib.c in the Fluent Bit code base for details.
      TENZIR_ASSERT(object);
      if (object->type != MSGPACK_OBJECT_ARRAY) {
        diagnostic::warning("invalid Fluent Bit message")
          .note("expected array as top-level object")
          .note("got MsgPack type {}", object->type)
          .emit(ctrl.diagnostics());
        return;
      }
      const auto& outer = msgpack::to_array(*object);
      if (outer.size() != 2) {
        diagnostic::warning("invalid Fluent Bit message")
          .note("expected two-element array at top-level object")
          .note("got {} elements", outer.size())
          .emit(ctrl.diagnostics());
        return;
      }
      // The outer framing is established, now create a new table slice row.
      auto row = builder.record();
      const auto& first = outer[0];
      const auto& second = outer[1];
      // The first-level array element must be either:
      // - [TIMESTAMP, METADATA] (array)
      // - TIMESTAMP (extension)
      if (first.type == MSGPACK_OBJECT_ARRAY) {
        auto xs = msgpack::to_array(first);
        if (xs.size() != 2) {
          diagnostic::warning("invalid Fluent Bit message")
            .note("wrong number of array elements in first-level array")
            .note("got {}, expected 2", xs.size())
            .emit(ctrl.diagnostics());
          return;
        }
        auto timestamp = msgpack::to_flb_time(xs[0]);
        if (not timestamp) {
          diagnostic::warning("invalid Fluent Bit message")
            .note("failed to parse timestamp in first-level array")
            .note("got MsgPack type {}", xs[0].type)
            .emit(ctrl.diagnostics());
          return;
        }
        row.field("timestamp").data(*timestamp);
        if (xs[1].type == MSGPACK_OBJECT_MAP) {
          auto map = msgpack::to_map(xs[1]);
          if (not map.empty()) {
            auto metadata = row.field("metadata");
            add(metadata, xs[1]);
          }
        } else {
          diagnostic::warning("invalid Fluent Bit message")
            .note("failed parse metadata in first-level array")
            .note("got MsgPack type {}, expected map", xs[1].type)
            .emit(ctrl.diagnostics());
        }
      } else if (auto timestamp = msgpack::to_flb_time(first)) {
        row.field("timestamp").data(*timestamp);
      } else {
        diagnostic::warning("invalid Fluent Bit message")
          .note("failed to parse first-level array element")
          .note("got MsgPack type {}, expected array or timestamp", first.type)
          .emit(ctrl.diagnostics());
      }
      // Process the MESSAGE, i.e., the second top-level array element.
      auto message = row.field("message");
      add(message, second);
    };
    auto last_finish = std::chrono::steady_clock::now();
    while ((*engine)->running()) {
      const auto now = std::chrono::steady_clock::now();
      // Poll the engine and process data that Fluent Bit already handed over.
      auto num_elements = (*engine)->try_consume(parse);
      if (num_elements == 0) {
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
        .hint("{}", engine.error())
        .emit(ctrl.diagnostics());
      co_return;
    }
    (*engine)->max_wait_before_stop(std::chrono::seconds(1));
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
        TENZIR_ASSERT(row);
        auto printer = json_printer{{
          .oneline = true,
        }};
        const auto ok = printer.print(it, *row);
        TENZIR_ASSERT(ok);
        // Wrap JSON object in the 2-element JSON array that Fluent Bit expects.
        auto message = fmt::format("[{}, {}]", flb_time_now(), event);
        if (not(*engine)->push(message))
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
