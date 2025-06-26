//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/record_batch.h>

#include <cstring>
#include <queue>
#include <stdexcept>
#include <string_view>

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

inline auto to_str(const msgpack_object& object) {
  return std::string_view{object.via.str.ptr, object.via.str.size};
}

inline auto to_array(const msgpack_object& object) {
  return std::span<msgpack_object>{object.via.array.ptr,
                                   size_t{object.via.array.size}};
}

inline auto to_map(const msgpack_object& object) {
  return std::span<msgpack_object_kv>{object.via.map.ptr,
                                      size_t{object.via.map.size}};
}

inline auto to_bin(const msgpack_object& object) {
  return std::span<const std::byte>{
    reinterpret_cast<const std::byte*>(object.via.bin.ptr),
    size_t{object.via.bin.size}};
}

/// A MsgPack object.
inline auto visit(auto f, const msgpack_object& object) {
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
    if (result == MSGPACK_UNPACK_SUCCESS) {
      return unpacked_.data;
    }
    return std::nullopt;
  }

private:
  msgpack_unpacked unpacked_;
};

/// Reimplementation of flb_time_msgpack_to_time to meet our needs.
inline auto to_flb_time(const msgpack_object& object) -> std::optional<time> {
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

/// A map of key-value pairs of Fluent Bit plugin configuration options.
using property_map = std::map<std::string, std::string>;

inline void to_property_map_or_request(const located<tenzir::record>& rec,
                                       property_map& map,
                                       std::vector<secret_request>& requests,
                                       diagnostic_handler& dh) {
  for (const auto& [key, value] : rec.inner) {
    // Avoid double quotes around strings.
    if (const auto* str = try_as<std::string>(&value)) {
      const auto [it, inserted] = map.try_emplace(key, *str);
      TENZIR_ASSERT(inserted);
      continue;
    }
    if (const auto* s = try_as<secret>(value)) {
      requests.emplace_back(
        *s, rec.source,
        [key, loc = rec.source, &dh, &map](resolved_secret_value v) {
          const auto [it, inserted] = map.try_emplace(
            key, std::string{v.utf8_view(key, loc, dh).unwrap()});
          TENZIR_ASSERT(inserted);
        });
      continue;
    }
    const auto [it, inserted] = map.try_emplace(key, fmt::format("{}", value));
    TENZIR_ASSERT(inserted);
  }
}

/// The arguments passed to the operator.
struct operator_args {
  located<std::string> plugin;                  ///< Fluent Bit plugin name.
  std::chrono::milliseconds poll_interval{250}; ///< Engine poll interval.
  located<record> service_properties;           ///< The global service options.
  located<record> args;                         ///< The plugin arguments.

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
  make_source(const operator_args& args, const record& global_config,
              const property_map& fluent_bit_args,
              const property_map& plugin_args, diagnostic_handler& dh)
    -> std::unique_ptr<engine> {
    auto result
      = make_engine(global_config, args.poll_interval, fluent_bit_args, dh);
    if (not result) {
      return result;
    }
    if (auto error = result->input(args.plugin.inner, plugin_args)) {
      error->annotations.emplace_back(true, "", args.plugin.source);
      dh.emit(std::move(*error));
      return {};
    }
    auto callback = flb_lib_out_cb{
      .cb = handle_lib_output,
      .data = result.get(),
    };
    // There are two options for the `lib` output:
    // - format: "msgpack" or "json"
    // - max_records: integer representing the maximum number of records to
    //   process per single flush call.
    if (auto error
        = result->output("lib", {{"format", "msgpack"}}, &callback)) {
      dh.emit(std::move(*error));
      return {};
    }
    if (auto error = result->start()) {
      dh.emit(std::move(*error));
      return {};
    }
    return result;
  }

  /// Constructs a Fluent Bit engine for use as "sink" in a pipeline.
  static auto make_sink(const operator_args& args, const record& plugin_config,
                        const property_map& fluent_bit_args,
                        const property_map& plugin_args, diagnostic_handler& dh)
    -> std::unique_ptr<engine> {
    auto result
      = make_engine(plugin_config, args.poll_interval, fluent_bit_args, dh);
    if (not result) {
      return result;
    }
    if (auto error = result->input("lib")) {
      dh.emit(std::move(*error));
      return {};
    }
    if (auto error = result->output(args.plugin.inner, plugin_args)) {
      error->annotations.emplace_back(true, "", args.plugin.source);
      dh.emit(std::move(*error));
      return {};
    }
    if (auto error = result->start()) {
      dh.emit(std::move(*error));
      return {};
    }
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
  auto push(std::string_view data) -> failure_or<void> {
    TENZIR_ASSERT(ctx_ != nullptr);
    TENZIR_ASSERT(ffd_ >= 0);
    auto written = size_t{};
    while (written != data.size()) {
      auto ret = flb_lib_push(ctx_, ffd_, data.data() + written,
                              data.size() - written);
      if (ret == FLB_LIB_ERROR) {
        return failure::promise();
      }
      TENZIR_ASSERT(ret >= 0);
      written += ret;
    }
    return {};
  }

private:
  static auto make_engine(const record& global_properties,
                          std::chrono::milliseconds poll_interval,
                          const property_map& local_properties,
                          diagnostic_handler& dh) -> std::unique_ptr<engine> {
    auto* ctx = flb_create();
    if (ctx == nullptr) {
      diagnostic::error("failed to create Fluent Bit context").emit(dh);
      return {};
    }
    // Initialize some TLS variables. If we don't do this we get a bad `free`
    // call in flb_destroy in case we try to use a plugin that does not exists.
    flb_sched_ctx_init();
    // Start with a less noisy log level.
    if (flb_service_set(ctx, "log_level", "error", nullptr) != 0) {
      diagnostic::error("failed to adjust Fluent Bit log_level").emit(dh);
      return {};
    }
    for (const auto& [key, value] : global_properties) {
      auto str_value = to_string(value);
      TENZIR_DEBUG("setting global service option: {}={}", key, str_value);
      if (flb_service_set(ctx, key.c_str(), str_value.c_str(), nullptr) != 0) {
        diagnostic::error("failed to set global service option: {}={}", key,
                          str_value)
          .emit(dh);
        return {};
      }
    }
    for (const auto& [key, value] : local_properties) {
      TENZIR_DEBUG("setting local service option: {}={}", key, value);
      if (flb_service_set(ctx, key.c_str(), value.c_str(), nullptr) != 0) {
        diagnostic::error("failed to set local service option: {}={}", key,
                          value)
          .emit(dh);
        return {};
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
    -> std::optional<diagnostic> {
    ffd_ = flb_input(ctx_, plugin.c_str(), nullptr);
    if (ffd_ < 0) {
      return diagnostic::error("failed to setup Fluent Bit `{}` input plugin ",
                               plugin)
        .note("error code `{}`", ffd_)
        .done();
      ;
    }
    // Apply user-provided plugin properties.
    for (const auto& [key, value] : properties) {
      TENZIR_DEBUG("setting {} plugin option: {}={}", plugin, key, value);
      if (flb_input_set(ctx_, ffd_, key.c_str(), value.c_str(), nullptr) != 0) {
        return diagnostic::error("failed to set Fluent Bit `{}` plugin option: "
                                 "{}={}",
                                 plugin, key, value)
          .done();
      }
    }
    return {};
  }

  auto output(const std::string& plugin, const property_map& properties = {},
              struct flb_lib_out_cb* callback = nullptr)
    -> std::optional<diagnostic> {
    auto ffd = flb_output(ctx_, plugin.c_str(), callback);
    if (ffd < 0) {
      return diagnostic::error("failed to setup Fluent Bit `{}` output plugin ",
                               plugin)
        .note("error code `{}`", ffd)
        .done();
    }
    // Apply user-provided plugin properties.
    for (const auto& [key, value] : properties) {
      TENZIR_DEBUG("setting {} plugin option: {}={}", plugin, key, value);
      if (flb_output_set(ctx_, ffd, key.c_str(), value.c_str(), nullptr) != 0) {
        return diagnostic::error("failed to set Fluent Bit `{}` plugin option: "
                                 "{}={}",
                                 plugin, key, value)
          .done();
      }
    }
    return {};
  }

  /// Starts the engine.
  auto start() -> std::optional<diagnostic> {
    TENZIR_ASSERT(ctx_ != nullptr);
    TENZIR_DEBUG("starting Fluent Bit engine");
    auto ret = flb_start(ctx_);
    if (ret == 0) {
      running_ = true;
      return {};
    }
    return diagnostic::error("failed to start fluentbit engine")
      .note("return code `{}`", ret)
      .done();
  }

  /// Stops the engine.
  auto stop() -> bool {
    TENZIR_ASSERT(ctx_ != nullptr);
    if (not running_) {
      TENZIR_DEBUG(
        "ignoring `stop()` for since the engine was not started successfully");
      return false;
    }
    TENZIR_DEBUG("stopping Fluent Bit engine");
    for (size_t i = 0; ctx_->status == FLB_LIB_OK && i < num_stop_polls_; ++i) {
      TENZIR_DEBUG("sleeping while Fluent Bit context is okay");
      std::this_thread::sleep_for(poll_interval_);
    }
    auto ret = flb_stop(ctx_);
    if (ret == 0) {
      running_ = false;
      return true;
    }
    TENZIR_ERROR("failed to stop fluentbit engine ({})", ret);
    return false;
  }

  flb_ctx_t* ctx_{nullptr}; ///< Fluent Bit context
  bool running_{false};     ///< Engine started/stopped status.
  int ffd_{-1};             ///< Fluent Bit handle for pushing data
  std::chrono::milliseconds poll_interval_{}; ///< How fast we check FB
  size_t num_stop_polls_{0};      ///< Number of polls in the destructor
  std::queue<chunk_ptr> queue_{}; ///< MsgPack chunks shared with Fluent Bit
  std::unique_ptr<std::mutex> buffer_mtx_{}; ///< Protects the shared buffer
};

auto add(auto field, const msgpack_object& object, diagnostic_handler& dh,
         bool decode = false) -> bool {
  auto f = detail::overload{
    [&](std::nullopt_t) {
      field.null();
      return true;
    },
    [&](auto x) {
      field.data(x);
      return true;
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
          return true;
        }
        return false;
      }
      field.data_unparsed(x);
      return true;
    },
    [&](std::span<const std::byte> xs) {
      field.data(blob{xs.begin(), xs.end()});
      return true;
    },
    [&](std::span<msgpack_object> xs) {
      auto list = field.list();
      for (const auto& x : xs) {
        if (not add(list, x, dh, decode)) {
          return false;
        }
      }
      return true;
    },
    [&](std::span<msgpack_object_kv> xs) {
      auto record = field.record();
      for (const auto& kvp : xs) {
        if (kvp.key.type != MSGPACK_OBJECT_STR) {
          diagnostic::warning("invalid Fluent Bit record")
            .note("failed to parse key")
            .note("got {}", kvp.key.type)
            .emit(dh);
          return false;
        }
        auto key = msgpack::to_str(kvp.key);
        auto field = record.unflattened_field(key);
        // TODO: restrict this attempt to decode to the top-level field "log"
        // only. We currently attempt to parse *all* fields named "log" as JSON.
        if (not add(field, kvp.val, dh, key == "log")) {
          return false;
        }
      }
      return true;
    },
    [&](const msgpack_object_ext& ext) {
      diagnostic::warning("unknown MsgPack type")
        .note("cannot handle MsgPack extensions")
        .note("got {}", ext.type)
        .emit(dh);
      return false;
    },
    [&](const unknown_msgpack_type&) {
      diagnostic::warning("unknown MsgPack type")
        .note("got {}", object.type)
        .emit(dh);
      return false;
    },
  };
  return msgpack::visit(f, object);
}

template <bool enable_source, bool enable_sink>
  requires(enable_source or enable_sink)
class fluent_bit_operator_impl final
  : public crtp_operator<fluent_bit_operator_impl<enable_source, enable_sink>> {
public:
  fluent_bit_operator_impl() = default;

  fluent_bit_operator_impl(operator_args operator_args,
                           multi_series_builder::options builder_options,
                           record config)
    : operator_args_{std::move(operator_args)},
      builder_options_{std::move(builder_options)},
      config_{std::move(config)} {
  }

  fluent_bit_operator_impl(operator_args operator_args, record config)
    requires(not enable_source)
    : operator_args_{std::move(operator_args)}, config_{std::move(config)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<table_slice>
    requires enable_source
  {
    co_yield {};
    auto requests = std::vector<secret_request>{};
    auto fluent_bit_args = property_map{};
    auto plugin_args = property_map{};
    to_property_map_or_request(operator_args_.service_properties,
                               fluent_bit_args, requests, ctrl.diagnostics());
    to_property_map_or_request(operator_args_.args, plugin_args, requests,
                               ctrl.diagnostics());
    if (ctrl.resolve_secrets_must_yield(std::move(requests))) {
      co_yield {};
    }
    auto engine = engine::make_source(operator_args_, config_, fluent_bit_args,
                                      plugin_args, ctrl.diagnostics());
    if (not engine) {
      co_return;
    }
    auto dh = transforming_diagnostic_handler{
      ctrl.diagnostics(),
      [&](diagnostic d) {
        d.message = fmt::format("fluent-bit parser: {}", d.message);
        return d;
      },
    };
    auto msb = multi_series_builder{
      builder_options_,
      dh,
    };
    auto parse = [&ctrl, &msb](chunk_ptr chunk) {
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
      auto row = msb.record();
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
          msb.remove_last();
          return;
        }
        auto timestamp = msgpack::to_flb_time(xs[0]);
        if (not timestamp) {
          diagnostic::warning("invalid Fluent Bit message")
            .note("failed to parse timestamp in first-level array")
            .note("got MsgPack type {}", xs[0].type)
            .emit(ctrl.diagnostics());
          msb.remove_last();
          return;
        }
        row.exact_field("timestamp").data(*timestamp);
        if (xs[1].type == MSGPACK_OBJECT_MAP) {
          auto map = msgpack::to_map(xs[1]);
          if (not map.empty()) {
            auto metadata = row.exact_field("metadata");
            if (not add(metadata, xs[1], ctrl.diagnostics())) {
              msb.remove_last();
              return;
            }
          }
        } else {
          diagnostic::warning("invalid Fluent Bit message")
            .note("failed parse metadata in first-level array")
            .note("got MsgPack type {}, expected map", xs[1].type)
            .emit(ctrl.diagnostics());
          msb.remove_last();
          return;
        }
      } else if (auto timestamp = msgpack::to_flb_time(first)) {
        row.exact_field("timestamp").data(*timestamp);
      } else {
        diagnostic::warning("invalid Fluent Bit message")
          .note("failed to parse first-level array element")
          .note("got MsgPack type {}, expected array or timestamp", first.type)
          .emit(ctrl.diagnostics());
        msb.remove_last();
        return;
      }
      // Process the MESSAGE, i.e., the second top-level array element.
      auto message = row.exact_field("message");
      if (not add(message, second, ctrl.diagnostics())) {
        msb.remove_last();
        return;
      }
    };
    while (engine->running()) {
      for (auto& v : msb.yield_ready_as_table_slice()) {
        co_yield std::move(v);
      }
      auto num_elements = engine->try_consume(parse);
      if (num_elements == 0) {
        TENZIR_DEBUG("sleeping for {}", operator_args_.poll_interval);
        std::this_thread::sleep_for(operator_args_.poll_interval);
      }
    }
    for (auto& v : msb.finalize_as_table_slice()) {
      co_yield std::move(v);
    }
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate>
    requires enable_sink
  {
    co_yield {};
    auto requests = std::vector<secret_request>{};
    auto fluent_bit_args = property_map{};
    auto plugin_args = property_map{};
    to_property_map_or_request(operator_args_.service_properties,
                               fluent_bit_args, requests, ctrl.diagnostics());
    to_property_map_or_request(operator_args_.args, plugin_args, requests,
                               ctrl.diagnostics());
    if (ctrl.resolve_secrets_must_yield(std::move(requests))) {
      co_yield {};
    }
    auto engine = engine::make_sink(operator_args_, config_, fluent_bit_args,
                                    plugin_args, ctrl.diagnostics());
    if (not engine) {
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
      auto array = check(to_record_batch(resolved_slice)->ToStructArray());
      auto failed = false;
      for (const auto& row : values3(*array)) {
        auto it = std::back_inserter(event);
        TENZIR_ASSERT(row);
        auto printer = json_printer{{
          .oneline = true,
        }};
        const auto ok = printer.print(it, *row);
        TENZIR_ASSERT(ok);
        // Wrap JSON object in the 2-element JSON array that Fluent Bit expects.
        auto message = fmt::format("[{}, {}]", flb_time_now(), event);
        if (engine->push(message).is_error()) {
          failed = true;
        }
        event.clear();
      }
      if (failed) {
        diagnostic::warning("failed to push data into Fluent Bit Engine")
          .emit(ctrl.diagnostics());
      }
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    if constexpr (enable_source and enable_sink) {
      return "fluent-bit";
    } else if constexpr (enable_source) {
      return "from_fluent_bit";
    } else {
      return "to_fluent_bit";
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    if constexpr (enable_source) {
      auto builder_options = builder_options_;
      builder_options.settings.ordered = order == event_order::ordered;
      auto replacement = std::make_unique<fluent_bit_operator_impl>(
        this->operator_args_, std::move(builder_options), this->config_);
      return {filter, order, std::move(replacement)};
    } else {
      TENZIR_UNUSED(filter, order);
      return do_not_optimize(*this);
    }
  }

  friend auto inspect(auto& f, fluent_bit_operator_impl& x) -> bool {
    return f.object(x).fields(f.field("operator_args", x.operator_args_),
                              f.field("builder_options", x.builder_options_),
                              f.field("config", x.config_));
  }

private:
  operator_args operator_args_;
  multi_series_builder::options builder_options_;
  record config_;
};

using fluent_bit_operator = fluent_bit_operator_impl<true, true>;
using fluent_bit_source_operator = fluent_bit_operator_impl<true, false>;
using fluent_bit_sink_operator = fluent_bit_operator_impl<false, true>;

} // namespace
} // namespace tenzir::plugins::fluentbit
