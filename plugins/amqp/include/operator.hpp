//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/config.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution_utilities.hpp>

#include <caf/expected.hpp>

#include <string_view>

#if __has_include(<rabbitmq-c/amqp.h>)
#  include <rabbitmq-c/amqp.h>
#  include <rabbitmq-c/ssl_socket.h>
#  include <rabbitmq-c/tcp_socket.h>
#else
#  include <amqp.h>
#  include <amqp_ssl_socket.h>
#  include <amqp_tcp_socket.h>
#endif

using namespace std::chrono_literals;

namespace tenzir::plugins::amqp {

namespace {

/// The default channel number.
constexpr auto default_channel = amqp_channel_t{1};

/// The default queue name.
constexpr auto default_exchange = std::string_view{"amq.direct"};

/// The default queue name.
constexpr auto default_queue = std::string_view{};

/// The default routing key.
constexpr auto default_routing_key = std::string_view{};

/// Assume ownership of the memory and wrap it in a chunk.
/// @param msg The bytes to move into a chunk.
inline auto move_into_chunk(amqp_bytes_t& bytes) -> chunk_ptr {
  auto deleter = [bytes]() noexcept {
    amqp_bytes_free(bytes);
  };
  return chunk::make(bytes.bytes, bytes.len, deleter);
}

inline auto as_amqp_bool(bool x) -> amqp_boolean_t {
  return x ? 1 : 0;
}

/// Interprets a string view as AMQP bytes.
inline auto as_amqp_bytes(std::string_view str) -> amqp_bytes_t {
  if (str.empty()) {
    return amqp_empty_bytes;
  }
  // Many RabbitMQ functions take an amqp_bytes_t structure as input.
  // Unfortunately there's not const-preserving equivalent, so we have to bite
  // the const_cast bullet.
  return {
    .len = str.size(),
    .bytes = const_cast<char*>(str.data()),
  };
}

/// Interprets a chunk as AMQP bytes.
inline auto as_amqp_bytes(chunk_ptr chunk) -> amqp_bytes_t {
  if (not chunk or chunk->size() == 0) {
    return amqp_empty_bytes;
  }
  // Many RabbitMQ functions take an amqp_bytes_t structure as input.
  // Unfortunately there's not const-preserving equivalent, so we have to bite
  // the const_cast bullet.
  return {
    .len = chunk->size(),
    .bytes = const_cast<std::byte*>(chunk->data()),
  };
}

/// Interprets AMQP bytes as string view.
inline auto as_string_view(amqp_bytes_t bytes) -> std::string_view {
  const auto* ptr = reinterpret_cast<const char*>(bytes.bytes);
  return std::string_view{ptr, std::string_view::size_type{bytes.len}};
}

/// Converts a status code into an error.
inline auto to_error(int status, std::string_view desc = "") -> caf::error {
  if (status == AMQP_STATUS_OK) {
    return {};
  }
  const auto* error_string = amqp_error_string2(status);
  if (desc.empty()) {
    return caf::make_error(ec::unspecified, std::string{error_string});
  }
  return caf::make_error(ec::unspecified,
                         fmt::format("{}: {}", desc, error_string));
}

/// Converts an RPC reply into an error.
inline auto to_error(const amqp_rpc_reply_t& reply) -> caf::error {
  switch (reply.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      break;
    case AMQP_RESPONSE_NONE:
      return caf::make_error(ec::end_of_input, "got EOF from socket");
    case AMQP_RESPONSE_SERVER_EXCEPTION:
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to execute RPC method {}",
                                         reply.reply.id));
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      return caf::make_error(
        ec::unspecified, fmt::format("failed to perform action: {}",
                                     amqp_error_string2(reply.library_error)));
  }
  return {};
}

inline auto parse_url(const record& config_, std::string_view str)
  -> std::optional<record> {
  auto info = amqp_connection_info{};
  auto copy = std::string{str};
  if (amqp_parse_url(copy.data(), &info) != AMQP_STATUS_OK) {
    return std::nullopt;
  }
  auto result = config_;
  if (info.host != nullptr) {
    result["hostname"] = std::string{info.host};
  }
  if (info.port != 0) {
    result["port"] = detail::narrow_cast<uint64_t>(info.port);
  }
  if (info.ssl != 0) {
    result["ssl"] = true;
  }
  if (info.vhost != nullptr) {
    result["vhost"] = std::string{info.vhost};
  }
  if (info.user != nullptr) {
    result["username"] = std::string{info.user};
  }
  if (info.password != nullptr) {
    result["password"] = std::string{info.password};
  }
  return result;
}

/// The configuration for the AMQP engine.
struct amqp_config {
  std::string hostname{"127.0.0.1"};
  uint16_t port{AMQP_PROTOCOL_PORT};
  bool ssl{false};
  std::string vhost{"/"};
  int max_channels{AMQP_DEFAULT_MAX_CHANNELS};
  int frame_size{AMQP_DEFAULT_FRAME_SIZE};
  int heartbeat{AMQP_DEFAULT_HEARTBEAT};
  std::string username{"guest"};
  std::string password{"guest"};
  amqp_sasl_method_enum sasl_method{AMQP_SASL_METHOD_PLAIN};
};

/// An abstraction to perform actions over the AMQP protocol.
///
/// Most of the code is an adaptation of the examples in the repository
/// https://github.com/alanxz/rabbitmq-c.
///
/// A great resource on RabbitMQ is the book by Gavin M. Roy available at
/// https://livebook.manning.com/book/rabbitmq-in-depth/.
class amqp_engine {
public:
  /// Aditional options for starting a consumer.
  struct consume_options {
    uint16_t channel{default_channel};
    std::string_view exchange{default_exchange};
    std::string_view routing_key{default_routing_key};
    std::string_view queue{default_queue};
    bool passive{false};
    bool durable{false};
    bool exclusive{false};
    bool auto_delete{true};
    bool no_local{false};
    bool no_ack{true};
  };

  /// Aditional options for starting a consumer.
  struct publish_options {
    uint16_t channel{default_channel};
    std::string_view exchange{default_exchange};
    std::string_view routing_key{default_routing_key};
    bool mandatory{false};
    bool immediate{false};
  };

  /// Constructs an AMQP engine from a config record.
  static auto make(record settings) -> caf::expected<amqp_engine> {
    amqp_config config;
    if (auto* hostname = get_if<std::string>(&settings, "hostname")) {
      config.hostname = std::move(*hostname);
    }
    if (auto* port = get_if<uint64_t>(&settings, "port")) {
      config.port = detail::narrow_cast<uint16_t>(*port);
    }
    if (auto* ssl = get_if<bool>(&settings, "ssl")) {
      config.ssl = *ssl;
    }
    if (auto* vhost = get_if<std::string>(&settings, "vhost")) {
      config.vhost = std::move(*vhost);
    }
    if (auto* max_channels = get_if<uint64_t>(&settings, "max_channels")) {
      config.max_channels = detail::narrow_cast<int>(*max_channels);
    }
    if (auto* frame_size = get_if<uint64_t>(&settings, "frame_size")) {
      config.frame_size = detail::narrow_cast<int>(*frame_size);
    }
    if (auto* heartbeat = get_if<uint64_t>(&settings, "heartbeat")) {
      config.heartbeat = detail::narrow_cast<int>(*heartbeat);
    }
    if (auto* username = get_if<std::string>(&settings, "username")) {
      config.username = std::move(*username);
    }
    if (auto* password = get_if<std::string>(&settings, "password")) {
      config.password = std::move(*password);
    }
    if (auto* sasl_method = get_if<std::string>(&settings, "sasl_method")) {
      if (*sasl_method == "plain") {
        config.sasl_method = AMQP_SASL_METHOD_PLAIN;
      } else if (*sasl_method == "external") {
        config.sasl_method = AMQP_SASL_METHOD_EXTERNAL;
      } else {
        return caf::make_error(ec::parse_error,
                               fmt::format("invalid SASL method: {}",
                                           *sasl_method));
      }
    }
    return amqp_engine{std::move(config)};
  }

  /// Constructs an AMQP engine from a typed configuration.
  /// @param config The AMQP configuration.
  explicit amqp_engine(amqp_config config)
    : config_{std::move(config)}, conn_{amqp_new_connection()} {
    TENZIR_ASSERT(conn_ != nullptr);
    TENZIR_DEBUG("constructing AMQP engine with the following parameters:");
    TENZIR_DEBUG("- hostname: {}", config_.hostname);
    TENZIR_DEBUG("- port: {}", config_.port);
    TENZIR_DEBUG("- ssl: {}", config_.ssl);
    TENZIR_DEBUG("- vhost: {}", config_.vhost);
    TENZIR_DEBUG("- max_channels: {}", config_.max_channels);
    TENZIR_DEBUG("- frame_size: {}", config_.frame_size);
    TENZIR_DEBUG("- heartbeat: {}", config_.heartbeat);
    TENZIR_DEBUG("- username: {}", config_.username);
    TENZIR_DEBUG("- password: ***");
    TENZIR_DEBUG("- SASL method: {}", static_cast<int>(config_.sasl_method));
    TENZIR_DEBUG("creating new TCP socket");
    if (config_.ssl) {
      socket_ = amqp_ssl_socket_new(conn_);
    } else {
      socket_ = amqp_tcp_socket_new(conn_);
    }
    TENZIR_ASSERT(socket_ != nullptr);
  }

  ~amqp_engine() {
    if (not conn_) {
      return;
    }
    TENZIR_DEBUG("closing AMQP connection");
    auto reply = amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
    if (auto err = to_error(reply); err.valid()) {
      TENZIR_DEBUG(err);
    }
    TENZIR_DEBUG("destroying AMQP connection");
    auto status = amqp_destroy_connection(conn_);
    if (auto err = to_error(status, "failed to destroy AMQP connection");
        err.valid()) {
      TENZIR_WARN(err);
    }
  }

  // Be a move-only handle type.
  amqp_engine(amqp_engine&) = delete;
  auto operator=(amqp_engine&) -> amqp_engine& = delete;
  auto operator=(amqp_engine&&) -> amqp_engine& = delete;

  amqp_engine(amqp_engine&& other) noexcept
    : config_{std::move(other.config_)},
      conn_{other.conn_},
      socket_{other.socket_},
      channel_{other.channel_} {
    other.conn_ = nullptr;
    other.socket_ = nullptr;
    other.channel_ = 0;
  }

  /// Connects to the server by opening a socket and logging in.
  auto connect() -> caf::error {
    if (auto err = open_socket(); err.valid()) {
      return err;
    }
    return login();
  }

  /// Opens a channel.
  /// @param number The channel number.
  auto open(amqp_channel_t number) -> caf::error {
    TENZIR_DEBUG("opening AMQP channel {}", number);
    amqp_channel_open(conn_, number);
    return to_error(amqp_get_rpc_reply(conn_));
  }

  /// Publishes a message as bytes.
  /// @param chunk The message payload.
  /// @param opts The publishing options.
  auto publish(chunk_ptr chunk, const publish_options& opts) -> caf::error {
    TENZIR_DEBUG("publishing {}-byte message with routing key {}",
                 chunk->size(), opts.routing_key);
    auto properties = nullptr;
    auto status = amqp_basic_publish(
      conn_, amqp_channel_t{opts.channel}, as_amqp_bytes(opts.exchange),
      as_amqp_bytes(opts.routing_key), as_amqp_bool(opts.mandatory),
      as_amqp_bool(opts.immediate), properties, as_amqp_bytes(chunk));
    return to_error(status);
  }

  /// Consumes frames from broker, simply for the side effect of processing
  /// heartbeats implicitly. Required if otherwise no interaction with the
  /// broker would occur.
  auto handle_heartbeat(operator_control_plane& ctrl) {
    amqp_frame_t frame;
    // We impose no timeout, either there is something to read or not. Never
    // block!
    struct timeval tv = {0, 0};
    int status;
    if (conn_ == nullptr) {
      return;
    }
    do {
      status = amqp_simple_wait_frame_noblock(conn_, &frame, &tv);
      if (AMQP_STATUS_TIMEOUT != status && AMQP_STATUS_OK != status) {
        diagnostic::warning("unexpected error while processing heartbeats")
          .note("{}", amqp_error_string2(status))
          .emit(ctrl.diagnostics());
        return;
      }
    } while (status == AMQP_STATUS_OK);
  }

  /// Starts a consumer by calling the basic.consume method.
  /// @param opts The consuming options.
  auto start_consumer(const consume_options& opts) -> caf::error {
    TENZIR_DEBUG("declaring queue '{}'", opts.queue);
    auto arguments = amqp_empty_table;
    auto* declare = amqp_queue_declare(
      conn_, amqp_channel_t{opts.channel}, as_amqp_bytes(opts.queue),
      as_amqp_bool(opts.passive), as_amqp_bool(opts.durable),
      as_amqp_bool(opts.exclusive), as_amqp_bool(opts.auto_delete), arguments);
    if (declare == nullptr) {
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to declare queue '{}', "
                                         "passive: {}, durable: {}, exclusive: "
                                         "{}, auto-delete: {}",
                                         opts.queue, opts.passive, opts.durable,
                                         opts.exclusive, opts.auto_delete));
    }
    TENZIR_DEBUG("got queue '{}' with {} messages and {} consumers",
                 as_string_view(declare->queue), declare->message_count,
                 declare->consumer_count);
    if (auto err = to_error(amqp_get_rpc_reply(conn_)); err.valid()) {
      return err;
    }
    auto declared_queue = std::string{as_string_view(declare->queue)};
    TENZIR_DEBUG("binding queue '{}' to exchange '{}' with routing key '{}'",
                 declared_queue, opts.exchange, opts.routing_key);
    amqp_queue_bind(conn_, amqp_channel_t{opts.channel},
                    as_amqp_bytes(declared_queue), as_amqp_bytes(opts.exchange),
                    as_amqp_bytes(opts.routing_key), arguments);
    if (auto err = to_error(amqp_get_rpc_reply(conn_)); err.valid()) {
      return err;
    }
    TENZIR_DEBUG("setting up consume");
    auto consumer_tag = amqp_empty_bytes;
    amqp_basic_consume(conn_, amqp_channel_t{opts.channel},
                       as_amqp_bytes(declared_queue), consumer_tag,
                       as_amqp_bool(opts.no_local), as_amqp_bool(opts.no_ack),
                       as_amqp_bool(opts.exclusive), arguments);
    return to_error(amqp_get_rpc_reply(conn_));
  }

  /// Consumes a message.
  /// @returns The message from the server.
  auto consume(std::optional<std::chrono::microseconds> timeout = {})
    -> caf::expected<chunk_ptr> {
    TENZIR_TRACE("consuming message");
    auto envelope = amqp_envelope_t{};
    amqp_maybe_release_buffers(conn_);
    timeval us{};
    if (timeout) {
      auto secs = std::chrono::duration_cast<std::chrono::seconds>(*timeout);
      us.tv_sec = detail::narrow_cast<time_t>(secs.count());
      us.tv_usec = detail::narrow_cast<suseconds_t>((*timeout - secs).count());
    }
    auto flags = 0;
    auto ret
      = amqp_consume_message(conn_, &envelope, timeout ? &us : nullptr, flags);
    if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
      TENZIR_DEBUG("got message from exchange '{}' on channel {} with routing "
                   "key '{}' having {} bytes",
                   as_string_view(envelope.exchange), envelope.channel,
                   as_string_view(envelope.routing_key),
                   envelope.message.body.len);
      auto result = move_into_chunk(envelope.message.body);
      empty_amqp_pool(&envelope.message.pool);
      amqp_bytes_free(envelope.routing_key);
      amqp_bytes_free(envelope.exchange);
      amqp_bytes_free(envelope.consumer_tag);
      return result;
    }
    // A timeout is no error.
    if (ret.library_error == AMQP_STATUS_TIMEOUT) {
      return chunk_ptr{};
    }
    // Now we're leaving the happy path.
    TENZIR_DEBUG(
      "reply type is {}, library error {} ({})",
      static_cast<std::underlying_type_t<amqp_response_type_enum>>(
        ret.reply_type),
      static_cast<std::underlying_type_t<amqp_status_enum>>(ret.library_error),
      amqp_error_string2(ret.library_error));
    if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
      if (ret.library_error != AMQP_STATUS_UNEXPECTED_STATE) {
        // Likely unrecoverable error, let the retry logic handle this
        return caf::make_error(
          ec::unspecified,
          fmt::format("amqp: {}", amqp_error_string2(ret.library_error)));
      }
      TENZIR_DEBUG("waiting for frame");
      auto frame = amqp_frame_t{};
      auto status = amqp_simple_wait_frame(conn_, &frame);
      if (auto err = to_error(status, "failed to wait for frame");
          err.valid()) {
        return err;
      }
      if (frame.frame_type == AMQP_FRAME_METHOD) {
        switch (frame.payload.method.id) {
          default:
            return caf::make_error(ec::unspecified,
                                   fmt::format("got unexpected method: {}",
                                               frame.payload.method.id));
          case AMQP_BASIC_ACK_METHOD:
            // If we've turned *publisher confirms* on, and we've
            // published a message here is a message being confirmed.
            break;
          case AMQP_BASIC_RETURN_METHOD: {
            // We end up here if a published message couldn't be routed
            // and the mandatory flag was set.
            TENZIR_DEBUG("got mandatory message that couldn't be routed");
            auto message = amqp_message_t{};
            auto ret = amqp_read_message(conn_, frame.channel, &message, 0);
            if (auto err = to_error(ret); err.valid()) {
              return err;
            }
            auto chunk = move_into_chunk(message.body);
            empty_amqp_pool(&message.pool);
            break;
          }
          case AMQP_CHANNEL_CLOSE_METHOD:
            // A `channel.close` method happens when a channel exception occurs.
            // This can happen by publishing to an exchange that doesn't exist.
            //
            // In this case we would need to open another channel, redeclare any
            // queues that were declared auto-delete, and restart any consumers
            // that were attached to the previous channel.
            return caf::make_error(ec::unspecified, "got channel.close");
          case AMQP_CONNECTION_CLOSE_METHOD:
            //  A `connection.close` method happens when a connection exception
            //  occurs. This can happen by trying to use a channel that isn't
            //  open.
            //
            //  In this case the whole connection must be restarted.
            return caf::make_error(ec::unspecified, "got connection.close");
        }
      }
    }
    return chunk_ptr{};
  }

private:
  auto open_socket() -> caf::error {
    TENZIR_DEBUG("opening AMQP socket to {}:{}", config_.hostname,
                 config_.port);
    TENZIR_ASSERT(socket_ != nullptr);
    auto p = detail::narrow_cast<int>(config_.port);
    auto status = amqp_socket_open(socket_, config_.hostname.c_str(), p);
    return to_error(status);
  }

  auto login() -> caf::error {
    auto reply
      = amqp_login(conn_, config_.vhost.c_str(),
                   detail::narrow_cast<int>(config_.max_channels),
                   config_.frame_size, config_.heartbeat, config_.sasl_method,
                   config_.username.c_str(), config_.password.c_str());
    if (auto err = to_error(reply); err.valid()) {
      return err;
    }
    return {};
  }

  amqp_config config_{};
  amqp_connection_state_t conn_{nullptr};
  amqp_socket_t* socket_{nullptr};
  int channel_{0};
};

/// The arguments for the saver and loader.
struct connector_args {
  std::optional<located<uint16_t>> channel;
  std::optional<located<std::string>> routing_key;
  std::optional<located<std::string>> exchange;
  std::optional<located<record>> options;
  std::optional<located<secret>> url;
  location op;

  friend auto inspect(auto& f, connector_args& x) -> bool {
    return f.object(x).fields(f.field("channel", x.channel),
                              f.field("routing_key", x.routing_key),
                              f.field("exchange", x.exchange),
                              f.field("options", x.options),
                              f.field("url", x.url), f.field("op", x.op));
  }
};

/// The arguments for the loader.
struct loader_args : connector_args {
  std::optional<located<std::string>> queue;
  bool passive{false};
  bool durable{false};
  bool exclusive{false};
  bool no_auto_delete{false};
  bool no_local{false};
  bool ack{false};

  friend auto inspect(auto& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("loader_args")
      .fields(f.field("connector_args", static_cast<connector_args&>(x)),
              f.field("queue", x.queue), f.field("passive", x.passive),
              f.field("durable", x.durable), f.field("exclusive", x.exclusive),
              f.field("no_auto_delete", x.no_auto_delete),
              f.field("no_local", x.no_local), f.field("ack", x.ack));
  }
};

/// The arguments for the saver.
struct saver_args : connector_args {
  bool mandatory{false};
  bool immediate{false};

  friend auto inspect(auto& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("connector_args", static_cast<connector_args&>(x)),
              f.field("mandatory", x.mandatory),
              f.field("immediate", x.immediate));
  }
};

auto set_or_fail(record& config, std::string_view key, std::string value,
                 location loc, diagnostic_handler& dh) -> void {
  const auto strings = std::set<std::string_view>{
    "hostname", "vhost", "sasl_method", "username", "password"};
  if (strings.contains(key)) {
    config[key] = std::move(value);
    return;
  }
  if (auto x = from_yaml(value)) {
    config[key] = std::move(*x);
    return;
  }
  diagnostic::error("failed to parse value for key `{}` in key-value pair", key)
    .primary(loc)
    .emit(dh);
}

class rabbitmq_loader final : public crtp_operator<rabbitmq_loader> {
public:
  rabbitmq_loader() = default;

  rabbitmq_loader(loader_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    co_yield {};
    auto& dh = ctrl.diagnostics();
    auto config = config_;
    auto secret_reqs = std::vector<secret_request>{};
    if (args_.options) {
      const auto& loc = args_.options->source;
      for (const auto& [k, v] : args_.options->inner) {
        match(
          v,
          [&](const concepts::arithmetic auto& x) {
            set_or_fail(config, k, fmt::to_string(x), loc, dh);
          },
          [&](const std::string& x) {
            set_or_fail(config, k, x, loc, dh);
          },
          [&](const secret& x) {
            auto req = secret_request{
              x,
              loc,
              [=, &config,
               &dh](const resolved_secret_value& x) -> failure_or<void> {
                TRY(auto str, x.utf8_view(k, loc, dh));
                set_or_fail(config, k, std::string{str}, loc, dh);
                return {};
              },
            };
            secret_reqs.push_back(std::move(req));
          },
          [](const auto&) {
            // validated in `plugin::make`
            TENZIR_UNREACHABLE();
          });
      }
    }
    if (args_.url) {
      auto req = secret_request{
        args_.url.value(),
        [&, loc = args_.url->source](
          const resolved_secret_value& val) -> failure_or<void> {
          TRY(auto str, val.utf8_view("url", loc, dh));
          if (auto cfg = parse_url(config, str)) {
            config = std::move(*cfg);
            return {};
          }
          diagnostic::error("failed to parse AMQP URL")
            .primary(loc)
            .hint("URL must adhere to the following format")
            .hint("amqp://[USERNAME[:PASSWORD]\\@]HOSTNAME[:PORT]/[VHOST]")
            .emit(dh);
          return failure::promise();
        },
      };
      secret_reqs.push_back(std::move(req));
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(secret_reqs));
    auto engine = amqp_engine::make(std::move(config));
    if (not engine) {
      diagnostic::error("failed to construct AMQP engine")
        .primary(args_.op)
        .note("{}", engine.error())
        .emit(ctrl.diagnostics());
      co_return;
    }
    if (auto err = engine->connect(); err.valid()) {
      diagnostic::error("failed to connect to AMQP server")
        .primary(args_.op)
        .note("{}", err)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto channel = args_.channel ? args_.channel->inner : default_channel;
    if (auto err = engine->open(channel); err.valid()) {
      diagnostic::error("failed to open AMQP channel {}", channel)
        .primary(args_.op)
        .note("{}", err)
        .emit(ctrl.diagnostics());
      co_return;
    }
    TENZIR_DEBUG("starting consumer");
    auto routing_key
      = args_.routing_key ? args_.routing_key->inner : default_routing_key;
    auto err = engine->start_consumer({
      .channel = channel,
      .exchange = args_.exchange ? args_.exchange->inner : default_exchange,
      .routing_key = routing_key,
      .queue = args_.queue ? args_.queue->inner : default_queue,
      .passive = args_.passive,
      .durable = args_.durable,
      .exclusive = args_.exclusive,
      .auto_delete = not args_.no_auto_delete,
      .no_local = args_.no_local,
      .no_ack = not args_.ack,
    });
    if (err.valid()) {
      diagnostic::error("failed to start AMQP consumer")
        .primary(args_.op)
        .hint("{}", err)
        .emit(ctrl.diagnostics());
      co_return;
    }
    TENZIR_DEBUG("looping over AMQP frames");
    while (true) {
      if (auto message = engine->consume(500ms)) {
        co_yield std::move(*message);
      } else {
        diagnostic::error("failed to consume message")
          .primary(args_.op)
          .hint("{}", message.error())
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "load_amqp";
  }

  friend auto inspect(auto& f, rabbitmq_loader& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_),
                              f.field("config", x.config_));
  }

private:
  loader_args args_;
  record config_;
};

class rabbitmq_saver final : public crtp_operator<rabbitmq_saver> {
public:
  rabbitmq_saver() = default;

  rabbitmq_saver(saver_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    co_yield {};
    auto& dh = ctrl.diagnostics();
    auto config = config_;
    auto secret_reqs = std::vector<secret_request>{};
    if (args_.options) {
      const auto& loc = args_.options->source;
      for (const auto& [k, v] : args_.options->inner) {
        match(
          v,
          [&](const concepts::arithmetic auto& x) {
            set_or_fail(config, k, fmt::to_string(x), loc, dh);
          },
          [&](const std::string& x) {
            set_or_fail(config, k, x, loc, dh);
          },
          [&](const secret& x) {
            auto req = secret_request{
              x,
              loc,
              [=, &config,
               &dh](const resolved_secret_value& x) -> failure_or<void> {
                TRY(auto str, x.utf8_view(k, loc, dh));
                set_or_fail(config, k, std::string{str}, loc, dh);
                return {};
              },
            };
            secret_reqs.push_back(std::move(req));
          },
          [](const auto&) {
            // validated in `plugin::make`
            TENZIR_UNREACHABLE();
          });
      }
    }
    if (args_.url) {
      auto req = secret_request{
        args_.url.value(),
        [&, loc = args_.url->source](
          const resolved_secret_value& val) -> failure_or<void> {
          TRY(auto str, val.utf8_view("url", loc, dh));
          if (auto cfg = parse_url(config, str)) {
            config = std::move(*cfg);
            return {};
          }
          diagnostic::error("failed to parse AMQP URL")
            .primary(loc)
            .hint("URL must adhere to the following format")
            .hint("amqp://[USERNAME[:PASSWORD]\\@]HOSTNAME[:PORT]/[VHOST]")
            .emit(dh);
          return failure::promise();
        },
      };
      secret_reqs.push_back(std::move(req));
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(secret_reqs));
    auto engine = std::shared_ptr<amqp_engine>{};
    if (auto eng = amqp_engine::make(config)) {
      engine = std::make_shared<amqp_engine>(std::move(*eng));
    } else {
      diagnostic::error(eng.error()).emit(ctrl.diagnostics());
    }
    if (auto err = engine->connect(); err.valid()) {
      diagnostic::error(err).emit(ctrl.diagnostics());
    }
    auto channel = args_.channel ? args_.channel->inner : default_channel;
    if (auto err = engine->open(channel); err.valid()) {
      diagnostic::error(err).emit(ctrl.diagnostics());
    }
    auto opts = amqp_engine::publish_options{
      .channel = channel,
      .exchange = args_.exchange ? args_.exchange->inner : default_exchange,
      .routing_key
      = args_.routing_key ? args_.routing_key->inner : default_routing_key,
      .mandatory = args_.mandatory,
      .immediate = args_.immediate,
    };
    auto heartbeat = try_get<uint64_t>(config, "heartbeat");
    if (heartbeat and *heartbeat and **heartbeat > 0) {
      // If we are requesting heartbeats, we are also responsible to handle
      // the heartbeats we get. If we have long gaps in interaction with the
      // broker, we need to proactively check explicitly if there is something
      // for us. We check 3 times per heartbeat interval, at most once per
      // second.
      auto interval = std::max(uint64_t{1}, **heartbeat / 3);
      TENZIR_DEBUG("using heartbeat interval of {} seconds", interval);
      detail::weak_run_delayed_loop(
        &ctrl.self(), std::chrono::seconds{interval}, [engine, &ctrl] {
          TENZIR_TRACE("processing heartbeats");
          engine->handle_heartbeat(ctrl);
        });
    }
    for (auto chunk : input) {
      if (not chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      if (auto err = engine->publish(chunk, opts); err.valid()) {
        diagnostic::error("failed to publish amqp message")
          .primary(args_.op)
          .note("size: {}", chunk->size())
          .note("channel: {}", opts.channel)
          .note("exchange: {}", opts.exchange)
          .note("routing key: {}", opts.routing_key)
          .hint("{}", err)
          .emit(ctrl.diagnostics());
      }
    }
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "save_amqp";
  }

  friend auto inspect(auto& f, rabbitmq_saver& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_),
                              f.field("config", x.config_));
  }

private:
  saver_args args_;
  record config_;
};

} // namespace

} // namespace tenzir::plugins::amqp
