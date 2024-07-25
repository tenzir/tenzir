//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/config.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/plugin.hpp>

#include <caf/expected.hpp>

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
auto move_into_chunk(amqp_bytes_t& bytes) -> chunk_ptr {
  auto deleter = [bytes]() noexcept {
    amqp_bytes_free(bytes);
  };
  return chunk::make(bytes.bytes, bytes.len, deleter);
}

auto as_amqp_bool(bool x) -> amqp_boolean_t {
  return x ? 1 : 0;
}

/// Interprets a string view as AMQP bytes.
auto as_amqp_bytes(std::string_view str) -> amqp_bytes_t {
  if (str.empty())
    return amqp_empty_bytes;
  // Many RabbitMQ functions take an amqp_bytes_t structure as input.
  // Unfortunately there's not const-preserving equivalent, so we have to bite
  // the const_cast bullet.
  return {
    .len = str.size(),
    .bytes = const_cast<char*>(str.data()),
  };
}

/// Interprets a chunk as AMQP bytes.
auto as_amqp_bytes(chunk_ptr chunk) -> amqp_bytes_t {
  if (not chunk or chunk->size() == 0)
    return amqp_empty_bytes;
  // Many RabbitMQ functions take an amqp_bytes_t structure as input.
  // Unfortunately there's not const-preserving equivalent, so we have to bite
  // the const_cast bullet.
  return {
    .len = chunk->size(),
    .bytes = const_cast<std::byte*>(chunk->data()),
  };
}

/// Interprets AMQP bytes as string view.
auto as_string_view(amqp_bytes_t bytes) -> std::string_view {
  const auto* ptr = reinterpret_cast<const char*>(bytes.bytes);
  return std::string_view{ptr, std::string_view::size_type{bytes.len}};
}

/// Converts a status code into an error.
auto to_error(int status, std::string_view desc = "") -> caf::error {
  if (status == AMQP_STATUS_OK)
    return {};
  const auto* error_string = amqp_error_string2(status);
  if (desc.empty())
    return caf::make_error(ec::unspecified, std::string{error_string});
  return caf::make_error(ec::unspecified,
                         fmt::format("{}: {}", desc, error_string));
}

/// Converts an RPC reply into an error.
auto to_error(const amqp_rpc_reply_t& reply) -> caf::error {
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
    if (auto* hostname = get_if<std::string>(&settings, "hostname"))
      config.hostname = std::move(*hostname);
    if (auto* port = get_if<uint64_t>(&settings, "port"))
      config.port = detail::narrow_cast<uint16_t>(*port);
    if (auto* ssl = get_if<bool>(&settings, "ssl"))
      config.ssl = *ssl;
    if (auto* vhost = get_if<std::string>(&settings, "vhost"))
      config.vhost = std::move(*vhost);
    if (auto* max_channels = get_if<uint64_t>(&settings, "max_channels"))
      config.max_channels = detail::narrow_cast<int>(*max_channels);
    if (auto* frame_size = get_if<uint64_t>(&settings, "frame_size"))
      config.frame_size = detail::narrow_cast<int>(*frame_size);
    if (auto* heartbeat = get_if<uint64_t>(&settings, "heartbeat"))
      config.heartbeat = detail::narrow_cast<int>(*heartbeat);
    if (auto* username = get_if<std::string>(&settings, "username"))
      config.username = std::move(*username);
    if (auto* password = get_if<std::string>(&settings, "password"))
      config.password = std::move(*password);
    if (auto* sasl_method = get_if<std::string>(&settings, "sasl_method")) {
      if (*sasl_method == "plain")
        config.sasl_method = AMQP_SASL_METHOD_PLAIN;
      else if (*sasl_method == "external")
        config.sasl_method = AMQP_SASL_METHOD_EXTERNAL;
      else
        return caf::make_error(ec::parse_error,
                               fmt::format("invalid SASL method: {}",
                                           *sasl_method));
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
    if (config_.ssl)
      socket_ = amqp_ssl_socket_new(conn_);
    else
      socket_ = amqp_tcp_socket_new(conn_);
    TENZIR_ASSERT(socket_ != nullptr);
  }

  ~amqp_engine() {
    if (not conn_)
      return;
    TENZIR_DEBUG("closing AMQP connection");
    auto reply = amqp_connection_close(conn_, AMQP_REPLY_SUCCESS);
    if (auto err = to_error(reply))
      TENZIR_DEBUG(err);
    TENZIR_DEBUG("destroying AMQP connection");
    auto status = amqp_destroy_connection(conn_);
    if (auto err = to_error(status, "failed to destroy AMQP connection"))
      TENZIR_WARN(err);
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
    if (auto err = open_socket())
      return err;
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
    if (conn_ == nullptr)
      return;
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
    if (declare == nullptr)
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to declare queue '{}', "
                                         "passive: {}, durable: {}, exclusive: "
                                         "{}, auto-delete: {}",
                                         opts.queue, opts.passive, opts.durable,
                                         opts.exclusive, opts.auto_delete));
    TENZIR_DEBUG("got queue '{}' with {} messages and {} consumers",
                 as_string_view(declare->queue), declare->message_count,
                 declare->consumer_count);
    if (auto err = to_error(amqp_get_rpc_reply(conn_)))
      return err;
    auto declared_queue = std::string{as_string_view(declare->queue)};
    TENZIR_DEBUG("binding queue '{}' to exchange '{}' with routing key '{}'",
                 declared_queue, opts.exchange, opts.routing_key);
    amqp_queue_bind(conn_, amqp_channel_t{opts.channel},
                    as_amqp_bytes(declared_queue), as_amqp_bytes(opts.exchange),
                    as_amqp_bytes(opts.routing_key), arguments);
    if (auto err = to_error(amqp_get_rpc_reply(conn_)))
      return err;
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
    if (ret.library_error == AMQP_STATUS_TIMEOUT)
      return chunk_ptr{};
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
        return caf::make_error(ec::unspecified,
                               fmt::format("amqp: {}",
                               amqp_error_string2(ret.library_error)));
      }
      TENZIR_DEBUG("waiting for frame");
      auto frame = amqp_frame_t{};
      auto status = amqp_simple_wait_frame(conn_, &frame);
      if (auto err = to_error(status, "failed to wait for frame"))
        return err;
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
            if (auto err = to_error(ret))
              return err;
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
    if (auto err = to_error(reply))
      return err;
    return {};
  }

  amqp_config config_{};
  amqp_connection_state_t conn_{nullptr};
  amqp_socket_t* socket_{nullptr};
  int channel_{0};
};

/// The arguments for the saver and loader.
struct connector_args {
  std::optional<located<unsigned short>> channel;
  std::optional<located<std::string>> routing_key;
  std::optional<located<std::string>> exchange;
  std::optional<located<std::string>> options;
  std::optional<located<std::string>> url;
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
      .fields(f.field("channel", x.channel), f.field("queue", x.queue),
              f.field("routing_key", x.routing_key),
              f.field("exchange", x.exchange), f.field("options", x.options),
              f.field("url", x.url), f.field("passive", x.passive),
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
      .fields(f.field("channel", x.channel),
              f.field("routing_key", x.routing_key),
              f.field("exchange", x.exchange), f.field("options", x.options),
              f.field("url", x.url), f.field("mandatory", x.mandatory),
              f.field("immediate", x.immediate));
  }
};

class rabbitmq_loader final : public plugin_loader {
public:
  rabbitmq_loader() = default;

  rabbitmq_loader(loader_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto make = [](operator_control_plane& ctrl, loader_args args,
                   record config) mutable -> generator<chunk_ptr> {
      auto engine = amqp_engine::make(config);
      if (not engine) {
        diagnostic::error("failed to construct AMQP engine")
          .note("{}", engine.error())
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (auto err = engine->connect()) {
        diagnostic::error("failed to connect to AMQP server")
          .note("{}", err)
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto channel = args.channel ? args.channel->inner : default_channel;
      if (auto err = engine->open(channel)) {
        diagnostic::error("failed to open AMQP channel {}", channel)
          .note("{}", err)
          .emit(ctrl.diagnostics());
        co_return;
      }
      TENZIR_DEBUG("starting consumer");
      auto routing_key
        = args.routing_key ? args.routing_key->inner : default_routing_key;
      auto err = engine->start_consumer({
        .channel = channel,
        .exchange = args.exchange ? args.exchange->inner : default_exchange,
        .routing_key = routing_key,
        .queue = args.queue ? args.queue->inner : default_queue,
        .passive = args.passive,
        .durable = args.durable,
        .exclusive = args.exclusive,
        .auto_delete = not args.no_auto_delete,
        .no_local = args.no_local,
        .no_ack = not args.ack,
      });
      if (err) {
        diagnostic::error("failed to start AMQP consumer")
          .hint("{}", err)
          .emit(ctrl.diagnostics());
        co_return;
      }
      co_yield {};
      TENZIR_DEBUG("looping over AMQP frames");
      while (true) {
        if (auto message = engine->consume(500ms)) {
          co_yield std::move(*message);
        } else {
          diagnostic::error("failed to consume message")
            .hint("{}", message.error())
            .emit(ctrl.diagnostics());
          break;
        }
      }
    };
    return make(ctrl, args_, config_);
  }

  auto name() const -> std::string override {
    return "amqp";
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, rabbitmq_loader& x) -> bool {
    return f.object(x)
      .pretty_name("rabbitmq_loader")
      .fields(f.field("args", x.args_), f.field("config", x.config_));
  }

private:
  loader_args args_;
  record config_;
};

class rabbitmq_saver final : public plugin_saver {
public:
  rabbitmq_saver() = default;

  rabbitmq_saver(saver_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto engine = std::shared_ptr<amqp_engine>{};
    if (auto eng = amqp_engine::make(config_)) {
      engine = std::make_shared<amqp_engine>(std::move(*eng));
    } else {
      return eng.error();
    }
    if (auto err = engine->connect()) {
      return err;
    }
    auto channel = args_.channel ? args_.channel->inner : default_channel;
    if (auto err = engine->open(channel))
      return err;
    auto opts = amqp_engine::publish_options{
      .channel = channel,
      .exchange = args_.exchange ? args_.exchange->inner : default_exchange,
      .routing_key
      = args_.routing_key ? args_.routing_key->inner : default_routing_key,
      .mandatory = args_.mandatory,
      .immediate = args_.immediate,
    };
    auto heartbeat = try_get<uint64_t>(config_, "heartbeat");
    if (heartbeat and *heartbeat and **heartbeat > 0) {
      // If we are requesting heartbeats, we are also responsible to handle the
      // heartbeats we get. If we have long gaps in interaction with the broker,
      // we need to proactively check explicitly if there is something for us.
      // We check 3 times per heartbeat interval, at most once per second.
      auto interval = std::max(uint64_t{1}, **heartbeat / 3);
      TENZIR_DEBUG("using heartbeat interval of {} seconds", interval);
      detail::weak_run_delayed_loop(&ctrl.self(),
        std::chrono::seconds{interval}, [engine, &ctrl] {
        TENZIR_TRACE("processing heartbeats");
        engine->handle_heartbeat(ctrl);
      });
    }
    return [&ctrl, engine, opts = std::move(opts)](chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0)
        return;
      if (auto err = engine->publish(chunk, opts))
        diagnostic::error("failed to publish {}-byte message", chunk->size())
          .note("channel: {}", opts.channel)
          .note("exchange: {}", opts.exchange)
          .note("routing key: {}", opts.routing_key)
          .hint("{}", err)
          .emit(ctrl.diagnostics());
      return;
    };
  }

  auto name() const -> std::string override {
    return "amqp";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, rabbitmq_saver& x) -> bool {
    return f.object(x)
      .pretty_name("rabbitmq_saver")
      .fields(f.field("args", x.args_), f.field("config", x.config_));
  }

private:
  saver_args args_;
  record config_;
};

class plugin final : public virtual loader_plugin<rabbitmq_loader>,
                     public virtual saver_plugin<rabbitmq_saver> {
public:
  auto initialize(const record& config, const record& /* global_config */)
    -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto [args, config] = parse_args<loader_args>(p);
    return std::make_unique<rabbitmq_loader>(std::move(args),
                                             std::move(config));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto [args, config] = parse_args<saver_args>(p);
    return std::make_unique<rabbitmq_saver>(std::move(args), std::move(config));
  }

  template <class Args>
  auto parse_args(parser_interface& p) const -> std::pair<Args, record> {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = Args{};
    parser.add("-c,--channel", args.channel, "<channel>");
    parser.add("-e,--exchange", args.exchange, "<exchange>");
    parser.add("-r,--routing_key", args.routing_key, "<key>");
    parser.add("-X,--set", args.options, "<key=value>,...");
    if constexpr (std::is_same_v<Args, loader_args>) {
      parser.add("-q,--queue", args.queue, "<queue>");
      parser.add("--passive", args.passive);
      parser.add("--durable", args.durable);
      parser.add("--exclusive", args.exclusive);
      parser.add("--no-auto-delete", args.no_auto_delete);
      parser.add("--no-local", args.no_local);
      parser.add("--ack", args.ack);
    } else if constexpr (std::is_same_v<Args, saver_args>) {
      parser.add("--mandatory", args.mandatory);
      parser.add("--immediate", args.immediate);
    }
    parser.add(args.url, "<url>");
    parser.parse(p);
    auto config = config_;
    if (args.url) {
      if (auto cfg = parse_url(args.url->inner))
        config = std::move(*cfg);
      else
        diagnostic::error("failed to parse AMQP URL")
          .primary(args.url->source)
          .hint("URL must adhere to the following format")
          .hint("amqp://[USERNAME[:PASSWORD]\\@]HOSTNAME[:PORT]/[VHOST]")
          .throw_();
    }
    if (args.options) {
      std::vector<std::pair<std::string, std::string>> kvps;
      if (not parsers::kvp_list(args.options->inner, kvps))
        diagnostic::error("invalid list of key=value pairs")
          .primary(args.options->source)
          .throw_();
      // For all string keys, we don't attempt automatic conversion.
      auto strings = std::set<std::string>{"hostname", "vhost", "sasl_method",
                                           "username", "password"};
      for (auto& [key, value] : kvps) {
        if (strings.contains(key))
          config[key] = std::move(value);
        else if (auto x = from_yaml(value))
          config[key] = std::move(*x);
        else
          diagnostic::error("failed to parse value in key-value pair")
            .primary(args.options->source)
            .note("value: {}", value)
            .throw_();
      }
    }
    return {std::move(args), std::move(config)};
  }

  auto parse_url(std::string_view str) const -> std::optional<record> {
    auto info = amqp_connection_info{};
    auto copy = std::string{str};
    if (amqp_parse_url(copy.data(), &info) != AMQP_STATUS_OK)
      return std::nullopt;
    auto result = config_;
    if (info.host != nullptr)
      result["hostname"] = std::string{info.host};
    if (info.port != 0)
      result["port"] = detail::narrow_cast<uint64_t>(info.port);
    if (info.ssl != 0)
      result["ssl"] = true;
    if (info.vhost != nullptr)
      result["vhost"] = std::string{info.vhost};
    if (info.user != nullptr)
      result["username"] = std::string{info.user};
    if (info.password != nullptr)
      result["password"] = std::string{info.password};
    return result;
  }

  auto name() const -> std::string override {
    return "amqp";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::amqp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::amqp::plugin)
