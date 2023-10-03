//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/config.hpp>
#include <tenzir/plugin.hpp>

#include <caf/expected.hpp>

#if TENZIR_MACOS
#  include <rabbitmq-c/amqp.h>
#  include <rabbitmq-c/ssl_socket.h>
#  include <rabbitmq-c/tcp_socket.h>
#else
#  include <amqp.h>
#  include <amqp_ssl_socket.h>
#  include <amqp_tcp_socket.h>
#endif

using namespace std::chrono_literals;

namespace tenzir::plugins::rabbitmq {

namespace {

/// The default channel number.
constexpr amqp_channel_t default_channel = 1;

/// The default queue name.
constexpr auto default_exchange = "amq.direct";

/// The default queue name.
constexpr auto default_queue = "tenzir";

/// Assume ownership of the memory and wrap it in a chunk.
/// @param msg The bytes to move into a chunk.
auto move_into_chunk(amqp_bytes_t& bytes) -> chunk_ptr {
  auto deleter = [bytes]() noexcept {
    amqp_bytes_free(bytes);
  };
  return chunk::make(bytes.bytes, bytes.len, deleter);
}

/// Interprets a string view as AMQP bytes.
auto as_amqp_bytes(std::string_view str) -> amqp_bytes_t {
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
  TENZIR_ASSERT(chunk != nullptr);
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
    return caf::make_error(ec::unspecified, error_string);
  return caf::make_error(ec::unspecified,
                         fmt::format("{}: {}", desc, error_string));
}

/// Converts an RPC reply into an error.
auto to_error(const amqp_rpc_reply_t& reply) -> caf::error {
  switch (reply.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      break;
    case AMQP_RESPONSE_NONE:
      return caf::make_error(ec::unspecified, "got EOF from socket");
    case AMQP_RESPONSE_SERVER_EXCEPTION:
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to execute RPC method {}",
                                         reply.reply.id));
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      return caf::make_error(
        ec::unspecified, fmt::format("failed perform action: {}",
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
  /// @param channel The channel number.
  /// @param exchange The name of the exchange.
  /// @param queue The name of the queue ("routing key").
  auto publish(chunk_ptr chunk, amqp_channel_t channel,
               std::string_view exchange, std::string_view queue)
    -> caf::error {
    TENZIR_DEBUG("publish {} bytes to queue {} at channel {}", chunk->size(),
                 queue, channel);
    auto routing_key = as_amqp_bytes(queue);
    auto mandatory = amqp_boolean_t{0};
    auto immediate = amqp_boolean_t{0};
    auto properties = nullptr;
    auto status = amqp_basic_publish(conn_, channel, as_amqp_bytes(exchange),
                                     routing_key, mandatory, immediate,
                                     properties, as_amqp_bytes(chunk));
    return to_error(status);
  }

  // TODO: need a better name for this function.
  auto consume(amqp_channel_t channel, std::string_view exchange,
               std::string_view queue) -> caf::error {
    TENZIR_DEBUG("declaring queue");
    auto passive = amqp_boolean_t{0};
    auto durable = amqp_boolean_t{0};
    auto exclusive = amqp_boolean_t{0};
    auto auto_delete = amqp_boolean_t{1};
    auto arguments = amqp_empty_table;
    auto* declare
      = amqp_queue_declare(conn_, channel, amqp_empty_bytes, passive, durable,
                           exclusive, auto_delete, arguments);
    if (auto err = to_error(amqp_get_rpc_reply(conn_)))
      return err;
    auto declared_queue = std::string{as_string_view(declare->queue)};
    TENZIR_DEBUG("binding queue");
    auto routing_key = as_amqp_bytes(queue);
    amqp_queue_bind(conn_, channel, as_amqp_bytes(declared_queue),
                    as_amqp_bytes(exchange), routing_key, arguments);
    if (auto err = to_error(amqp_get_rpc_reply(conn_)))
      return err;
    TENZIR_DEBUG("setting up consume");
    auto consumer_tag = amqp_empty_bytes;
    auto no_local = amqp_boolean_t{0};
    auto no_ack = amqp_boolean_t{1};
    amqp_basic_consume(conn_, channel, as_amqp_bytes(declared_queue),
                       consumer_tag, no_local, no_ack, exclusive, arguments);
    return to_error(amqp_get_rpc_reply(conn_));
  }

  /// Consumes a message.
  /// @returns The message from the server.
  auto consume_message(std::optional<std::chrono::microseconds> timeout = {})
    -> caf::expected<chunk_ptr> {
    TENZIR_DEBUG("consuming message");
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
    if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION
        && ret.library_error == AMQP_STATUS_UNEXPECTED_STATE) {
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
  std::optional<located<std::string>> queue;
  std::optional<located<std::string>> exchange;
  std::optional<located<std::string>> options;
  std::optional<located<std::string>> url;

  friend auto inspect(auto& f, connector_args& x) -> bool {
    return f.object(x)
      .pretty_name("connector_args")
      .fields(f.field("channel", x.channel), f.field("queue", x.queue),
              f.field("exchange", x.exchange), f.field("options", x.options),
              f.field("url", x.url));
  }
};

class rabbitmq_loader final : public plugin_loader {
public:
  rabbitmq_loader() = default;

  rabbitmq_loader(connector_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    auto engine = amqp_engine::make(config_);
    if (not engine) {
      diagnostic::error("{}", engine.error()).emit(ctrl.diagnostics());
      return std::nullopt;
    }
    if (auto err = engine->connect()) {
      diagnostic::error("{}", err).emit(ctrl.diagnostics());
      return std::nullopt;
    }
    auto make = [&ctrl](connector_args args,
                        amqp_engine engine) mutable -> generator<chunk_ptr> {
      auto channel = args.channel ? args.channel->inner : default_channel;
      if (auto err = engine.open(channel)) {
        diagnostic::error("failed to open AMQP channel {}", channel)
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto exchange = args.exchange ? args.exchange->inner : default_exchange;
      auto queue = args.queue ? args.queue->inner : default_queue;
      if (auto err = engine.consume(channel, exchange, queue)) {
        diagnostic::error("failed to setup AMQP consume")
          .emit(ctrl.diagnostics());
        co_return;
      }
      co_yield {};
      TENZIR_DEBUG("looping over AMQP frames");
      while (true) {
        if (auto message = engine.consume_message(500ms)) {
          co_yield std::move(*message);
        } else {
          diagnostic::error("failed to consume message")
            .hint("{}", message.error())
            .emit(ctrl.diagnostics());
          break;
        }
      }
    };
    return make(args_, std::move(*engine));
  }

  auto to_string() const -> std::string override {
    return {};
  }

  auto name() const -> std::string override {
    return "rabbitmq";
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
  connector_args args_;
  record config_;
};

class rabbitmq_saver final : public plugin_saver {
public:
  rabbitmq_saver() = default;

  rabbitmq_saver(connector_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto engine = amqp_engine::make(config_);
    if (not engine)
      return engine.error();
    if (auto err = engine->connect())
      return err;
    auto channel = args_.channel ? args_.channel->inner : default_channel;
    if (auto err = engine->open(channel))
      return err;
    auto exchange = args_.exchange ? args_.exchange->inner : default_exchange;
    auto queue = args_.queue ? args_.queue->inner : default_queue;
    return [&ctrl, engine = std::make_shared<amqp_engine>(std::move(*engine)),
            channel, exchange = std::move(exchange),
            queue = std::move(queue)](chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0)
        return;
      if (auto err = engine->publish(chunk, channel, exchange, queue))
        diagnostic::error("failed to publish {}-byte message", chunk->size())
          .note("channel: {}", channel)
          .note("exchange: {}", exchange)
          .note("queue: {}", queue)
          .hint("{}", err)
          .emit(ctrl.diagnostics());
      return;
    };
  }

  auto name() const -> std::string override {
    return "rabbitmq";
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
  connector_args args_;
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
    auto [args, config] = parse_args(p);
    return std::make_unique<rabbitmq_loader>(std::move(args),
                                             std::move(config));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto [args, config] = parse_args(p);
    return std::make_unique<rabbitmq_saver>(std::move(args), std::move(config));
  }

  auto parse_args(parser_interface& p) const
    -> std::pair<connector_args, record> {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto args = connector_args{};
    parser.add("-c,--channel", args.channel, "<channel>");
    parser.add("-e,--exchange", args.exchange, "<exchange>");
    parser.add("-q,--queue", args.queue, "<queue>");
    parser.add("-X,--set", args.options, "<key=value>,...");
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
    return "rabbitmq";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::rabbitmq

TENZIR_REGISTER_PLUGIN(tenzir::plugins::rabbitmq::plugin)
