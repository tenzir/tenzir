//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async/task.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/plugin/avro.hpp>
#include <tenzir/try.hpp>
#include <tenzir/uuid.hpp>

#include <folly/Uri.h>
#include <folly/coro/Task.h>

#include <deque>
#include <unordered_map>

namespace tenzir::plugins::kafka {

struct AvroEnvelope {
  std::string schema_path;
  size_t payload_offset = 0;
};

/// The header takes precedence, including when its contents are malformed.
inline auto avro_envelope(std::span<std::byte const> payload,
                          Option<std::span<std::byte const>> header)
  -> Result<AvroEnvelope, std::string> {
  auto bytes = header ? *header : payload;
  if (bytes.empty()) {
    return Err{"missing Confluent schema identifier"};
  }
  auto size = size_t{0};
  auto path = std::string{};
  switch (std::to_integer<uint8_t>(bytes.front())) {
    case 0: {
      size = 5;
      if (bytes.size() < size) {
        return Err{"truncated Confluent schema ID"};
      }
      auto id = uint32_t{0};
      for (auto byte : bytes.subspan(1, 4)) {
        id = (id << 8) | std::to_integer<uint8_t>(byte);
      }
      path = fmt::format("/schemas/ids/{}", id);
      break;
    }
    case 1: {
      size = 17;
      if (bytes.size() < size) {
        return Err{"truncated Confluent schema GUID"};
      }
      auto guid = uuid{std::span<std::byte const, 16>{bytes.data() + 1, 16}};
      path = fmt::format("/schemas/guids/{}", guid);
      break;
    }
    default:
      return Err{"unsupported Confluent schema identifier version"};
  }
  if (header and bytes.size() != size) {
    return Err{"unexpected trailing bytes in Confluent Avro schema header"};
  }
  return AvroEnvelope{std::move(path), header ? 0 : size};
}

/// Owned by the single resolver task. Compiled schemas are immutable and shared
/// with CPU workers, so concurrent messages never duplicate a registry lookup.
class AvroRegistry {
public:
  AvroRegistry(Box<HttpPool> pool, std::string base_path,
               std::vector<http::Header> headers,
               AvroDecoderPlugin const& plugin,
               folly::Executor::KeepAlive<> cpu_executor)
    : pool_{std::move(pool)},
      base_path_{std::move(base_path)},
      headers_{std::move(headers)},
      plugin_{plugin},
      cpu_executor_{std::move(cpu_executor)} {
  }

  auto resolve(std::string const& path)
    -> Task<Result<Arc<AvroDecoder>, std::string>> {
    if (auto it = cache_.find(path); it != cache_.end()) {
      co_return it->second;
    }
    auto state = FetchState{};
    CO_TRY(auto schema, co_await fetch(path, state, 0));
    CO_TRY(auto decoder, co_await folly::coro::co_withExecutor(
                           cpu_executor_, compile(schema, state.references)));
    if (cache_.size() == 128) {
      cache_.erase(order_.front());
      order_.pop_front();
    }
    order_.push_back(path);
    cache_.emplace(path, decoder);
    co_return decoder;
  }

private:
  struct FetchState {
    std::vector<std::pair<std::string, std::string>> references;
    std::unordered_map<std::string, std::string> names;
    // An empty value marks a lookup still on the recursion stack.
    std::unordered_map<std::string, Option<std::string>> schemas;
    size_t remaining = 16 * 1024 * 1024;
  };

  auto
  compile(std::string const& schema,
          std::vector<std::pair<std::string, std::string>> const& references)
    -> Task<Result<Arc<AvroDecoder>, std::string>> {
    co_return plugin_.compile(schema, references);
  }

  auto fetch(std::string const& path, FetchState& state, size_t depth)
    -> Task<Result<std::string, std::string>> {
    if (auto it = state.schemas.find(path); it != state.schemas.end()) {
      if (not it->second) {
        co_return Err{"cyclic Avro schema references are not supported"};
      }
      co_return *it->second;
    }
    if (depth >= 32 or state.schemas.size() >= 129) {
      co_return Err{"Avro schema references exceed the supported limit"};
    }
    state.schemas.emplace(path, None{});
    auto body = std::string{};
    auto oversized = false;
    auto remaining_before_request = state.remaining;
    auto response_result = co_await pool_->stream_get(
      base_path_ + path, headers_,
      HttpStreamCallbacks{
        .on_headers = {},
        .on_body = [&](std::string chunk) -> Task<bool> {
          if (chunk.size() > state.remaining) {
            oversized = true;
            co_return true;
          }
          state.remaining -= chunk.size();
          body += chunk;
          co_return false;
        },
        .on_retry =
          [&] {
            body.clear();
            state.remaining = remaining_before_request;
          },
      });
    co_await folly::coro::co_safe_point;
    if (oversized) {
      co_return Err{"Avro registry response exceeds 16 MiB"};
    }
    CO_TRY(auto response, std::move(response_result));
    if (not response.is_status_success()) {
      co_return Err{fmt::format("schema registry returned HTTP {} for {}",
                                response.status_code, path)};
    }
    auto parsed = from_json(body);
    if (not parsed or not is<record>(*parsed)) {
      co_return Err{"schema registry returned an invalid JSON object"};
    }
    auto const& object = as<record>(*parsed);
    if (auto it = object.find("schemaType");
        it != object.end() and it->second != data{"AVRO"}) {
      co_return Err{"schema registry returned a non-Avro schema"};
    }
    auto schema = object.find("schema");
    if (schema == object.end() or not is<std::string>(schema->second)) {
      co_return Err{"schema registry response is missing a schema string"};
    }
    if (auto it = object.find("references"); it != object.end()) {
      auto const* refs = try_as<list>(&it->second);
      if (not refs) {
        co_return Err{"invalid schema registry references"};
      }
      for (auto const& value : *refs) {
        auto const* ref = try_as<record>(&value);
        if (not ref) {
          co_return Err{"invalid schema registry reference"};
        }
        auto name = ref->find("name");
        auto subject = ref->find("subject");
        auto version = ref->find("version");
        if (name == ref->end() or not is<std::string>(name->second)
            or subject == ref->end() or not is<std::string>(subject->second)
            or version == ref->end() or not is<int64_t>(version->second)
            or as<int64_t>(version->second) <= 0) {
          co_return Err{"invalid schema registry reference"};
        }
        auto encoded = std::string{};
        for (auto c : as<std::string>(subject->second)) {
          auto byte = static_cast<unsigned char>(c);
          if ((byte >= 'a' and byte <= 'z') or (byte >= 'A' and byte <= 'Z')
              or (byte >= '0' and byte <= '9') or byte == '-' or byte == '_'
              or byte == '.' or byte == '~') {
            encoded += c;
          } else {
            encoded += fmt::format("%{:02X}", byte);
          }
        }
        auto const& ref_name = as<std::string>(name->second);
        auto ref_path = fmt::format("/subjects/{}/versions/{}?deleted=true",
                                    encoded, as<int64_t>(version->second));
        if (auto existing = state.names.find(ref_name);
            existing != state.names.end()) {
          if (existing->second != ref_path) {
            co_return Err{fmt::format(
              "conflicting Avro schema references for name '{}'", ref_name)};
          }
          if (not state.schemas.at(ref_path)) {
            co_return Err{"cyclic Avro schema references are not supported"};
          }
          continue;
        }
        if (state.names.size() >= 128) {
          co_return Err{"Avro schema references exceed the supported limit"};
        }
        // Reserve the mapping before descending so transitive references
        // cannot bind this name to a different subject or version.
        state.names.emplace(ref_name, ref_path);
        CO_TRY(auto definition, co_await fetch(ref_path, state, depth + 1));
        state.references.emplace_back(ref_name, std::move(definition));
      }
    }
    auto result = as<std::string>(schema->second);
    state.schemas.at(path) = result;
    co_return result;
  }

  Box<HttpPool> pool_;
  std::string base_path_;
  std::vector<http::Header> headers_;
  AvroDecoderPlugin const& plugin_;
  folly::Executor::KeepAlive<> cpu_executor_;
  std::unordered_map<std::string, Arc<AvroDecoder>> cache_;
  std::deque<std::string> order_;
};

} // namespace tenzir::plugins::kafka
