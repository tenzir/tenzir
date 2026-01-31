//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/string/quoted_string.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/identifier.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>

#include <cctype>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace tenzir::plugins::tql {
namespace {

// Maximum buffer size to prevent DoS from malformed input (64 MiB).
constexpr auto max_record_size = size_t{64} * 1024 * 1024;

// Maximum nesting depth to prevent stack overflow from deeply nested structures.
constexpr auto max_nesting_depth = size_t{256};

/// Parser for JSON-escaped strings that produces unescaped output.
/// This handles the standard JSON escape sequences: \\, \", \/, \b, \f, \n,
/// \r, \t, and \uXXXX.
struct json_string_parser : parser_base<json_string_parser> {
  using attribute = std::string;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& result) const -> bool {
    // Check if we should skip result modification (unused_type or const)
    constexpr bool skip_result
      = std::is_same_v<unused_type, std::remove_cvref_t<Attribute>>
        || std::is_const_v<std::remove_reference_t<Attribute>>;

    if (f == l || *f != '"') {
      return false;
    }
    ++f; // Skip opening quote
    // Reserve capacity to minimize reallocations (escaped strings are shorter)
    if constexpr (not skip_result) {
      result.reserve(static_cast<size_t>(std::distance(f, l)));
    }
    while (f != l && *f != '"') {
      if (*f == '\\') {
        // Handle escape sequence
        ++f; // Skip backslash
        if (f == l) {
          return false;
        }
        char escaped = *f;
        ++f;
        if constexpr (not skip_result) {
          switch (escaped) {
            case '\\':
              result.push_back('\\');
              break;
            case '"':
              result.push_back('"');
              break;
            case '/':
              result.push_back('/');
              break;
            case 'b':
              result.push_back('\b');
              break;
            case 'f':
              result.push_back('\f');
              break;
            case 'r':
              result.push_back('\r');
              break;
            case 'n':
              result.push_back('\n');
              break;
            case 't':
              result.push_back('\t');
              break;
            case 'u': {
              // Unicode escape: \uXXXX - simplified handling
              if (l - f < 4) {
                return false;
              }
              // For now, only handle \u00XX (single-byte)
              char hex[5] = {*f, *(f + 1), *(f + 2), *(f + 3), '\0'};
              f += 4;
              if (hex[0] == '0' && hex[1] == '0') {
                // Convert hex to byte
                auto hex_to_nibble = [](char c) -> int {
                  if (c >= '0' && c <= '9') {
                    return c - '0';
                  }
                  if (c >= 'a' && c <= 'f') {
                    return c - 'a' + 10;
                  }
                  if (c >= 'A' && c <= 'F') {
                    return c - 'A' + 10;
                  }
                  return -1;
                };
                int hi = hex_to_nibble(hex[2]);
                int lo = hex_to_nibble(hex[3]);
                if (hi < 0 || lo < 0) {
                  return false;
                }
                result.push_back(static_cast<char>((hi << 4) | lo));
              } else {
                // Leave multi-byte unicode as-is (literal \uXXXX)
                result.push_back('\\');
                result.push_back('u');
                result.append(hex, 4);
              }
              break;
            }
            default:
              // Unknown escape - invalid
              return false;
          }
        }
      } else {
        if constexpr (not skip_result) {
          result.push_back(*f);
        }
        ++f;
      }
    }
    if (f == l || *f != '"') {
      return false;
    }
    ++f; // Skip closing quote
    return true;
  }
};

/// TQL-specific data parser that handles the output format of write_tql.
/// Key differences from the generic data_parser:
/// - Records use {field: value} syntax (not <...>)
/// - Maps are not supported
/// - Blobs use b"..." syntax
/// - Field names can be identifiers or quoted strings
struct tql_data_parser : parser_base<tql_data_parser> {
  using attribute = data;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& a) const -> bool {
    static auto p = make<Iterator>();
    return p(f, l, a);
  }

private:
  template <class Iterator>
  static auto make() {
    using namespace parser_literals;
    rule<Iterator, data> p;
    auto ws = ignore(*parsers::space);
    auto x = ws >> ref(p) >> ws;

    // JSON-style string parser for proper escape handling
    auto json_str = json_string_parser{};

    // Blob parser: b"..." - parses b prefix followed by quoted string
    // and converts to blob type
    auto blob_parser = ('b' >> json_str)->*[](const std::string& s) -> data {
      auto b = blob{};
      b.reserve(s.size());
      for (char c : s) {
        b.push_back(static_cast<std::byte>(c));
      }
      return b;
    };

    // Field name: identifier OR quoted string
    auto field_name = parsers::identifier | json_str;

    // Named field: ws field_name ws : x
    auto named_field = ws >> field_name >> ws >> ':' >> x;

    // Trailing comma pattern (same as data_parser)
    auto trailing_comma = ~(',' >> ws);

    // TQL record: {field: value, ...} using same pattern as data_parser
    auto tql_record
      = '{' >> ~parse_as<record>(named_field % ',') >> trailing_comma >> '}';

    // TQL list: [value, ...] using same pattern as data_parser
    auto tql_list = '[' >> ~(x % ',') >> trailing_comma >> ']';

    // Order matters: subnet before ip, ip before duration
    // This ensures that "3d::" is parsed as IP, not duration followed by "::"
    // clang-format off
    p = parsers::net
      | parsers::ip
      | parsers::time
      | parsers::duration
      | parsers::number  // handles int64, uint64, double
      | parsers::boolean
      | blob_parser      // Must be before json_str to match b"..."
      | json_str         // JSON-escaped strings
      | tql_list
      | tql_record
      | parsers::null
      ;
    // clang-format on
    return p;
  }
};

/// Finds the end of a complete TQL record in the input buffer.
/// Returns the position after the closing brace, or npos if incomplete.
/// Handles nested braces and strings correctly.
auto find_record_end(std::string_view input) -> std::optional<size_t> {
  if (input.size() > max_record_size) {
    return std::nullopt; // Too large
  }
  size_t depth = 0;
  bool in_string = false;
  bool escape = false;
  for (size_t i = 0; i < input.size(); ++i) {
    char c = input[i];
    if (escape) {
      escape = false;
      continue;
    }
    if (c == '\\' && in_string) {
      escape = true;
      continue;
    }
    if (c == '"') {
      in_string = not in_string;
      continue;
    }
    if (in_string) {
      continue;
    }
    if (c == '{' || c == '[') {
      ++depth;
      if (depth > max_nesting_depth) {
        return std::nullopt; // Too deeply nested
      }
    } else if (c == '}' || c == ']') {
      --depth;
      if (depth == 0) {
        return i + 1; // Position after closing brace
      }
    }
  }
  return std::nullopt; // Incomplete record
}

/// Parse a complete TQL record from the buffer.
auto parse_tql_record(std::string_view input, diagnostic_handler& dh)
  -> std::optional<data> {
  auto parser = tql_data_parser{};
  const auto* f = input.begin();
  const auto* l = input.end();
  data result;
  // Skip leading whitespace
  while (f != l && std::isspace(static_cast<unsigned char>(*f)) != 0) {
    ++f;
  }
  if (f == l) {
    return std::nullopt;
  }
  if (not parser.parse(f, l, result)) {
    diagnostic::warning("failed to parse TQL record").emit(dh);
    return std::nullopt;
  }
  return result;
}

/// Recursively adds data to a multi_series_builder.
/// The depth parameter tracks recursion depth to prevent stack overflow.
void add_data_to_object(const data& d,
                        detail::multi_series_builder::object_generator& obj,
                        size_t depth = 0);
void add_data_to_list(const data& d,
                      detail::multi_series_builder::list_generator& list_gen,
                      size_t depth = 0);

void add_data_to_object(const data& d,
                        detail::multi_series_builder::object_generator& obj,
                        size_t depth) {
  if (depth > max_nesting_depth) {
    obj.null();
    return;
  }
  auto f = detail::overload{
    [&](caf::none_t) {
      obj.null();
    },
    [&](bool x) {
      obj.data(x);
    },
    [&](int64_t x) {
      obj.data(x);
    },
    [&](uint64_t x) {
      obj.data(x);
    },
    [&](double x) {
      obj.data(x);
    },
    [&](duration x) {
      obj.data(x);
    },
    [&](time x) {
      obj.data(x);
    },
    [&](const std::string& x) {
      obj.data(x);
    },
    [&](const blob& x) {
      obj.data(x);
    },
    [&](const ip& x) {
      obj.data(x);
    },
    [&](const subnet& x) {
      obj.data(x);
    },
    [&](enumeration x) {
      obj.data(x);
    },
    [&](const list& x) {
      auto list_gen = obj.list();
      for (const auto& elem : x) {
        add_data_to_list(elem, list_gen, depth + 1);
      }
    },
    [&](const record& x) {
      auto rec = obj.record();
      for (const auto& [key, value] : x) {
        auto field = rec.exact_field(key);
        add_data_to_object(value, field, depth + 1);
      }
    },
    [&](const auto&) {
      // map, pattern, secret - not expected in TQL format
      obj.null();
    },
  };
  match(d, f);
}

void add_data_to_list(const data& d,
                      detail::multi_series_builder::list_generator& list_gen,
                      size_t depth) {
  if (depth > max_nesting_depth) {
    list_gen.null();
    return;
  }
  auto f = detail::overload{
    [&](caf::none_t) {
      list_gen.null();
    },
    [&](bool x) {
      list_gen.data(x);
    },
    [&](int64_t x) {
      list_gen.data(x);
    },
    [&](uint64_t x) {
      list_gen.data(x);
    },
    [&](double x) {
      list_gen.data(x);
    },
    [&](duration x) {
      list_gen.data(x);
    },
    [&](time x) {
      list_gen.data(x);
    },
    [&](const std::string& x) {
      list_gen.data(x);
    },
    [&](const blob& x) {
      list_gen.data(x);
    },
    [&](const ip& x) {
      list_gen.data(x);
    },
    [&](const subnet& x) {
      list_gen.data(x);
    },
    [&](enumeration x) {
      list_gen.data(x);
    },
    [&](const list& x) {
      auto nested = list_gen.list();
      for (const auto& elem : x) {
        add_data_to_list(elem, nested, depth + 1);
      }
    },
    [&](const record& x) {
      auto rec = list_gen.record();
      for (const auto& [key, value] : x) {
        auto field = rec.exact_field(key);
        add_data_to_object(value, field, depth + 1);
      }
    },
    [&](const auto&) {
      list_gen.null();
    },
  };
  match(d, f);
}

void add_record_to_builder(const record& r, multi_series_builder& builder) {
  auto rec = builder.record();
  for (const auto& [key, value] : r) {
    auto field = rec.exact_field(key);
    add_data_to_object(value, field);
  }
}

// Empty args struct for read_tql (no arguments needed)
struct ReadTqlArgs {};

class ReadTql final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadTql(ReadTqlArgs /*args*/) {
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not input || input->size() == 0) {
      co_return;
    }
    // Lazily construct the builder on first use (it needs diagnostic_handler)
    if (not builder_) {
      builder_.emplace(multi_series_builder::options{}, ctx);
    }
    // Append new data to buffer
    buffer_.append(reinterpret_cast<const char*>(input->data()), input->size());
    // Check buffer size limit
    if (buffer_.size() - buffer_offset_ > max_record_size) {
      diagnostic::error("input buffer exceeds maximum size of 64 MiB").emit(ctx);
      co_return;
    }
    // Process complete records from buffer
    co_await process_buffer(push, ctx, false);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx) -> Task<void> override {
    // Ensure builder exists even if no data was processed
    if (not builder_) {
      builder_.emplace(multi_series_builder::options{}, ctx);
    }
    // Process any remaining complete records in buffer
    co_await process_buffer(push, ctx, true);
    // Finalize and yield remaining slices
    for (auto& slice : builder_->finalize_as_table_slice()) {
      co_await push(std::move(slice));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    // Note: The multi_series_builder state is not serialized. Records that
    // have been parsed but not yet yielded (via yield_ready_as_table_slice)
    // may be lost on restore. This is acceptable because yield_ready is called
    // after each parsed record, so any pending state is minimal.
    // Before serializing, compact the buffer to remove already-processed data.
    compact_buffer();
    serde("buffer", buffer_);
  }

private:
  /// Compacts the buffer by removing already-processed data at the front.
  void compact_buffer() {
    if (buffer_offset_ > 0) {
      buffer_.erase(0, buffer_offset_);
      buffer_offset_ = 0;
    }
  }

  /// Returns a view of the unprocessed portion of the buffer.
  auto buffer_view() const -> std::string_view {
    return std::string_view{buffer_}.substr(buffer_offset_);
  }

  /// Advances the buffer offset, compacting periodically to avoid unbounded
  /// memory growth. The compaction threshold is set at half the buffer size.
  void advance_buffer(size_t n) {
    buffer_offset_ += n;
    // Compact when offset exceeds half the buffer size
    if (buffer_offset_ > buffer_.size() / 2) {
      compact_buffer();
    }
  }

  /// Processes complete records from the buffer.
  /// If is_final is true, emits a warning for incomplete trailing records.
  auto process_buffer(Push<table_slice>& push, OpCtx& ctx, bool is_final)
    -> Task<void> {
    while (buffer_offset_ < buffer_.size()) {
      // Skip leading whitespace
      while (
        buffer_offset_ < buffer_.size()
        && std::isspace(static_cast<unsigned char>(buffer_[buffer_offset_]))
             != 0) {
        ++buffer_offset_;
      }
      auto view = buffer_view();
      if (view.empty()) {
        break;
      }
      // Find end of record
      auto end = find_record_end(view);
      if (not end) {
        if (is_final && not view.empty()) {
          diagnostic::warning("incomplete record at end of input").emit(ctx);
        }
        break;
      }
      // Parse the complete record
      auto record_str = view.substr(0, *end);
      auto parsed = parse_tql_record(record_str, ctx);
      if (parsed) {
        if (const auto* r = try_as<record>(*parsed)) {
          add_record_to_builder(*r, *builder_);
        } else {
          diagnostic::warning("expected record at top level, got other type")
            .emit(ctx);
        }
      }
      // Advance past the processed record
      advance_buffer(*end);
      // Yield any ready slices
      for (auto& slice : builder_->yield_ready_as_table_slice()) {
        co_await push(std::move(slice));
      }
    }
  }

  std::string buffer_;
  size_t buffer_offset_ = 0;
  std::optional<multi_series_builder> builder_;
};

class read_tql_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_tql";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadTqlArgs, ReadTql>{
      "https://docs.tenzir.com/operators/read_tql"};
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::tql

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tql::read_tql_plugin)
