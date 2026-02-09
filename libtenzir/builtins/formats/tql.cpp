//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/identifier.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/escapers.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>

#include <cctype>
#include <cstddef>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

namespace tenzir::plugins::tql {
namespace {

// Maximum buffer size to prevent DoS from malformed input (64 MiB).
constexpr auto max_record_size = size_t{64} * 1024 * 1024;

// Maximum nesting depth to prevent stack overflow from deeply nested structures.
constexpr auto max_nesting_depth = size_t{256};

/// Parser for JSON strings that reuses `detail::json_unescape`.
struct json_string_parser : parser_base<json_string_parser> {
  using attribute = std::string;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& result) const -> bool {
    if (f == l || *f != '"') {
      return false;
    }
    auto begin = f;
    auto it = f;
    auto escape = false;
    ++it;
    while (it != l) {
      if (escape) {
        escape = false;
      } else if (*it == '\\') {
        escape = true;
      } else if (*it == '"') {
        constexpr auto skip_result
          = std::is_same_v<unused_type, std::remove_cvref_t<Attribute>>
            || std::is_const_v<std::remove_reference_t<Attribute>>;
        auto decoded = std::string{};
        decoded.reserve(static_cast<size_t>(std::distance(begin, it)));
        auto escaped_begin = begin;
        ++escaped_begin;
        auto out = std::back_inserter(decoded);
        while (escaped_begin != it) {
          if (not detail::json_unescaper(escaped_begin, it, out)) {
            return false;
          }
        }
        if constexpr (not skip_result) {
          result = std::move(decoded);
        }
        ++it;
        f = it;
        return true;
      }
      ++it;
    }
    return false;
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
      if (depth == 0) {
        return std::nullopt;
      }
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
  input = detail::trim_front(input);
  if (input.empty()) {
    return std::nullopt;
  }
  auto parser = tql_data_parser{};
  const auto* f = input.begin();
  const auto* l = input.end();
  data result;
  if (not parser.parse(f, l, result)) {
    diagnostic::warning("failed to parse TQL record").emit(dh);
    return std::nullopt;
  }
  return result;
}

/// Recursively adds data to a multi_series_builder generator.
template <class Generator>
void add_data_to_builder(const data& d, Generator& gen, size_t depth = 0) {
  if (depth > max_nesting_depth) {
    gen.null();
    return;
  }
  auto f = detail::overload{
    [&](caf::none_t) {
      gen.null();
    },
    [&](bool x) {
      gen.data(x);
    },
    [&](int64_t x) {
      gen.data(x);
    },
    [&](uint64_t x) {
      gen.data(x);
    },
    [&](double x) {
      gen.data(x);
    },
    [&](duration x) {
      gen.data(x);
    },
    [&](time x) {
      gen.data(x);
    },
    [&](const std::string& x) {
      gen.data(x);
    },
    [&](const blob& x) {
      gen.data(x);
    },
    [&](const ip& x) {
      gen.data(x);
    },
    [&](const subnet& x) {
      gen.data(x);
    },
    [&](enumeration x) {
      gen.data(x);
    },
    [&](const list& x) {
      auto list_gen = gen.list();
      for (const auto& elem : x) {
        add_data_to_builder(elem, list_gen, depth + 1);
      }
    },
    [&](const record& x) {
      auto rec = gen.record();
      for (const auto& [key, value] : x) {
        auto field = rec.exact_field(key);
        add_data_to_builder(value, field, depth + 1);
      }
    },
    [&](const auto&) {
      // map, pattern, secret - not expected in TQL format
      gen.null();
    },
  };
  match(d, f);
}

void add_record_to_builder(const record& r, multi_series_builder& builder) {
  auto rec = builder.record();
  for (const auto& [key, value] : r) {
    auto field = rec.exact_field(key);
    add_data_to_builder(value, field);
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
    auto d = Describer<ReadTqlArgs, ReadTql>{};
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::tql

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tql::read_tql_plugin)
