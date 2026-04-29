//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/json_parser.hpp"

#include "tenzir/option.hpp"
#include "tenzir/try_simdjson.hpp"

#include <cstring>

namespace tenzir::json {

namespace {

// Returns the number of bytes at the end of the view that are part of an
// incomplete UTF-8 sequence. This mirrors simdjson's trim_partial_utf8().
auto count_trailing_partial_utf8(std::string_view view) -> size_t {
  if (view.empty()) {
    return 0;
  }
  const auto* buf = reinterpret_cast<const uint8_t*>(view.data());
  const auto len = view.size();
  // Check for incomplete multi-byte sequences at the end:
  // - 0xC0-0xFF at end: lead byte of 2-4 byte sequence with only 1 byte
  // - 0xE0-0xFF at len-2: lead byte of 3-4 byte sequence with only 2 bytes
  // - 0xF0-0xFF at len-3: lead byte of 4 byte sequence with only 3 bytes
  if (buf[len - 1] >= 0xC0) {
    return 1;
  }
  if (len >= 2 and buf[len - 2] >= 0xE0) {
    return 2;
  }
  if (len >= 3 and buf[len - 3] >= 0xF0) {
    return 3;
  }
  return 0;
}

auto parse_ndjson_lines(ndjson_parser& parser, SimdjsonPaddedBuffer const& buf,
                        size_t begin, size_t end) -> void {
  TENZIR_ASSERT(begin <= end);
  TENZIR_ASSERT(end <= buf.size());
  auto pos = begin;
  while (pos < end) {
    auto const* nl = static_cast<char const*>(std::memchr(
      reinterpret_cast<char const*>(buf.data() + pos), '\n', end - pos));
    auto line_end = nl ? static_cast<size_t>(
                           nl - reinterpret_cast<char const*>(buf.data()) + 1)
                       : end;
    auto line_len = line_end - pos;
    if (line_len > 1 or buf.data()[pos] != std::byte{'\n'}) {
      parser.parse(buf.padded_view(pos, line_len));
      if (parser.abort_requested) {
        return;
      }
    }
    pos = line_end;
  }
}

} // namespace

auto with_surrounding_bytes(diagnostic_builder b, std::string_view source,
                            simdjson::simdjson_result<const char*> loc,
                            size_t window) -> diagnostic_builder {
  if (loc.error() != simdjson::error_code::SUCCESS) {
    return b;
  }
  if (source.empty()) {
    return b;
  }
  const auto* pos = loc.value_unsafe();
  TENZIR_ASSERT_GEQ(pos, source.data());
  TENZIR_ASSERT_LEQ(pos, source.data() + source.size());
  auto offset = static_cast<size_t>(pos - source.data());
  auto start = offset > window ? offset - window : size_t{0};
  auto end = std::min(offset + window, source.size());
  auto view = source.substr(start, end - start);
  auto context = std::string{};
  context.reserve(view.size() + 6);
  if (start > 0) {
    context += "...";
  }
  for (auto c : view) {
    if (c < 32 or c == 127) {
      context += '?';
    } else {
      context += c;
    }
  }
  if (end < source.size()) {
    context += "...";
  }
  auto arrow_col = (start > 0 ? 3u : 0u) + (offset - start);
  b = std::move(b).note("context:\n{}\n{}^", context,
                        std::string(arrow_col, ' '));
  b = std::move(b).note("total buffer size: {}", source.size());
  return b;
}

auto ndjson_parser::parse(simdjson::padded_string_view json_line) -> void {
  ++lines_processed_;
  simdjson::ondemand::document_stream stream;
  if (auto err = this->json_parser
                   .iterate_many(json_line.data(), json_line.size(),
                                 initial_simdjson_batch_size)
                   .get(stream)) {
    diagnostic::warning("{}", error_message(err)).emit(*dh);
    return;
  }
  size_t objects_parsed = 0;
  size_t diags_emitted = 0;
  for (auto doc_it = stream.begin(); doc_it != stream.end();
       ++doc_it, ++objects_parsed) {
    if (auto err = doc_it.error()) {
      auto line = std::string_view{json_line.data(), json_line.size()};
      with_surrounding_bytes(diagnostic::warning("{}", error_message(err))
                               .note("line {}", lines_processed_)
                               .note("skipped invalid JSON at index {}",
                                     doc_it.current_index()),
                             line, line.data() + doc_it.current_index())
        .emit(*dh);
      ++diags_emitted;
      break; // if the iterator itself errors, the document structure is
             // invalid.
    }
    auto doc = *doc_it;
    TENZIR_ASSERT(not doc.current_location().error());
    if (auto err = doc.error()) {
      auto line = std::string_view{json_line.data(), json_line.size()};
      auto loc = doc.current_location();
      const auto* pos = loc.value_unsafe();
      with_surrounding_bytes(diagnostic::warning("{}", error_message(err))
                               .note("line {} column {}", lines_processed_,
                                     pos - line.data())
                               .note("skipped invalid JSON"),
                             line, loc)
        .emit(*dh);
      ++diags_emitted;
      break;
    }
    auto val = doc.get_value();
    if (auto err = val.error()) {
      auto line = std::string_view{json_line.data(), json_line.size()};
      auto loc = doc.current_location();
      const auto* pos = loc.value_unsafe();
      with_surrounding_bytes(diagnostic::warning("{}", error_message(err))
                               .note("line {} column {}", lines_processed_,
                                     pos - line.data())
                               .note("skipped invalid JSON"),
                             line, loc)
        .emit(*dh);
      ++diags_emitted;
      break;
    }
    auto parser = doc_parser{json_line, *dh, lines_processed_};
    auto success = parser.parse_object(val.value_unsafe(), builder.record());
    if (not success) {
      builder.remove_last();
      ++diags_emitted;
      break;
    }
  }
  auto line = std::string_view{json_line.data(), json_line.size()};
  if (objects_parsed == 0 and diags_emitted == 0) {
    with_surrounding_bytes(diagnostic::warning("line did not contain a single "
                                               "valid JSON object")
                             .note("line {}", lines_processed_)
                             .note("skipped invalid JSON"),
                           line, line.data())
      .emit(*dh);
  } else if (objects_parsed > 1) {
    with_surrounding_bytes(
      diagnostic::warning("more than one JSON object in line")
        .note("line {}", lines_processed_)
        .note("encountered a total of {} objects", objects_parsed),
      line, line.data())
      .emit(*dh);
  }
  auto truncated_count = stream.truncated_bytes();
  if (truncated_count > 0 and objects_parsed) {
    with_surrounding_bytes(diagnostic::warning("skipped remaining invalid JSON "
                                               "bytes")
                             .note("line {}", lines_processed_)
                             .note("{} bytes remained", truncated_count)
                             .note("skipped invalid JSON"),
                           line, line.data() + line.size() - truncated_count)
      .emit(*dh);
  }
}

auto ndjson_parser::validate_completion() const -> void {
  // noop, just exists for easy of implementation
}

auto streaming_ndjson_parser::parse_chunk(SimdjsonPaddedBuffer const& data,
                                          std::string_view name,
                                          diagnostic_handler& dh)
  -> std::vector<table_slice> {
  if (data.empty() and partial_.empty()) {
    return {};
  }
  auto find_last_newline
    = [](std::span<std::byte const> bytes) -> Option<size_t> {
    for (auto i = bytes.size(); i > 0; --i) {
      if (bytes[i - 1] == std::byte{'\n'}) {
        return i - 1;
      }
    }
    return None{};
  };
  auto find_first_newline
    = [](std::span<std::byte const> bytes) -> Option<size_t> {
    for (auto i = size_t{0}; i < bytes.size(); ++i) {
      if (bytes[i] == std::byte{'\n'}) {
        return i;
      }
    }
    return None{};
  };
  auto parser = ndjson_parser{std::string{name}, dh, {}};
  auto bytes = data.view();
  if (not partial_.empty()) {
    auto first_newline = find_first_newline(bytes);
    if (not first_newline) {
      partial_.append(bytes);
      return {};
    }
    partial_.append(bytes.subspan(0, *first_newline + 1));
    parse_ndjson_lines(parser, partial_, 0, partial_.size());
    partial_.clear();
    if (parser.abort_requested) {
      return parser.builder.finalize_as_table_slice();
    }
    auto consumed = *first_newline + 1;
    bytes = bytes.subspan(consumed);
    if (bytes.empty()) {
      return parser.builder.finalize_as_table_slice();
    }
  }
  auto last_newline = find_last_newline(bytes);
  if (not last_newline) {
    partial_.append(bytes);
    return parser.builder.finalize_as_table_slice();
  }
  auto complete_size = *last_newline + 1;
  parse_ndjson_lines(parser, data, data.size() - bytes.size(),
                     data.size() - bytes.size() + complete_size);
  if (complete_size < bytes.size()) {
    partial_.assign(bytes.subspan(complete_size));
  }
  return parser.builder.finalize_as_table_slice();
}

auto streaming_ndjson_parser::finish(std::string_view name,
                                     diagnostic_handler& dh)
  -> std::vector<table_slice> {
  if (partial_.empty()) {
    return {};
  }
  auto parser = ndjson_parser{std::string{name}, dh, {}};
  parse_ndjson_lines(parser, partial_, 0, partial_.size());
  partial_.clear();
  return parser.builder.finalize_as_table_slice();
}

auto default_parser::parse(const chunk::view_type& json_chunk) -> void {
  // Whether to retry on a capacity error
  auto retry_capacity_failure = false;
  // How many documents passed the simdjson batch_size.
  // Those documents must be skipped in order to not duplicate events.
  auto completed_documents = size_t{0};
  buffer_.append(
    {reinterpret_cast<const char*>(json_chunk.data()), json_chunk.size()});
  auto view = buffer_.view();
  do {
    retry_capacity_failure = false;
    auto err
      = json_parser.iterate_many(view.data(), view.length(), current_batch_size)
          .get(stream_);
    if (err) {
      // For the simdjson 3.1 it seems impossible to have an error
      // returned here so it is hard to understand if we can recover from
      // it somehow.
      buffer_.reset();
      diagnostic::warning("{}", error_message(err))
        .note("failed to parse")
        .emit(*dh);
      return;
    }
    auto current_document = size_t{};
    for (auto doc_ : stream_) {
      // Skip documents that passed the simdjson batch_size limits previously
      if (current_document < completed_documents) {
        continue;
      }
      ++current_document;
      // doc.error() will inherit all errors from *doc_it and get_value.
      // No need to check after each operation.
      auto doc = doc_.get_value();
      if (auto err = doc.error()) {
        if (err == simdjson::CAPACITY) {
          current_batch_size *= 2;
          retry_capacity_failure = current_batch_size < max_simdjson_batch_size;
          if (retry_capacity_failure) {
            break;
          }
        }
        abort_requested = true;
        with_surrounding_bytes(
          diagnostic::error("{}", error_message(err)).note("found invalid JSON"),
          view, doc.current_location())
          .emit(*dh);
        return;
      }
      TENZIR_ASSERT(not doc.current_location().error());
      ++completed_documents;
      if (arrays_of_objects_) {
        auto arr = doc.value_unsafe().get_array();
        if (arr.error()) {
          abort_requested = true;
          diagnostic::error("expected an array of objects").emit(*dh);
          return;
        }
        for (auto&& elem : arr.value_unsafe()) {
          if (auto err = elem.error()) {
            with_surrounding_bytes(diagnostic::error("{}", error_message(err))
                                     .note("found invalid JSON array"),
                                   view, elem.current_location())
              .emit(*dh);
            return;
          }
          TENZIR_ASSERT(not elem.current_location().error());
          const auto source = std::string_view{
            elem.current_location().value_unsafe(),
            view.data() + view.size(),
          };
          auto row = builder.record();
          auto success
            = doc_parser{source, *dh}.parse_object(elem.value_unsafe(), row);
          if (not success) {
            builder.remove_last();
            // It should be fine to continue here, because at least the array
            // structure we are iterating is valid. That is ensured by the
            // elem.error() check above
            continue;
          }
        }
      } else {
        TENZIR_ASSERT(not doc.current_location().error());
        const auto source = std::string_view{
          doc.current_location().value_unsafe(),
          view.data() + view.size(),
        };
        const auto type = check(doc.type());
        if (type != simdjson::ondemand::json_type::object) {
          auto diag = diagnostic::error("expected an object");
          if (type == simdjson::ondemand::json_type::array) {
            diag
              = std::move(diag).hint("use the `arrays_of_objects=true` option");
          }
          std::move(diag).emit(*dh);
          return;
        }
        auto row = builder.record();
        auto success
          = doc_parser{source, *dh}.parse_object(doc.value_unsafe(), row);
        if (not success) {
          builder.remove_last();
          break;
        }
      }
    }
  } while (retry_capacity_failure);
  handle_truncated_bytes();
}

auto default_parser::validate_completion() -> void {
  if (not buffer_.view().empty()) {
    diagnostic::error("parser input ended with incomplete object").emit(*dh);
    abort_requested = true;
  }
}

auto default_parser::handle_truncated_bytes() -> void {
  auto truncated_bytes = stream_.truncated_bytes();
  auto view = buffer_.view();
  // Likely not needed, but should be harmless. Needs additional
  // investigation in the future.
  if (truncated_bytes > view.size()) {
    abort_requested = true;
    diagnostic::error("detected malformed JSON").emit(*dh);
    return;
  }
  // simdjson's truncated_bytes() is calculated after trimming partial UTF-8
  // sequences from the end of the input. We need to add those bytes back to
  // avoid losing them, which would cause UTF-8 validation errors when the
  // continuation bytes arrive in the next chunk.
  auto partial_utf8_bytes = count_trailing_partial_utf8(view);
  auto bytes_to_keep
    = std::min(truncated_bytes + partial_utf8_bytes, view.size());
  if (bytes_to_keep == 0) {
    buffer_.reset();
    return;
  }
  buffer_.truncate(bytes_to_keep);
}
} // namespace tenzir::json
