//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/json_parser.hpp"

#include "tenzir/try_simdjson.hpp"

namespace tenzir::json {

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
      diagnostic::warning("{}", error_message(err))
        .note("line {}", lines_processed_)
        .note("skipped invalid JSON at index {}", doc_it.current_index())
        .emit(*dh);
      ++diags_emitted;
      break; // if the iterator itself errors, the document structure is
             // invalid.
    }
    auto doc = *doc_it;
    TENZIR_ASSERT(not doc.current_location().error());
    if (auto err = doc.error()) {
      diagnostic::warning("{}", error_message(err))
        .note("line {} column {}", lines_processed_,
              doc.current_location().value_unsafe() - json_line.data())
        .note("skipped invalid JSON")
        .emit(*dh);
      ++diags_emitted;
      break;
    }
    auto val = doc.get_value();
    if (auto err = val.error()) {
      diagnostic::warning("{}", error_message(err))
        .note("line {} column {}", lines_processed_,
              doc.current_location().value_unsafe() - json_line.data())
        .note("skipped invalid JSON")
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
  if (objects_parsed == 0 and diags_emitted == 0) {
    diagnostic::warning("line did not contain a single valid JSON object")
      .note("line {}", lines_processed_)
      .note("skipped invalid JSON")
      .emit(*dh);
  } else if (objects_parsed > 1) {
    diagnostic::warning("more than one JSON object in line")
      .note("line {}", lines_processed_)
      .note("encountered a total of {} objects", objects_parsed)
      .emit(*dh);
  }
  auto truncated_count = stream.truncated_bytes();
  if (truncated_count > 0 and objects_parsed) {
    diagnostic::warning("skipped remaining invalid JSON bytes")
      .note("line {}", lines_processed_)
      .note("{} bytes remained", truncated_count)
      .note("skipped invalid JSON")
      .emit(*dh);
  }
}

auto ndjson_parser::validate_completion() const -> void {
  // noop, just exists for easy of implementation
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
        diagnostic::error("{}", error_message(err))
          .note("found invalid JSON")
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
            diagnostic::error("{}", error_message(err))
              .note("found invalid JSON array")
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
  if (truncated_bytes == 0) {
    buffer_.reset();
    return;
  }
  // Likely not needed, but should be harmless. Needs additional
  // investigation in the future.
  if (truncated_bytes > buffer_.view().size()) {
    abort_requested = true;
    diagnostic::error("detected malformed JSON").emit(*dh);
    return;
  }
  buffer_.truncate(truncated_bytes);
}
} // namespace tenzir::json
