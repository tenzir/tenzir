//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/adaptive_table_slice_builder.hpp>
#include <vast/arrow_table_slice.hpp>
#include <vast/concept/parseable/numeric/bool.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/defaults.hpp>
#include <vast/detail/assert.hpp>
#include <vast/detail/padded_buffer.hpp>
#include <vast/generator.hpp>
#include <vast/operator_control_plane.hpp>
#include <vast/plugin.hpp>

#include <arrow/record_batch.h>

#include <simdjson.h>

namespace vast::plugins::json {

namespace {

// The simdjson suggests to initialize the padding part to either 0s or spaces.
using json_buffer = detail::padded_buffer<simdjson::SIMDJSON_PADDING, '\0'>;

caf::error
make_generic_parser_err(simdjson::error_code err, std::string_view faulty_json,
                        std::string_view faulty_json_description) {
  return caf::make_error(
    ec::parse_error,
    fmt::format("failed to parse '{}' and skips JSON '{}': {}",
                faulty_json_description, faulty_json, error_message(err)));
}

class doc_parser {
public:
  doc_parser(std::string_view parsed_document, operator_control_plane& ctrl)
    : parsed_document_{parsed_document}, ctrl_{ctrl} {
  }

  auto parse_object(simdjson::ondemand::value v, auto&& field_pusher,
                    size_t depth = 0u) -> void {
    auto obj = v.get_object().value_unsafe();
    for (auto pair : obj) {
      if (pair.error()) {
        report_parse_err(v, "key value pair");
        return;
      }
      auto maybe_key = pair.unescaped_key();
      if (maybe_key.error()) {
        report_parse_err(v, "key in an object");
        return;
      }
      auto key = maybe_key.value_unsafe();
      auto val = pair.value();
      if (val.error()) {
        report_parse_err(val, fmt::format("object value at key {}", key));
        return;
      }
      auto field = field_pusher.push_field(key);
      parse_impl(val.value_unsafe(), field, depth + 1);
    }
  }

private:
  auto report_parse_err(auto& v, std::string description) -> void {
    ctrl_.warn(caf::make_error(
      ec::parse_error,
      fmt::format("unable to parse {} from an input: {} located in line: {}",
                  std::move(description), v.current_location().value_unsafe(),
                  parsed_document_)));
  }

  auto parse_number(simdjson::ondemand::value val, auto& pusher) -> void {
    switch (val.get_number_type().value_unsafe()) {
      case simdjson::ondemand::number_type::floating_point_number: {
        auto result = val.get_double();
        if (result.error()) {
          report_parse_err(val, "a number");
          return;
        }
        pusher.add(result.value_unsafe());
        return;
      }
      case simdjson::ondemand::number_type::signed_integer: {
        auto result = val.get_int64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return;
        }
        pusher.add(result.value_unsafe());
        return;
      }
      case simdjson::ondemand::number_type::unsigned_integer: {
        auto result = val.get_uint64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return;
        }
        pusher.add(result.value_unsafe());
        return;
      }
    }
  }

  auto parse_string(simdjson::ondemand::value val, auto& pusher) -> void {
    auto maybe_str = val.get_string();
    if (maybe_str.error()) {
      report_parse_err(val, "a string");
      return;
    }
    auto str = val.get_string().value_unsafe();
    using namespace parser_literals;
    static constexpr auto parser
      = parsers::time | parsers::duration | parsers::net | parsers::ip;
    data result;
    if (parser(str, result)) {
      pusher.add(make_view(result));
      return;
    }
    // Take the input as-is if nothing worked.
    pusher.add(str);
  }

  auto parse_array(simdjson::ondemand::array arr, auto& pusher, size_t depth)
    -> void {
    auto list = pusher.push_list();
    for (auto element : arr) {
      if (element.error()) {
        report_parse_err(element, "an array element");
        continue;
      }
      parse_impl(element.value_unsafe(), list, depth + 1);
    }
  }

  auto parse_impl(simdjson::ondemand::value val, auto& pusher, size_t depth)
    -> void {
    if (depth > defaults::max_recursion)
      die("nesting too deep in json_parser parse");
    auto type = val.type();
    if (type.error())
      return;
    switch (val.type().value_unsafe()) {
      case simdjson::ondemand::json_type::null:
        return;
      case simdjson::ondemand::json_type::number:
        parse_number(val, pusher);
        return;
      case simdjson::ondemand::json_type::boolean: {
        auto result = val.get_bool();
        if (result.error()) {
          report_parse_err(val, "a boolean value");
          return;
        }
        pusher.add(result.value_unsafe());
        return;
      }
      case simdjson::ondemand::json_type::string:
        parse_string(val, pusher);
        return;
      case simdjson::ondemand::json_type::array:
        parse_array(val.get_array().value_unsafe(), pusher, depth + 1);
        return;
      case simdjson::ondemand::json_type::object:
        parse_object(val, pusher.push_record(), depth + 1);
        return;
    }
  }

  std::string_view parsed_document_;
  operator_control_plane& ctrl_;
};

class line_by_line_parser {
public:
  line_by_line_parser(simdjson::ondemand::parser& parser, json_buffer& buffer,
                      adaptive_table_slice_builder& builder,
                      operator_control_plane& ctrl_plane)
    : parser_{parser},
      buffer_{buffer},
      builder_{builder},
      ctrl_plane_{ctrl_plane} {
  }

  void parse_next_line() {
    auto view = buffer_.view();
    auto bytes_to_parse = view.length();
    auto end = view.find('\n');
    auto str_to_parse
      = std::string_view{view.begin(), end == std::string_view::npos
                                         ? view.end()
                                         : view.begin() + end + 1};
    auto padded_str = simdjson::padded_string{str_to_parse};
    auto doc = parser_.iterate(padded_str);
    buffer_.truncate(bytes_to_parse - str_to_parse.size());

    if (auto err = doc.error()) {
      ctrl_plane_.warn(make_generic_parser_err(err, view, "string"));
      return;
    }

    auto val = doc.get_value();
    if (auto err = val.error()) {
      ctrl_plane_.warn(make_generic_parser_err(err, view, "string"));
      return;
    }
    auto row = builder_.push_row();
    doc_parser{view, ctrl_plane_}.parse_object(val.value_unsafe(), row);
  }

private:
  simdjson::ondemand::parser& parser_;
  json_buffer& buffer_;
  adaptive_table_slice_builder& builder_;
  operator_control_plane& ctrl_plane_;
};

auto parse(simdjson::ondemand::document_stream::iterator doc_it,
           adaptive_table_slice_builder& builder, operator_control_plane& ctrl)
  -> simdjson::error_code {
  // val.error() will inherit all errors from *doc_it and and get_value so no
  // need to check after each operation.
  auto val = (*doc_it).get_value();
  if (auto err = val.error()) {
    return err;
  }
  auto row = builder.push_row();
  doc_parser{doc_it.source(), ctrl}.parse_object(val.value_unsafe(), row);
  return {};
}

} // namespace

class plugin final : public virtual parser_plugin,
                     public virtual printer_plugin {
  auto make_parser(std::span<std::string const> args,
                   generator<chunk_ptr> json_chunk_generator,
                   operator_control_plane& ctrl) const
    -> caf::expected<parser> override {
    if (!args.empty()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("json parser received unexpected "
                                         "arguments: {}",
                                         fmt::join(args, ", ")));
    };
    return [](generator<chunk_ptr> json_chunk_generator,
              operator_control_plane& ctrl) -> generator<table_slice> {
      // TODO: change max table slice size to be fetched from options.
      constexpr auto max_table_slice_rows = defaults::import::table_slice_size;
      auto parser = simdjson::ondemand::parser{};
      auto stream = simdjson::ondemand::document_stream{};
      auto slice_builder = adaptive_table_slice_builder{};
      auto json_to_parse_buffer = json_buffer{};
      for (auto chnk : json_chunk_generator) {
        VAST_ASSERT(chnk);
        if (chnk->size() == 0u) {
          co_yield std::move(slice_builder).finish();
          slice_builder = {};
          continue;
        }
        json_to_parse_buffer.append(
          {reinterpret_cast<const char*>(chnk->data()), chnk->size()});
        auto view = json_to_parse_buffer.view();
        auto err = parser
                     .iterate_many(view.data(), view.length(),
                                   simdjson::ondemand::DEFAULT_BATCH_SIZE)
                     .get(stream);
        if (err) {
          // For the simdjson 3.1 it seems impossible to have an error
          // returned here so it is hard to understand if we can recover from
          // it somehow.
          json_to_parse_buffer.reset();
          ctrl.warn(caf::make_error(ec::parse_error, error_message(err)));
          continue;
        }
        for (auto doc_it = stream.begin(); doc_it != stream.end(); ++doc_it) {
          if (auto err = parse(doc_it, slice_builder, ctrl)) {
            ctrl.warn(caf::make_error(
              ec::parse_error, fmt::format("failed to fully parse '{}' : {}. "
                                           "Some events can be skipped.",
                                           view, error_message(err))));
            continue;
          }
          if (slice_builder.rows() == max_table_slice_rows) {
            co_yield std::move(slice_builder).finish();
            slice_builder = {};
          }
        }
        if (auto trunc = stream.truncated_bytes(); trunc == 0) {
          json_to_parse_buffer.reset();
        }
        // Simdjson returns that more bytes are left unparsed than the
        // input. It is hard to reason what it means that a parser didn't manage
        // to parse more bytes than the input had. These type of return values
        // only seemed to sometimes occur where the input json couldn't be split
        // into documents. We don't have any hint where we should resume the
        // JSON from. We can either skip the whole input or try parsing it line
        // by line, which can still extract some events
        else if (trunc > view.size()) {
          auto line_parser = line_by_line_parser{parser, json_to_parse_buffer,
                                                 slice_builder, ctrl};
          while (json_to_parse_buffer) {
            line_parser.parse_next_line();
            if (slice_builder.rows() == max_table_slice_rows) {
              co_yield std::move(slice_builder).finish();
              slice_builder = {};
            }
          }
        } else {
          json_to_parse_buffer.truncate(trunc);
        }
      }
      if (auto slice = std::move(slice_builder).finish(); slice.rows() > 0u)
        co_yield std::move(slice);
    }(std::move(json_chunk_generator), ctrl);
  }

  auto default_loader(std::span<std::string const>) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"stdin", {}};
  }

  auto make_printer(std::span<std::string const> args, type input_schema,
                    operator_control_plane&) const
    -> caf::expected<printer> override {
    (void)input_schema;
    if (!args.empty()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("json printer received unexpected "
                                         "arguments: {}",
                                         fmt::join(args, ", ")));
    };
    return to_printer([](table_slice slice) -> generator<chunk_ptr> {
      if (slice.rows() == 0) {
        co_return;
      }
      // JSON printer should output NDJSON, see:
      // https://github.com/ndjson/ndjson-spec
      auto printer = vast::json_printer{{.oneline = true}};
      // TODO: Since this printer is per-schema we can write an optimized
      // version of it that gets the schema ahead of time and only expects data
      // corresponding to exactly that schema.
      auto buffer = std::vector<char>{};
      auto resolved_slice = resolve_enumerations(slice);
      auto array
        = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
      auto out_iter = std::back_inserter(buffer);
      for (const auto& row :
           values(caf::get<record_type>(slice.schema()), *array)) {
        VAST_ASSERT_CHEAP(row);
        const auto ok = printer.print(out_iter, *row);
        VAST_ASSERT_CHEAP(ok);
        out_iter = fmt::format_to(out_iter, "\n");
      }
      auto chunk = chunk::make(std::move(buffer));
      co_yield std::move(chunk);
    });
  }

  auto default_saver(std::span<std::string const>) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"stdout", {}};
  }

  auto printer_allows_joining() const -> bool override {
    return true;
  };

  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "json";
  }
};

} // namespace vast::plugins::json

VAST_REGISTER_PLUGIN(vast::plugins::json::plugin)
