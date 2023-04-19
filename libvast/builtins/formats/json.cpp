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

auto parse_impl(simdjson::ondemand::value val, auto& pusher, size_t depth)
  -> simdjson::error_code;

auto parse_number(simdjson::ondemand::value val, auto& pusher)
  -> simdjson::error_code {
  switch (val.get_number_type().value_unsafe()) {
    case simdjson::ondemand::number_type::floating_point_number: {
      auto result = val.get_double();
      if (result.error())
        return result.error();
      pusher.add(result.value_unsafe());
      return {};
    }
    case simdjson::ondemand::number_type::signed_integer: {
      auto result = val.get_int64();
      if (result.error())
        return result.error();
      pusher.add(result.value_unsafe());
      return {};
    }
    case simdjson::ondemand::number_type::unsigned_integer: {
      auto result = val.get_uint64();
      if (result.error())
        return result.error();
      pusher.add(result.value_unsafe());
      return {};
    }
  }
  die("unhandled json number type in switch statement");
}

auto parse_string(simdjson::ondemand::value val, auto& pusher)
  -> simdjson::error_code {
  auto maybe_str = val.get_string();
  if (maybe_str.error())
    return maybe_str.error();
  auto str = val.get_string().value_unsafe();
  using namespace parser_literals;
  static constexpr auto parser
    = parsers::time | parsers::duration | parsers::net | parsers::ip
      | as<caf::none_t>("nil"_p) | as<caf::none_t>(parsers::ch<'_'>);
  data result;
  if (parser(str, result)) {
    pusher.add(make_view(result));
    return {};
  }
  // Take the input as-is if nothing worked.
  pusher.add(str);
  return {};
}

auto parse_array(simdjson::ondemand::value val, auto& pusher, size_t depth)
  -> simdjson::error_code {
  auto result = val.get_array();
  if (result.error())
    return result.error();
  auto lst = result.value_unsafe();
  auto list = pusher.push_list();
  for (auto element : lst) {
    if (element.error())
      return element.error();
    if (auto parsed_element
        = parse_impl(element.value_unsafe(), list, depth + 1))
      return parsed_element;
  }
  return {};
}

auto parse_object(simdjson::ondemand::value val, auto&& field_pusher,
                  size_t depth) -> simdjson::error_code {
  auto result = val.get_object();
  if (result.error())
    return result.error();
  auto obj = result.value_unsafe();
  for (auto pair : obj) {
    if (pair.error())
      return pair.error();
    auto key = pair.unescaped_key();
    if (key.error())
      return key.error();
    auto val = pair.value();
    if (val.error())
      return val.error();
    auto field = field_pusher.push_field(key.value_unsafe());
    if (auto err = parse_impl(val.value_unsafe(), field, depth + 1))
      return err;
  }
  return {};
}

auto parse_impl(simdjson::ondemand::value val, auto& pusher, size_t depth)
  -> simdjson::error_code {
  if (depth > defaults::max_recursion)
    die("nesting too deep in json_parser parse");
  auto type = val.type();
  if (type.error())
    return type.error();
  // The type can be retrieved successfully while getting the exact value can
  // still return an error. The simdjson only checks the first character of a
  // value to check the type (
  // https://github.com/simdjson/simdjson/blob/f435fddda148bec9e798dd3bdb1988af569d40af/singleheader/simdjson.h#L29098-L29117)
  // E.g a value 32958329532; would yield a double type while getting the value
  // would raise an error.
  switch (val.type().value_unsafe()) {
    case simdjson::ondemand::json_type::null:
      return {};
    case simdjson::ondemand::json_type::number:
      return parse_number(val, pusher);
    case simdjson::ondemand::json_type::boolean: {
      auto result = val.get_bool();
      if (result.error())
        return result.error();
      pusher.add(result.value_unsafe());
      return {};
    }
    case simdjson::ondemand::json_type::string:
      return parse_string(val, pusher);
    case simdjson::ondemand::json_type::array:
      return parse_array(val, pusher, depth + 1);
    case simdjson::ondemand::json_type::object:
      return parse_object(val, pusher.push_record(), depth + 1);
  }
  die("unhandled json object type in switch statement");
}

auto parse(simdjson::simdjson_result<simdjson::ondemand::document_reference> doc,
           adaptive_table_slice_builder& builder) -> simdjson::error_code {
  if (doc.error())
    return doc.error();
  auto val = doc.get_value();
  if (val.error())
    return val.error();
  auto row = builder.push_row();
  if (auto err = parse_object(val.value_unsafe(), row, 0u)) {
    row.cancel();
    return err;
  }
  return {};
}

void emit_warning(operator_control_plane& ctrl, simdjson::error_code err) {
  ctrl.warn(caf::make_error(ec::parse_error, error_message(err)));
}

} // namespace

class plugin final : public virtual parser_plugin,
                     public virtual printer_plugin {
  auto make_parser(std::span<std::string const> args,
                   generator<chunk_ptr> json_chunk_generator,
                   operator_control_plane& ctrl) const
    -> caf::expected<parser> override {
    if (!args.empty()) {
      return caf::make_error(ec::invalid_argument, "unexpected arguments");
    };
    return [](generator<chunk_ptr> json_chunk_generator,
              operator_control_plane& ctrl) -> generator<table_slice> {
      auto parser = simdjson::ondemand::parser{};
      auto stream = simdjson::ondemand::document_stream{};
      auto slice_builder = adaptive_table_slice_builder{};
      // The simdjson suggests to initialize the padding part to either 0s or
      // spaces.
      auto json_to_parse_buffer
        = detail::padded_buffer<simdjson::SIMDJSON_PADDING, '\0'>{};
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
          emit_warning(ctrl, err);
          continue;
        }
        for (auto doc : stream) {
          if (auto err = parse(doc, slice_builder)) {
            emit_warning(ctrl, err);
            continue;
          }
          // TODO: change max table slice size to be fetched from options.
          if (slice_builder.rows() == defaults::import::table_slice_size) {
            co_yield std::move(slice_builder).finish();
            slice_builder = {};
          }
        }
        if (auto trunc = stream.truncated_bytes(); trunc == 0) {
          json_to_parse_buffer.reset();
        } else {
          json_to_parse_buffer.truncate(trunc);
        }
      }
      if (auto slice = std::move(slice_builder).finish(); slice.rows() > 0u)
        co_yield std::move(slice);
    }(std::move(json_chunk_generator), ctrl);
  }

  auto default_loader(std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    (void)args; // TODO
    return {"stdin", {}};
  }

  auto make_printer(std::span<std::string const> args, type input_schema,
                    operator_control_plane&) const
    -> caf::expected<printer> override {
    if (!args.empty()) {
      return caf::make_error(ec::invalid_argument, "unexpected arguments");
    };
    auto input_type = caf::get<record_type>(input_schema);
    return [input_type](table_slice slice) -> generator<chunk_ptr> {
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
      for (const auto& row : values(input_type, *array)) {
        VAST_ASSERT_CHEAP(row);
        const auto ok = printer.print(out_iter, *row);
        VAST_ASSERT_CHEAP(ok);
        out_iter = fmt::format_to(out_iter, "\n");
      }
      auto chunk = chunk::make(std::move(buffer));
      co_yield std::move(chunk);
    };
  }

  auto default_saver(std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    (void)args; // TODO
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
