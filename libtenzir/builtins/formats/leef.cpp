//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/pusher.hpp>
#include <tenzir/box.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/coding.hpp>
#include <tenzir/detail/line_range.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/error.hpp>
#include <tenzir/module.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/event_builder.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/read_detection.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>
#include <tenzir/view3.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <memory>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

// The Log Event Extended Format (LEEF) is an event representation that has been
// popularized by IBM QRadar. The official documentation at
// https://www.ibm.com/docs/en/dsm?topic=overview-leef-event-components provides
// more details into the spec.

// TODO:
// - Parse the devTime attribute (and devTimeFormat) and assign it to the event
//   timestamp. An option for this behavior should exist.
// - Use the *Label field suffix as field name, e.g., Foo="42"\tFooLabel="xxx"
//   should be translated into xxx=42 by the parser. An option for this behavior
//   should exist.
// - Stretch: consider a timezone option in case devTimeFormat doesn't contain
//   one.
namespace tenzir::plugins::leef {

namespace {

// TODO: it's unlclear whether that's correct. There is not much info out there
// in the internet that tells us how to do this properly.
/// Unescapes LEEF string data containing \r, \n, \\, and \=.
auto unescape(std::string_view::iterator begin, std::string_view::iterator end,
              std::back_insert_iterator<std::string> out)
  -> std::string_view::iterator {
  TENZIR_UNUSED(end);
  TENZIR_ASSERT_EXPENSIVE(*std::prev(begin) == '\\');
  TENZIR_ASSERT_EXPENSIVE(begin < end);
  switch (*begin) {
    case 'n': {
      out = '\n';
      return ++begin;
    }
    case 'r': {
      out = '\n';
      return ++begin;
    }
    case 't': {
      out = '\t';
      return ++begin;
    }
    case '=': {
      out = '=';
      return ++begin;
    }
    case '\\': {
      out = '\\';
      return ++begin;
    }
    default: {
      return begin;
    }
  }
  TENZIR_UNREACHABLE();
  return begin;
}

/// Parses a LEEF delimiter.
auto parse_delimiter(std::string_view field) -> std::variant<char, diagnostic> {
  if (field.empty()) {
    return diagnostic::warning("got empty delimiter")
      .note("LEEF v2.0 requires a delimiter specification")
      .hint("delimiter must be a single character or start with 'x' or '0x'")
      .done();
  }
  if (field.starts_with("x") or field.starts_with("0x")) {
    // Spec: "The hex value can be represented by the prefix 0x or x, followed
    // by a series of 1-4 characters (0-9A-Fa-f)."
    // Me: WTH should 3 hex characters represent? I get 1. And 2. Also 4. But 3?
    auto i = field.find('x');
    TENZIR_ASSERT(i != std::string_view::npos);
    auto hex = field.substr(i + 1);
    for (auto h : hex) {
      if (std::isxdigit(h) == 0) {
        return diagnostic::warning("invalid hex delimiter: {}", field)
          .hint("hex delimiters with 'x' or '0x' require subsequent hex chars")
          .done();
      }
    }
    switch (hex.size()) {
      default:
        return diagnostic::warning("wrong hex delimiter size: {}", hex.size())
          .hint("need 1 or 2 hex chars")
          .done();
      case 1:
        return detail::hex_to_byte('0', hex[0]);
      case 2:
        return detail::hex_to_byte(hex[0], hex[1]);
      // TODO: address this only once a user ever gets such a weird log.
      case 3:
      case 4:
        return diagnostic::warning("wrong number of hex delimiters: {}",
                                   hex.size())
          .note("cannot interpret 3 or 4 characters in a meaningful way")
          .hint("need 1 or 2 hex chars")
          .done();
    }
  } else if (field.size() > 1) {
    return diagnostic::warning("invalid non-hex delimiter")
      .hint("expected a single character, but got {}", field.size())
      .done();
  }
  return field[0];
}

/// Parses the LEEF attributes field as a sequence of key-value pairs.
auto parse_attribute_values(char delimiter, std::string_view attributes,
                            detail::quoting_escaping_policy const& quoting,
                            auto&& emit_pair) -> Option<diagnostic> {
  while (not attributes.empty()) {
    auto attr_end = quoting.find_not_in_quotes(attributes, delimiter);
    /// We greedily accept more than one consecutive separator
    while (attr_end < attributes.size() - 1
           and attributes[attr_end + 1] == delimiter) {
      ++attr_end;
    }
    const auto attribute = attributes.substr(0, attr_end);
    auto sep_pos = quoting.find_not_in_quotes(attribute, '=', 0, true);
    if (sep_pos == 0) {
      return diagnostic::warning("missing key before separator in attributes")
        .note("attribute was `{}`", attribute)
        .done();
    }
    if (sep_pos == attribute.npos) {
      return diagnostic::warning("missing key-value separator in attribute")
        .note("attribute was `{}`", attribute)
        .done();
    }
    auto key = attribute.substr(0, sep_pos);
    auto value
      = quoting.unquote_unescape(detail::trim(attribute.substr(sep_pos + 1)));
    emit_pair(key, std::move(value));
    if (attr_end != attributes.npos) {
      attributes.remove_prefix(attr_end + 1);
    } else {
      break;
    }
  }
  return {};
}

auto parse_attributes(char delimiter, std::string_view attributes, auto builder,
                      detail::quoting_escaping_policy const& quoting)
  -> Option<diagnostic> {
  return parse_attribute_values(
    delimiter, attributes, quoting,
    [&](std::string_view key, std::string value) {
      if constexpr (detail::multi_series_builder::has_unflattened_field<
                      decltype(builder)>) {
        builder.unflattened_field(key).data_unparsed(std::move(value));
      } else {
        auto field = builder.field(key);
        auto parsed = detail::data_builder::best_effort_parser(value);
        if (parsed) {
          field.data(*parsed);
        } else {
          field.data(std::move(value));
        }
      }
    });
}

struct ParsedHeader {
  std::vector<std::string> fields;
  char delimiter = '\t';
};

auto parse_header(std::string_view line) -> variant<ParsedHeader, diagnostic> {
  // We first need to find out whether we are LEEF 1.0 or 2.0. The latter has
  // one additional top-level component.
  auto num_fields = 0u;
  if (not line.starts_with("LEEF:")) {
    return diagnostic::warning("invalid LEEF event")
      .hint("LEEF events start with LEEF:$VERSION|...")
      .done();
  }
  auto pipe = line.find('|');
  if (pipe == std::string_view::npos) {
    return diagnostic::warning("invalid LEEF event")
      .note("could not find a pipe (|) that separates LEEF metadata")
      .done();
  }
  auto colon = line.find(':');
  TENZIR_ASSERT(colon != std::string_view::npos);

  const auto leef_version = line.substr(colon + 1, pipe - colon - 1);
  if (leef_version == "1.0") {
    num_fields = 5;
  } else if (leef_version == "2.0") {
    num_fields = 6;
  } else {
    return diagnostic::warning("unsupported LEEF version: {}", leef_version)
      .hint("only 1.0 and 2.0 are valid values")
      .done();
  }
  auto fields = detail::split_escaped(line, "|", "\\", num_fields);
  if (fields.size() != num_fields + 1) {
    return diagnostic::warning("LEEF {} requires at least {} fields",
                               leef_version, num_fields + 1)
      .note("got {} fields", fields.size())
      .done();
  }
  auto delimiter = '\t';
  if (leef_version == "2.0") {
    auto delim = parse_delimiter(fields[5]);
    if (auto const* c = try_as<char>(delim)) {
      TENZIR_DEBUG("parsed LEEF delimiter: {:#04x}", *c);
      delimiter = *c;
    } else {
      return std::move(as<diagnostic>(delim));
    }
  }
  return ParsedHeader{std::move(fields), delimiter};
}

[[nodiscard]] auto parse_line(std::string_view line, auto& builder,
                              detail::quoting_escaping_policy const& quoting)
  -> Option<diagnostic> {
  auto parsed = parse_header(line);
  auto* header = try_as<ParsedHeader>(parsed);
  if (not header) {
    return std::move(as<diagnostic>(parsed));
  }
  auto& fields = header->fields;
  auto r = builder.record();
  r.field("leef_version").data(fields[0].substr(5));
  r.field("vendor").data(std::move(fields[1]));
  r.field("product_name").data(std::move(fields[2]));
  r.field("product_version").data(std::move(fields[3]));
  r.field("event_class_id").data(std::move(fields[4]));

  auto d = parse_attributes(header->delimiter, fields.back(),
                            r.field("attributes").record(), quoting);
  if (d) {
    builder.remove_last();
    return d;
  }
  return {};
}

struct ReadLeefArgs {
  multi_series_builder::options msb_options;
  location operator_location = location::unknown;
  OptimizationArgs<opt::Order> optimization = {};
};

class ReadLeef final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadLeef(ReadLeefArgs args) : args_{std::move(args)} {
    if (args_.msb_options.settings.default_schema_name.empty()) {
      args_.msb_options.settings.default_schema_name = "leef.event";
    }
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    quoting_ = detail::quoting_escaping_policy{.unescape_operation = unescape};
    dh_.emplace(std::in_place, ctx.dh(), [this](diagnostic d) {
      if (args_.operator_location) {
        auto replaced_unknown_location = false;
        for (auto& annotation : d.annotations) {
          if (annotation.source) {
            continue;
          }
          annotation.source = args_.operator_location;
          replaced_unknown_location = true;
        }
        if (not replaced_unknown_location and d.annotations.empty()) {
          d.annotations.emplace(d.annotations.begin(), true, "",
                                args_.operator_location);
        }
      }
      d.notes.emplace(d.notes.begin(), diagnostic_note_kind::note,
                      fmt::format("line {}", line_counter_));
      return d;
    });
    args_.msb_options.settings.ordered
      = args_.optimization.order == EventOrder::ordered;
    msb_ = multi_series_builder{args_.msb_options, *dh_};
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    TENZIR_ASSERT(msb_);
    co_await pusher_.push(msb_->yield_ready_as_table_slice(), push);
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(msb_);
    auto& dh = **dh_;
    auto const* begin = reinterpret_cast<char const*>(input->data());
    auto const* const end = begin + input->size();
    auto ready = series_builder::YieldReadyResult{};
    if (ended_on_carriage_return_ and *begin == '\n') {
      ++begin;
    }
    ended_on_carriage_return_ = false;
    for (auto const* current = begin; current != end; ++current) {
      if (*current != '\n' and *current != '\r') {
        continue;
      }
      if (buffer_.empty()) {
        process_line({begin, current}, dh);
      } else {
        buffer_.append(begin, current);
        process_line(buffer_, dh);
        buffer_.clear();
      }
      ready.merge(msb_->yield_ready_as_table_slice());
      if (*current == '\r') {
        if (current + 1 == end) {
          ended_on_carriage_return_ = true;
        } else if (*(current + 1) == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    buffer_.append(begin, end);
    ready.merge(msb_->yield_ready_as_table_slice());
    co_await pusher_.push(std::move(ready), push);
  }

  auto finalize(Push<table_slice>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    TENZIR_ASSERT(msb_);
    if (not buffer_.empty()) {
      process_line(buffer_, **dh_);
      buffer_.clear();
    }
    for (auto& slice : msb_->finalize_as_table_slice()) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    TENZIR_ASSERT(msb_);
    for (auto& slice : msb_->finalize_as_table_slice()) {
      co_await push(std::move(slice));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("ended_on_carriage_return", ended_on_carriage_return_);
    serde("line_counter", line_counter_);
  }

private:
  auto process_line(std::string_view line, diagnostic_handler& dh) -> void {
    ++line_counter_;
    if (line.empty()) {
      TENZIR_DEBUG("LEEF parser ignored empty line");
      return;
    }
    auto d = parse_line(line, *msb_, quoting_);
    if (d) {
      dh.emit(std::move(*d));
    }
  }

  ReadLeefArgs args_;
  std::string buffer_;
  bool ended_on_carriage_return_ = false;
  size_t line_counter_ = 0;
  detail::quoting_escaping_policy quoting_;
  Option<Box<transforming_diagnostic_handler>> dh_;
  Option<multi_series_builder> msb_;
  SeriesPusher pusher_;
};

/// Validate attributes before opening an event, so a malformed line leaves no
/// partial record in the batch.
auto parse_event(std::string_view line, nova::EventBuilder& builder,
                 detail::quoting_escaping_policy const& quoting)
  -> Option<diagnostic> {
  auto parsed = parse_header(line);
  auto* header = try_as<ParsedHeader>(parsed);
  if (not header) {
    return std::move(as<diagnostic>(parsed));
  }
  auto attributes = std::vector<std::pair<std::string_view, std::string>>{};
  auto diag
    = parse_attribute_values(header->delimiter, header->fields.back(), quoting,
                             [&](std::string_view key, std::string value) {
                               attributes.emplace_back(key, std::move(value));
                             });
  if (diag) {
    return diag;
  }
  auto const& fields = header->fields;
  auto row = builder.event();
  row.field("leef_version").data(std::string_view{fields[0]}.substr(5));
  row.field("vendor").data(std::string_view{fields[1]});
  row.field("product_name").data(std::string_view{fields[2]});
  row.field("product_version").data(std::string_view{fields[3]});
  row.field("event_class_id").data(std::string_view{fields[4]});
  auto record = row.field("attributes").record();
  for (auto const& [key, value] : attributes) {
    record.field(key).data_unparsed(value);
  }
  return {};
}

class ReadLeefEvents final : public Operator<chunk_ptr, nova::Events> {
public:
  explicit ReadLeefEvents(ReadLeefArgs args)
    : args_{std::move(args)}, timeout_{args_.msb_options.settings.timeout} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    quoting_ = detail::quoting_escaping_policy{.unescape_operation = unescape};
    dh_.emplace(std::in_place, ctx.dh(), [this](diagnostic d) {
      if (args_.operator_location and not d.has_location()) {
        d.annotations.emplace_back(true, std::string{},
                                   args_.operator_location);
      }
      if (diagnostic_line_) {
        d.notes.emplace(d.notes.begin(), diagnostic_note_kind::note,
                        fmt::format("line {}", *diagnostic_line_));
      }
      return d;
    });
    auto settings = nova::event_builder_settings(args_.msb_options);
    settings.infer_unparsed_under = "attributes";
    if (settings.default_schema_name.empty()) {
      settings.default_schema_name = "leef.event";
    }
    auto builder = nova::EventBuilder::make(std::move(settings), **dh_);
    if (builder) {
      builder_ = std::move(builder).unwrap();
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return builder_ ? OperatorState::normal : OperatorState::done;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await timeout_.wait();
    co_return {};
  }

  auto process_task(Any, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    co_await maybe_emit_ready(push);
  }

  auto process(chunk_ptr input, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    if (not builder_) {
      co_return;
    }
    if (not input or input->size() == 0) {
      co_await maybe_emit_ready(push);
      co_return;
    }
    auto const* begin = reinterpret_cast<char const*>(input->data());
    auto const* const end = begin + input->size();
    if (ended_on_carriage_return_ and *begin == '\n') {
      ++begin;
    }
    ended_on_carriage_return_ = false;
    for (auto const* current = begin; current != end; ++current) {
      if (*current != '\n' and *current != '\r') {
        continue;
      }
      if (buffer_.empty()) {
        process_line({begin, current});
      } else {
        buffer_.append(begin, current);
        process_line(buffer_);
        buffer_.clear();
      }
      if (static_cast<size_t>(builder_->length())
          >= args_.msb_options.settings.desired_batch_size) {
        co_await emit_finished(push);
      }
      if (*current == '\r') {
        if (current + 1 == end) {
          ended_on_carriage_return_ = true;
        } else if (*(current + 1) == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    buffer_.append(begin, end);
    co_await maybe_emit_ready(push);
  }

  auto finalize(Push<nova::Events>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    if (builder_ and not buffer_.empty()) {
      process_line(buffer_);
      buffer_.clear();
    }
    co_await emit_finished(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    co_await emit_finished(push);
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("ended_on_carriage_return", ended_on_carriage_return_);
    serde("line_counter", line_counter_);
    if (serde.is_loading()) {
      timeout_.reset();
    }
  }

private:
  auto process_line(std::string_view line) -> void {
    ++line_counter_;
    if (line.empty()) {
      return;
    }
    diagnostic_line_ = line_counter_;
    if (auto diag = parse_event(line, *builder_, quoting_)) {
      (**dh_).emit(std::move(*diag));
    }
    diagnostic_line_ = None{};
  }

  auto emit_finished(Push<nova::Events>& push) -> Task<void> {
    if (builder_ and builder_->length() > 0) {
      auto events = builder_->finish();
      timeout_.reset();
      co_await push(std::move(events));
    }
  }

  auto maybe_emit_ready(Push<nova::Events>& push) -> Task<void> {
    if (builder_ and timeout_.poll(builder_->length())) {
      co_await emit_finished(push);
    }
  }

  ReadLeefArgs args_;
  std::string buffer_;
  bool ended_on_carriage_return_ = false;
  size_t line_counter_ = 0;
  detail::quoting_escaping_policy quoting_;
  // Present only while parsing a line, not during setup or batch finalization.
  Option<size_t> diagnostic_line_;
  Option<Box<transforming_diagnostic_handler>> dh_;
  Option<nova::EventBuilder> builder_;
  BatchTimeout timeout_;
};

class read_leef final : public virtual operator_factory_plugin,
                        public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_leef";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadLeefArgs, ReadLeef, ReadLeefEvents>{ReadLeefArgs{
      .msb_options = {.settings = {.default_schema_name = "leef.event"}},
    }};
    auto msb = add_msb_to_describer(d, &ReadLeefArgs::msb_options);
    d.validate(msb);
    d.validate([msb](DescribeCtx& ctx) -> Empty {
      return nova::validate_event_builder_options(msb, ctx);
    });
    d.operator_location(&ReadLeefArgs::operator_location);
    d.optimization(&ReadLeefArgs::optimization);
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    return {
      .extensions = {"leef"},
      .mime_types = {"application/x-leef"},
    };
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate(
        "read_leef", read_detection::specificity::dialect,
        [](read_detection_input input) {
          input.bytes = detail::trim_front(input.bytes);
          return read_detection::magic_prefix(input, "LEEF:");
        }),
    };
  }
};
struct ParseLeefArgs {
  nova::ValueArgument x;
  nova::EventBuilder::Settings settings;
  location call;
};

class ParseLeefFunction {
public:
  static auto eval(ParseLeefArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto const length = frame.length();
    auto strings = args.x.data.get_alternative<nova::String>();
    auto accepted
      = strings ? strings->present : nova::storage::BitMap{length, false};
    if (auto nulls = args.x.data.get_alternative<nova::Null>()) {
      accepted = accepted | nulls->present;
    }
    if (auto invalid = frame.mask().and_not(accepted); invalid.any()) {
      auto row = *nova::storage::true_bits(invalid).begin();
      match(args.x.data.get(row), [&]<class T>(nova::RowView<T>) {
        diagnostic::warning("`parse_leef` expected `string`, got `{}`",
                            nova::Type<T>::static_name)
          .primary(args.x.source)
          .emit(frame);
      });
    }
    auto dh = transforming_diagnostic_handler{
      frame, [&](diagnostic d) {
        if (not d.has_location()) {
          d.annotations.emplace_back(true, std::string{}, args.call);
        }
        return d;
      }};
    auto builder = nova::EventBuilder::make_prevalidated(args.settings, dh);
    auto quoting = detail::quoting_escaping_policy{};
    for (auto row = nova::storage::Index{0}; row < length; ++row) {
      if (not frame.mask().get(row)) {
        builder.skip();
        continue;
      }
      if (not strings or not strings->present.get(row)) {
        builder.value().null();
        continue;
      }
      if (auto diag = parse_event(*strings->data.get(row), builder, quoting)) {
        dh.emit(std::move(*diag));
        builder.value().null();
      }
    }
    return builder.finish_data();
  }
};

class parse_leef final : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "parse_leef";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<ParseLeefArgs, ParseLeefFunction>{};
    d.positional("x", &ParseLeefArgs::x, "string");
    d.call_location(&ParseLeefArgs::call);
    auto validate
      = nova::add_event_builder_to_describer(d, &ParseLeefArgs::settings);
    d.validate([validate](ParseLeefArgs& args,
                          nova::FunctionValidateCtx& ctx) -> failure_or<void> {
      args.settings.infer_unparsed_under = "attributes";
      return validate(args, ctx);
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "string");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(
      parser, true, multi_series_builder_argument_parser::merge_option::hidden);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    return function_use::make([call = inv.call.get_location(),
                               msb_ops = std::move(msb_opts),
                               expr = std::move(expr)](auto eval, session ctx) {
      return map_series(eval(expr), [&](series arg) {
        auto f = detail::overload{
          [&](const arrow::StringArray& arg) -> multi_series {
            auto builder = multi_series_builder{
              msb_ops,
              ctx,
            };
            auto quoting = detail::quoting_escaping_policy{};
            for (auto string : arg) {
              if (not string) {
                builder.null();
                continue;
              }
              auto diag = parse_line(*string, builder, quoting);
              if (diag) {
                ctx.dh().emit(std::move(*diag));
                builder.null();
              }
            }
            return multi_series{builder.finalize()};
          },
          [&](const auto&) -> multi_series {
            diagnostic::warning("`parse_leef` expected `string`, got "
                                "`{}`",
                                arg.type.kind())
              .primary(call)
              .emit(ctx);
            /// TODO: We actually know the type it would produce here, sans the
            /// attributes
            return series::null(null_type{}, arg.length());
          },
        };
        return match(*arg.array, f);
      });
    });
  }
};

struct printer_args final {
  ast::expression attributes;
  ast::expression vendor;
  ast::expression product_name;
  ast::expression product_version;
  ast::expression event_class_id;
  located<std::string> delimiter = located{"\t", location::unknown};
  located<std::string> null_value = located{std::string{}, location::unknown};
  located<std::string> flatten_separator
    = located{std::string{"."}, location::unknown};
  location op;

  auto add_to(argument_parser2& p) -> void {
    p.positional("attributes", attributes, "record");
    p.named("vendor", vendor, "string");
    p.named("product_name", product_name, "string");
    p.named("product_version", product_version, "string");
    p.named("event_class_id", event_class_id, "string");
    p.named_optional("delimiter", delimiter, "string");
    p.named_optional("null_value", null_value);
    p.named_optional("flatten_separator", flatten_separator);
  }

  auto loc(into_location loc) const -> location {
    return loc ? loc : op;
  }

  friend auto inspect(auto& f, printer_args& x) -> bool {
    return f.object(x).fields(
      f.field("attributes", x.attributes), f.field("vendor", x.vendor),
      f.field("product_name", x.product_name),
      f.field("product_version", x.product_version),
      f.field("event_class_id", x.event_class_id),
      f.field("delimiter", x.delimiter), f.field("null_value", x.null_value),
      f.field("flatten_separator", x.flatten_separator), f.field("op", x.op));
  }
};

void append_attributes(std::string& out, record_view3 attributes,
                       std::string_view delim, location loc,
                       diagnostic_handler& dh) {
  const auto f = detail::overload{
    [&](const caf::none_t&) {
      // noop
    },
    [&](auto v) {
      fmt::format_to(std::back_inserter(out), "{}", v);
    },

    [&](view3<list>) {
      diagnostic::warning("`list` is not supported in a LEEF attribute value")
        .primary(loc)
        .emit(dh);
    },
    [&](view3<tenzir::pattern>) {
      TENZIR_UNREACHABLE();
    },
    [&](view3<tenzir::record>) {
      TENZIR_UNREACHABLE();
    },
  };
  for (const auto& [k, v] : attributes) {
    out += k;
    out += '=';
    match(v, f);
    out.append(delim);
  }
  // Remove the final delimiter again
  out.erase(out.size() - 1);
}

struct PrintLeefArgs {
  nova::ValueArgument attributes;
  nova::ValueArgument vendor;
  nova::ValueArgument product_name;
  nova::ValueArgument product_version;
  nova::ValueArgument event_class_id;
  located<std::string> delimiter = located{"\t", location::unknown};
  located<std::string> null_value = located{std::string{}, location::unknown};
  located<std::string> flatten_separator
    = located{std::string{"."}, location::unknown};
};

/// Collects the flattened leaf paths of records inside a list. Values remain
/// unsupported, but their flattened field names match the record printer.
auto collect_leef_list_paths(nova::RowView<nova::Data> value,
                             std::string_view prefix,
                             std::string_view separator,
                             std::vector<std::string>& paths) -> bool {
  return match(
    value,
    [&](nova::RowView<nova::List> list) {
      auto found = false;
      for (auto element : list) {
        found
          = collect_leef_list_paths(element, prefix, separator, paths) or found;
      }
      return found;
    },
    [&](nova::RowView<nova::Record> record) {
      for (auto [name, field] : record) {
        auto path = fmt::format("{}{}", prefix, name);
        if (not collect_leef_list_paths(field, path + std::string{separator},
                                        separator, paths)
            and std::ranges::find(paths, path) == paths.end()) {
          paths.push_back(std::move(path));
        }
      }
      return true;
    },
    [](auto const&) {
      return false;
    });
}

/// Collects scalar and list leaves in the record's own field order.
auto collect_leef_attributes(
  nova::RowView<nova::Record> record, std::string_view prefix,
  std::string_view separator,
  std::vector<std::pair<std::string, nova::RowView<nova::Data>>>& fields)
  -> void {
  for (auto [name, value] : record) {
    auto path = fmt::format("{}{}", prefix, name);
    match(
      value,
      [&](nova::RowView<nova::Record> nested) {
        collect_leef_attributes(nested, path + std::string{separator},
                                separator, fields);
      },
      [&](nova::RowView<nova::List> list) {
        auto paths = std::vector<std::string>{};
        if (collect_leef_list_paths(list, path + std::string{separator},
                                    separator, paths)) {
          for (auto& leaf : paths) {
            fields.emplace_back(std::move(leaf), value);
          }
        } else {
          fields.emplace_back(std::move(path), value);
        }
      },
      [&](auto const&) {
        fields.emplace_back(std::move(path), value);
      });
  }
}

class PrintLeefFunction {
public:
  static auto eval(PrintLeefArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto const length = frame.length();
    auto valid = frame.mask();
    auto header_args = std::array{
      std::pair{"vendor", &args.vendor},
      std::pair{"product_name", &args.product_name},
      std::pair{"product_version", &args.product_version},
      std::pair{"event_class_id", &args.event_class_id},
    };
    auto headers
      = std::array<Option<nova::MaskedArray<nova::Array<nova::String>>>, 4>{};
    for (auto i = size_t{0}; i < header_args.size(); ++i) {
      auto const& [name, arg] = header_args[i];
      headers[i] = arg->data.get_alternative<nova::String>();
      auto present = headers[i] ? headers[i]->present
                                : nova::storage::BitMap{length, false};
      auto invalid = frame.mask().and_not(present);
      if (auto nulls = arg->data.get_alternative<nova::Null>()) {
        if ((frame.mask() & nulls->present).any()) {
          diagnostic::warning("`{}` is `null`", name)
            .primary(arg->source)
            .emit(frame);
        }
        invalid = invalid.and_not(nulls->present);
      }
      if (invalid.any()) {
        auto row = *nova::storage::true_bits(invalid).begin();
        match(arg->data.get(row), [&]<class T>(nova::RowView<T>) {
          diagnostic::warning("`{}` must be `string`", name)
            .primary(arg->source, "got `{}`", nova::Type<T>::static_name)
            .emit(frame);
        });
      }
      valid = valid & present;
    }
    auto records = args.attributes.data.get_alternative<nova::Record>();
    auto present
      = records ? records->present : nova::storage::BitMap{length, false};
    if (auto invalid = frame.mask().and_not(present); invalid.any()) {
      auto row = *nova::storage::true_bits(invalid).begin();
      match(args.attributes.data.get(row), [&]<class T>(nova::RowView<T>) {
        diagnostic::warning("`attributes` must be `record`")
          .primary(args.attributes.source, "got `{}`",
                   nova::Type<T>::static_name)
          .emit(frame);
      });
    }
    valid = valid & present;
    auto builder = nova::ArrayBuilder<nova::Data>{};
    auto fields
      = std::vector<std::pair<std::string, nova::RowView<nova::Data>>>{};
    auto out = std::string{};
    for (auto row = nova::storage::Index{0}; row < length; ++row) {
      if (not frame.mask().get(row)) {
        builder.skip();
        continue;
      }
      if (not valid.get(row)) {
        builder.null();
        continue;
      }
      out = args.delimiter.inner == "\t" ? "LEEF:1.0|" : "LEEF:2.0|";
      auto ok = true;
      for (auto i = size_t{0}; i < headers.size(); ++i) {
        auto value = *headers[i]->data.get(row);
        if (value.contains('|')) {
          diagnostic::warning("`{}` contains illegal character `|`",
                              header_args[i].first)
            .primary(header_args[i].second->source)
            .emit(frame);
          ok = false;
          break;
        }
        out += value;
        out += '|';
      }
      if (not ok) {
        builder.null();
        continue;
      }
      if (args.delimiter.inner != "\t") {
        out += args.delimiter.inner;
        out += '|';
      }
      fields.clear();
      collect_leef_attributes(records->data.get(row), "",
                              args.flatten_separator.inner, fields);
      // Keep flattened-name collisions distinct, as `flatten` does.
      auto existing = std::unordered_set<std::string>{};
      for (auto const& [name, value] : fields) {
        existing.insert(name);
      }
      auto seen = std::unordered_set<std::string>{};
      auto first = true;
      for (auto const& [name, value] : fields) {
        auto unique = name;
        if (not seen.insert(name).second) {
          for (auto suffix = size_t{1};; ++suffix) {
            unique = fmt::format("{}_{}", name, suffix);
            if (existing.insert(unique).second) {
              break;
            }
          }
        }
        if (not std::exchange(first, false)) {
          out += args.delimiter.inner;
        }
        out += unique;
        out += '=';
        match(
          value, [](nova::RowView<nova::Null>) {},
          [&](nova::RowView<nova::List>) {
            diagnostic::warning("`list` is not supported in a LEEF attribute "
                                "value")
              .primary(args.attributes.source)
              .emit(frame);
          },
          [](nova::RowView<nova::Record>) {
            TENZIR_UNREACHABLE();
          },
          [&](auto scalar) {
            fmt::format_to(std::back_inserter(out), "{}", *scalar);
          });
      }
      builder.data(std::string_view{out});
    }
    return builder.finish();
  }
};

class print_leef final : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "print_leef";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<PrintLeefArgs, PrintLeefFunction>{};
    d.positional("attributes", &PrintLeefArgs::attributes, "record");
    d.named("vendor", &PrintLeefArgs::vendor, "string");
    d.named("product_name", &PrintLeefArgs::product_name, "string");
    d.named("product_version", &PrintLeefArgs::product_version, "string");
    d.named("event_class_id", &PrintLeefArgs::event_class_id, "string");
    d.named_optional("delimiter", &PrintLeefArgs::delimiter);
    d.named_optional("null_value", &PrintLeefArgs::null_value);
    d.named_optional("flatten_separator", &PrintLeefArgs::flatten_separator);
    d.validate([](PrintLeefArgs& args,
                  diagnostic_handler& dh) -> failure_or<void> {
      if (args.delimiter.inner.size() != 1) {
        diagnostic::error("custom LEEF `delimiter` must be a single character")
          .primary(args.delimiter, "got `{}`", args.delimiter.inner)
          .emit(dh);
        return failure::promise();
      }
      if (args.delimiter.inner == "|") {
        diagnostic::error("custom LEEF `delimiter` must not be `|`")
          .primary(args.delimiter)
          .emit(dh);
        return failure::promise();
      }
      if (args.null_value.inner.contains('|')) {
        diagnostic::error("`null_value` must not contain `|`")
          .primary(args.null_value)
          .emit(dh);
        return failure::promise();
      }
      if (args.flatten_separator.inner.contains('|')) {
        diagnostic::error("`flatten_separator` must not contain `|`")
          .primary(args.flatten_separator)
          .emit(dh);
        return failure::promise();
      }
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto parser = argument_parser2::function(name());
    auto args = printer_args{};
    args.op = inv.call.get_location();
    args.add_to(parser);
    TRY(parser.parse(inv, ctx));
    if (args.delimiter.inner.size() != 1) {
      diagnostic::error("custom LEEF `delimiter` must be a single character")
        .primary(args.delimiter, "got `{}`", args.delimiter.inner)
        .emit(ctx);
      return failure::promise();
    }
    if (args.delimiter.inner == "|") {
      diagnostic::error("custom LEEF `delimiter` must not be `|`")
        .primary(args.delimiter)
        .emit(ctx);
      return failure::promise();
    }
    if (args.null_value.inner.contains("|")) {
      diagnostic::error("`null_value` must not contain `|`")
        .primary(args.null_value)
        .emit(ctx);
      return failure::promise();
    }
    if (args.flatten_separator.inner.contains("|")) {
      diagnostic::error("`flatten_separator` must not contain `|`")
        .primary(args.flatten_separator)
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([args = std::move(args)](
                                auto eval, session ctx) -> multi_series {
      const auto arr = std::array{
        eval(args.vendor),          eval(args.product_name),
        eval(args.product_version), eval(args.event_class_id),
        eval(args.attributes),
      };
      return map_series(
        arr, [&args, &ctx](const std::span<series> x) -> multi_series {
          TENZIR_ASSERT(x.size() == 5);
          const auto& vendor_series = x[0];
          const auto& product_name_series = x[1];
          const auto& product_version_series = x[2];
          const auto& event_class_id_series = x[3];
          const auto attributes_series_f
            = flatten(x[4], args.flatten_separator.inner);
          const auto& attributes_series = attributes_series_f.series;
          TENZIR_ASSERT(vendor_series.length() == product_name_series.length());
          TENZIR_ASSERT(vendor_series.length()
                        == product_version_series.length());
          TENZIR_ASSERT(vendor_series.length()
                        == event_class_id_series.length());
          TENZIR_ASSERT(vendor_series.length() == attributes_series.length());
          bool ok = true;
#define TYPE_CHECK_AND_MAKE_GEN(NAME, TYPE)                                    \
  if (not(NAME##_series.type.kind().template is<TYPE>())) {                    \
    ok = false;                                                                \
    diagnostic::warning("`" #NAME "` must be `{}`", type_kind{tag_v<TYPE>})    \
      .primary(args.loc(args.NAME), "got `{}`", NAME##_series.type.kind())     \
      .emit(ctx);                                                              \
  }                                                                            \
  auto NAME##_gen = values3(*NAME##_series.array)
          TYPE_CHECK_AND_MAKE_GEN(vendor, string_type);
          TYPE_CHECK_AND_MAKE_GEN(product_name, string_type);
          TYPE_CHECK_AND_MAKE_GEN(product_version, string_type);
          TYPE_CHECK_AND_MAKE_GEN(event_class_id, string_type);
          TYPE_CHECK_AND_MAKE_GEN(attributes, record_type);
#undef TYPE_CHECK_AND_MAKE_GEN
          if (not ok) {
            return series::null(string_type{}, vendor_series.length());
          }
          auto builder = type_to_arrow_builder_t<string_type>{};
          check(builder.Reserve(vendor_series.length()));
          auto str = std::string{};
          while (true) {
            const auto vendor = vendor_gen.next();
            const auto product_name = product_name_gen.next();
            const auto product_version = product_version_gen.next();
            const auto event_class_id = event_class_id_gen.next();
            const auto attributes = attributes_gen.next();
            if (not vendor) {
              TENZIR_ASSERT(not product_name);
              TENZIR_ASSERT(not product_version);
              TENZIR_ASSERT(not event_class_id);
              TENZIR_ASSERT(not attributes);
              break;
            }
            str = "LEEF:";
            if (args.delimiter.inner == "\t") {
              str.append("1.0");
            } else {
              str.append("2.0");
            }
            str += '|';
#define CHECK_APPEND_VALUE(field)                                              \
  if (auto* s = try_as<view3<std::string>>(*field)) {                          \
    if (s->contains('|')) {                                                    \
      diagnostic::warning("`" #field "` contains illegal character `|`")       \
        .primary(args.field)                                                   \
        .emit(ctx);                                                            \
      check(builder.AppendNull());                                             \
      continue;                                                                \
    } else {                                                                   \
      str.append(as<view3<std::string>>(*field));                              \
    }                                                                          \
  } else if (is<view3<caf::none_t>>(*field)) {                                 \
    diagnostic::warning("`" #field "` is `null`")                              \
      .primary(args.field)                                                     \
      .emit(ctx);                                                              \
    check(builder.AppendNull());                                               \
    continue;                                                                  \
  } else {                                                                     \
    TENZIR_UNREACHABLE();                                                      \
  }                                                                            \
  str += '|'
            CHECK_APPEND_VALUE(vendor);
            CHECK_APPEND_VALUE(product_name);
            CHECK_APPEND_VALUE(product_version);
            CHECK_APPEND_VALUE(event_class_id);
#undef CHECK_APPEND_VALUE
            if (args.delimiter.inner != "\t") {
              str.append(args.delimiter.inner);
              str += '|';
            }
            if (auto* r = try_as<view3<record>>(*attributes)) {
              append_attributes(str, *r, args.delimiter.inner,
                                args.attributes.get_location(), ctx);
            } else {
              TENZIR_ASSERT(is<view3<caf::none_t>>(*attributes));
              diagnostic::warning("`attributes` is `null`")
                .primary(args.attributes)
                .emit(ctx);
            }
            check(builder.Append(str));
          }
          return series{string_type{}, check(builder.Finish())};
        });
    });
  }
};

} // namespace
} // namespace tenzir::plugins::leef

TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::read_leef)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::parse_leef)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::print_leef)
