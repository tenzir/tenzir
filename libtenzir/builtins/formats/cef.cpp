//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/line_range.hpp>
#include <tenzir/detail/make_io_stream.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/error.hpp>
#include <tenzir/format/multi_schema_reader.hpp>
#include <tenzir/format/reader.hpp>
#include <tenzir/module.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <istream>
#include <memory>

namespace tenzir::plugins::cef {

namespace {

/// Unescapes CEF string data containing \r, \n, \\, and \=.
std::string unescape(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (auto i = 0u; i < value.size(); ++i) {
    if (value[i] != '\\') {
      result += value[i];
    } else if (i + 1 < value.size()) {
      auto next = value[i + 1];
      switch (next) {
        default:
          result += next;
          break;
        case 'r':
        case 'n':
          result += '\n';
          break;
      }
      ++i;
    }
  }
  return result;
}

/// A shallow representation a of a CEF message.
struct message_view {
  uint16_t cef_version;
  std::string device_vendor;
  std::string device_product;
  std::string device_version;
  std::string signature_id;
  std::string name;
  std::string severity;
  record extension;
};

/// Parses the CEF extension field as a sequence of key-value pairs for further
/// downstream processing.
/// @param extension The string value of the extension field.
/// @returns A vector of key-value pairs with properly unescaped values.
caf::expected<record> parse_extension(std::string_view extension) {
  record result;
  auto splits = detail::split_escaped(extension, "=", "\\");
  if (splits.size() < 2)
    return caf::make_error(ec::parse_error, fmt::format("need at least one "
                                                        "key=value pair: {}",
                                                        extension));
  // Process intermediate 'k0=a b c k1=d e f' extensions. The algorithm splits
  // on '='. The first split is a key and the last split is a value. All
  // intermediate splits are "reversed" in that they have the pattern 'a b c k1'
  // where 'a b c' is the value from the previous key and 'k1`' is the key for
  // the next value.
  auto key = splits[0];
  // Strip leading whitespace on first key. The spec says that trailing
  // whitespace is considered part of the previous value, except for the last
  // space that is split on.
  for (size_t i = 0; i < key.size(); ++i)
    if (key[i] != ' ') {
      key = key.substr(i);
      break;
    }
  // Converts a raw, unescaped string to a data instance.
  auto to_data = [](std::string_view str) -> data {
    auto unescaped = unescape(str);
    auto parsed = data{};
    if (not(parsers::data - parsers::pattern)(unescaped, parsed)) {
      parsed = unescaped;
    }
    return parsed;
  };
  for (auto i = 1u; i < splits.size() - 1; ++i) {
    auto split = splits[i];
    auto j = split.rfind(' ');
    if (j == std::string_view::npos)
      return caf::make_error(
        ec::parse_error,
        fmt::format("invalid 'key=value=key' extension: {}", split));
    if (j == 0)
      return caf::make_error(
        ec::parse_error,
        fmt::format("empty value in 'key= value=key' extension: {}", split));
    auto value = split.substr(0, j);
    result.emplace(std::string{key}, to_data(value));
    key = split.substr(j + 1); // next key
  }
  auto value = splits[splits.size() - 1];
  result.emplace(std::string{key}, to_data(value));
  return result;
}

/// Converts a string view into a message.
caf::error convert(std::string_view line, message_view& msg) {
  using namespace std::string_view_literals;
  // Pipes in the extension field do not need escaping.
  auto fields = detail::split_escaped(line, "|", "\\", 8);
  if (fields.size() != 8)
    return caf::make_error(ec::parse_error, //
                           fmt::format("need exactly 8 fields, got '{}'",
                                       fields.size()));
  // Field 0: Version
  auto i = fields[0].find(':');
  if (i == std::string_view::npos)
    return caf::make_error(ec::parse_error, //
                           fmt::format("CEF version requires ':', got '{}'",
                                       fields[0]));
  auto cef_version_str
    = std::string_view{std::next(fields[0].begin(), i + 1), fields[0].end()};
  if (!parsers::u16(cef_version_str, msg.cef_version))
    return caf::make_error(ec::parse_error, //
                           fmt::format("failed to parse CEF version, got '{}'",
                                       cef_version_str));
  // Fields 1-6.
  msg.device_vendor = std::move(fields[1]);
  msg.device_product = std::move(fields[2]);
  msg.device_version = std::move(fields[3]);
  msg.signature_id = std::move(fields[4]);
  msg.name = std::move(fields[5]);
  msg.severity = std::move(fields[6]);
  // Field 7: Extension
  if (auto kvps = parse_extension(fields[7]))
    msg.extension = std::move(*kvps);
  else
    return kvps.error();
  return caf::none;
}

/// Infers a schema from a message.
/// @param msg The message to infer a schema from.
/// @returns The inferred schema.
type infer(const message_view& msg) {
  static constexpr auto name = "cef.event";
  // These fields are always present.
  auto fields = std::vector<struct record_type::field>{
    {"cef_version", uint64_type{}},    {"device_vendor", string_type{}},
    {"device_product", string_type{}}, {"device_version", string_type{}},
    {"signature_id", string_type{}},   {"name", string_type{}},
    {"severity", string_type{}},
  };
  // Infer extension record, if present.
  auto deduce = [](const data& value) -> type {
    if (auto t = type::infer(value))
      return t;
    return type{string_type{}};
  };
  if (!msg.extension.empty()) {
    std::vector<struct record_type::field> ext_fields;
    ext_fields.reserve(msg.extension.size());
    for (const auto& [key, value] : msg.extension)
      ext_fields.emplace_back(std::string{key}, deduce(value));
    fields.emplace_back("extension", record_type{std::move(ext_fields)});
  }
  return {name, record_type{std::move(fields)}};
}

/// Parses a line of ASCII as CEF message.
/// @param msg The CEF message.
/// @param builder The table slice builder to add the message to.
void add(const message_view& msg, builder_ref builder) {
  auto event = builder.record();
  event.field("cef_version", uint64_t{msg.cef_version});
  event.field("device_vendor", msg.device_vendor);
  event.field("device_product", msg.device_product);
  event.field("device_version", msg.device_version);
  event.field("signature_id", msg.signature_id);
  event.field("name", msg.name);
  event.field("severity", msg.severity);
  event.field("extension", msg.extension);
}

class reader : public format::reader {
public:
  using super = format::reader;

  /// Constructs a CEF reader.
  /// @param options Additional options.
  /// @param in The stream of JSON objects.
  explicit reader(const caf::settings& options, std::unique_ptr<std::istream> in
                                                = nullptr)
    : super(options) {
    if (in != nullptr)
      reset(std::move(in));
  }

  reader(const reader&) = delete;
  reader& operator=(const reader&) = delete;
  reader(reader&&) noexcept = default;
  reader& operator=(reader&&) noexcept = default;
  ~reader() override = default;

  void reset(std::unique_ptr<std::istream> in) override {
    TENZIR_ASSERT(in != nullptr);
    input_ = std::move(in);
    lines_ = std::make_unique<detail::line_range>(*input_);
  }

  caf::error module(class module) override {
    // Not implemented.
    return caf::none;
  }

  [[nodiscard]] class module module() const override {
    class module result {};
    return result;
  }

  [[nodiscard]] const char* name() const override {
    return "cef-reader";
  }

protected:
  report status() const override {
    using namespace std::string_literals;
    uint64_t invalid_line = num_invalid_lines_;
    if (num_invalid_lines_ > 0)
      TENZIR_WARN("{} failed to parse {} of {} recent lines",
                  detail::pretty_type_name(this), num_invalid_lines_,
                  num_lines_);
    num_invalid_lines_ = 0;
    num_lines_ = 0;
    return {.data = {
              {name() + ".invalid-line"s, invalid_line},
            }};
  }

  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& cons) override {
    TENZIR_ASSERT(max_events > 0);
    TENZIR_ASSERT(max_slice_size > 0);
    size_t produced = 0;
    auto builder = series_builder{};
    auto finish = [&] {
      for (auto&& slice : builder.finish_as_table_slice("cef.event")) {
        cons(std::move(slice));
      }
    };
    while (produced < max_events) {
      if (lines_->done()) {
        finish();
        return caf::make_error(ec::end_of_input, "input exhausted");
      }
      if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
          && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
        TENZIR_DEBUG("{} reached batch timeout",
                     detail::pretty_type_name(this));
        finish();
        return ec::timeout;
      }
      bool timed_out = lines_->next_timeout(read_timeout_);
      if (timed_out) {
        TENZIR_DEBUG("{} stalled at line {}", detail::pretty_type_name(this),
                     lines_->line_number());
        return ec::stalled;
      }
      auto& line = lines_->get();
      ++num_lines_;
      if (line.empty()) {
        // Ignore empty lines.
        TENZIR_DEBUG("{} ignores empty line at {}",
                     detail::pretty_type_name(this), lines_->line_number());
        continue;
      }
      auto msg = to<message_view>(std::string_view{line});
      if (!msg) {
        TENZIR_WARN("{} failed to parse CEF messge: {}",
                    detail::pretty_type_name(this), msg.error());
        ++num_invalid_lines_;
        continue;
      }
      auto schema = infer(*msg);
      add(*msg, builder);
      produced++;
      batch_events_++;
      if (detail::narrow<size_t>(builder.length()) == max_slice_size) {
        finish();
      }
    }
    finish();
    return caf::none;
  }

private:
  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  mutable size_t num_invalid_lines_ = 0;
  mutable size_t num_lines_ = 0;
};

auto impl(generator<std::optional<std::string_view>> lines,
          operator_control_plane& ctrl) -> generator<table_slice> {
  auto builder = series_builder{};
  for (auto&& line : lines) {
    // TODO: Flush builder if maximum batch size or timeout is reached.
    if (!line) {
      co_yield {};
      continue;
    }
    if (line->empty()) {
      TENZIR_DEBUG("CEF parser ignored empty line");
      continue;
    }
    auto msg = to<message_view>(*line);
    if (!msg) {
      ctrl.warn(caf::make_error(ec::parse_error,
                                fmt::format("CEF parser failed to parse "
                                            "message: {} "
                                            "(line: '{}')",
                                            msg.error(), *line)));
      continue;
    }
    add(*msg, builder);
  }
  for (auto& slice : builder.finish_as_table_slice("cef.event")) {
    co_yield std::move(slice);
  }
}

class cef_parser final : public plugin_parser {
public:
  auto name() const -> std::string override {
    return "cef";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return impl(to_lines(std::move(input)), ctrl);
  }

  friend auto inspect(auto&, cef_parser&) -> bool {
    return true;
  }
};

class plugin final : public virtual reader_plugin,
                     public virtual parser_plugin<cef_parser> {
  [[nodiscard]] const char* reader_format() const override {
    return "cef";
  }

  [[nodiscard]] const char* reader_help() const override {
    return "imports logs in Common Event Format (CEF)";
  }

  [[nodiscard]] config_options
  reader_options(command::opts_builder&&) const override {
    return {};
  }

  [[nodiscard]] std::unique_ptr<format::reader>
  make_reader(const caf::settings& options) const override {
    auto in = detail::make_input_stream(options);
    return std::make_unique<reader>(options, in ? std::move(*in) : nullptr);
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    argument_parser{"cef", "https://docs.tenzir.com/next/formats/cef"}.parse(p);
    return std::make_unique<cef_parser>();
  }
};

} // namespace

} // namespace tenzir::plugins::cef

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::plugin)
