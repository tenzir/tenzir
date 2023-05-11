//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/numeric.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/data.hpp>
#include <vast/detail/assert.hpp>
#include <vast/detail/line_range.hpp>
#include <vast/detail/make_io_stream.hpp>
#include <vast/detail/string.hpp>
#include <vast/error.hpp>
#include <vast/format/multi_schema_reader.hpp>
#include <vast/format/reader.hpp>
#include <vast/module.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/to_lines.hpp>
#include <vast/type.hpp>
#include <vast/view.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <istream>
#include <memory>

namespace vast::plugins::cef {

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
  std::string_view device_vendor;
  std::string_view device_product;
  std::string_view device_version;
  std::string_view signature_id;
  std::string_view name;
  std::string_view severity;
  record extension;
};

/// Parses the CEF extension field as a sequence of key-value pairs for further
/// downstream processing.
/// @param extension The string value of the extension field.
/// @returns A vector of key-value pairs with properly unescaped values.
caf::expected<record> parse_extension(std::string_view extension) {
  record result;
  auto splits = detail::split(extension, "=", "\\");
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
    if (auto x = to<data>(unescaped))
      return std::move(*x);
    return unescaped;
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
  auto fields = detail::split(line, "|", "\\", 8);
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
  auto cef_version_str = fields[0].substr(i + 1);
  if (!parsers::u16(cef_version_str, msg.cef_version))
    return caf::make_error(ec::parse_error, //
                           fmt::format("failed to parse CEF version, got '{}'",
                                       cef_version_str));
  // Fields 1-6.
  msg.device_vendor = fields[1];
  msg.device_product = fields[2];
  msg.device_version = fields[3];
  msg.signature_id = fields[4];
  msg.name = fields[5];
  msg.severity = fields[6];
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
caf::error add(const message_view& msg, table_slice_builder& builder) {
  auto append = [&](const auto& x) -> caf::error {
    if (!builder.add(make_data_view(x)))
      return caf::make_error(ec::parse_error, //
                             fmt::format("failed to add value: {}", x));
    return caf::none;
  };
  // High-order helper function for the monadic caf::error::eval utility.
  auto f = [&](const auto& x) {
    return [&]() {
      return append(x);
    };
  };
  // Append first 7 fields.
  if (auto err
      = caf::error::eval(f(uint64_t{msg.cef_version}), f(msg.device_vendor),
                         f(msg.device_product), f(msg.device_version),
                         f(msg.signature_id), f(msg.name), f(msg.severity)))
    return err;
  // Append extension fields.
  for (const auto& [_, value] : msg.extension) {
    if (auto err = append(value))
      return err;
  }
  return caf::none;
}

class reader : public format::multi_schema_reader {
public:
  using super = multi_schema_reader;

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
    VAST_ASSERT(in != nullptr);
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
  system::report status() const override {
    using namespace std::string_literals;
    uint64_t invalid_line = num_invalid_lines_;
    if (num_invalid_lines_ > 0)
      VAST_WARN("{} failed to parse {} of {} recent lines",
                detail::pretty_type_name(this), num_invalid_lines_, num_lines_);
    num_invalid_lines_ = 0;
    num_lines_ = 0;
    return {.data = {
              {name() + ".invalid-line"s, invalid_line},
            }};
  }

  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& cons) override {
    VAST_ASSERT(max_events > 0);
    VAST_ASSERT(max_slice_size > 0);
    size_t produced = 0;
    table_slice_builder_ptr bptr = nullptr;
    while (produced < max_events) {
      if (lines_->done())
        return finish(cons, caf::make_error(ec::end_of_input, "input "
                                                              "exhausted"));
      if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
          && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
        VAST_DEBUG("{} reached batch timeout", detail::pretty_type_name(this));
        return finish(cons, ec::timeout);
      }
      bool timed_out = lines_->next_timeout(read_timeout_);
      if (timed_out) {
        VAST_DEBUG("{} stalled at line {}", detail::pretty_type_name(this),
                   lines_->line_number());
        return ec::stalled;
      }
      auto& line = lines_->get();
      ++num_lines_;
      if (line.empty()) {
        // Ignore empty lines.
        VAST_DEBUG("{} ignores empty line at {}",
                   detail::pretty_type_name(this), lines_->line_number());
        continue;
      }
      auto msg = to<message_view>(std::string_view{line});
      if (!msg) {
        VAST_WARN("{} failed to parse CEF messge: {}",
                  detail::pretty_type_name(this), msg.error());
        ++num_invalid_lines_;
        continue;
      }
      auto schema = infer(*msg);
      bptr = builder(schema);
      if (bptr == nullptr)
        return caf::make_error(ec::parse_error, "unable to get a builder");
      if (auto err = add(*msg, *bptr)) {
        VAST_WARN("{} failed to parse line {}: {} ({})",
                  detail::pretty_type_name(this), lines_->line_number(), line,
                  err);
        ++num_invalid_lines_;
        continue;
      }
      produced++;
      batch_events_++;
      if (bptr->rows() == max_slice_size)
        if (auto err = finish(cons, bptr))
          return err;
    }
    return finish(cons);
  }

private:
  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  mutable size_t num_invalid_lines_ = 0;
  mutable size_t num_lines_ = 0;
};

class plugin final : public virtual reader_plugin,
                     public virtual parser_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return caf::none;
  }

  [[nodiscard]] std::string name() const override {
    return "cef";
  }

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

  auto make_parser(std::vector<std::string> args, generator<chunk_ptr> loader,
                   operator_control_plane& ctrl) const
    -> caf::expected<parser> override {
    if (!args.empty()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("CEF parser does not expecte any "
                                         "arguments but got [{}]",
                                         fmt::join(args, ", ")));
    }
    return std::invoke(
      [](generator<std::optional<std::string_view>> lines,
         operator_control_plane& ctrl) -> generator<table_slice> {
        auto builder = std::optional<table_slice_builder>{};
        for (auto&& line : lines) {
          if (!line) {
            co_yield {};
            continue;
          }
          if (line->empty()) {
            VAST_DEBUG("CEF parser ignored empty line");
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
          auto schema = infer(*msg);
          if (!builder || builder->schema() != schema) {
            if (builder) {
              co_yield builder->finish();
            }
            builder = table_slice_builder{schema};
          }
          if (auto error = add(*msg, *builder)) {
            ctrl.warn(
              caf::make_error(ec::parse_error, fmt::format("CEF parser failed "
                                                           "to add message: {} "
                                                           "(line: '{}')",
                                                           error, line)));
            continue;
          }
        }
        if (builder) {
          co_yield builder->finish();
        }
      },
      to_lines(std::move(loader)), ctrl);
  }

  auto default_loader(std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    (void)args;
    return {"stdin", {}};
  }
};

} // namespace vast::plugins::cef

VAST_REGISTER_PLUGIN(vast::plugins::cef::plugin)
