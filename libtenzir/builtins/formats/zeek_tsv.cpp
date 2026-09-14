//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/pusher.hpp"
#include "tenzir/box.hpp"
#include "tenzir/cast.hpp"
#include "tenzir/concept/parseable/string/any.hpp"
#include "tenzir/concept/parseable/tenzir/option_set.hpp"
#include "tenzir/concept/parseable/tenzir/pipeline.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/detail/string_literal.hpp"
#include "tenzir/detail/to_xsv_sep.hpp"
#include "tenzir/detail/zeekify.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/read_detection.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view.hpp"

#include <arrow/record_batch.h>
#include <arrow/util/utf8.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/none.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>

namespace tenzir::plugins::zeek_tsv {

namespace {

template <concrete_type Type>
struct zeek_parser {
  auto operator()(const Type&, char, const std::string&) const
    -> rule<std::string_view::const_iterator, type_to_data_t<Type>> {
    TENZIR_UNIMPLEMENTED();
  }
};

template <>
struct zeek_parser<bool_type> {
  auto operator()(const bool_type&, char, const std::string&) const {
    return parsers::tf;
  }
};

template <>
struct zeek_parser<int64_type> {
  auto operator()(const int64_type&, char, const std::string&) const {
    return parsers::i64;
  }
};

template <>
struct zeek_parser<uint64_type> {
  auto operator()(const uint64_type&, char, const std::string&) const {
    return parsers::u64;
  }
};

template <>
struct zeek_parser<double_type> {
  auto operator()(const double_type&, char, const std::string&) const {
    return parsers::real.then([](double x) {
      return x;
    });
  }
};

template <>
struct zeek_parser<duration_type> {
  auto operator()(const duration_type&, char, const std::string&) const {
    return parsers::real.then([](double x) {
      return std::chrono::duration_cast<duration>(double_seconds(x));
    });
  }
};

template <>
struct zeek_parser<time_type> {
  auto operator()(const time_type&, char, const std::string&) const {
    return parsers::real.then([](double x) {
      return time{} + std::chrono::duration_cast<duration>(double_seconds(x));
    });
  }
};

template <>
struct zeek_parser<string_type> {
  auto operator()(const string_type&, char separator,
                  const std::string& set_separator) const
    -> rule<std::string_view::const_iterator, std::string> {
    auto decode = [](std::string x) {
      auto unescaped = detail::byte_unescape(x);
      if (arrow::util::ValidateUTF8(unescaped)) {
        return unescaped;
      }
      return detail::byte_escape(x);
    };
    if (set_separator.empty()) {
      return (+(parsers::any - separator)).then(decode);
    }
    return (+(parsers::any - separator - set_separator)).then(decode);
  }
};

template <>
struct zeek_parser<ip_type> {
  auto operator()(const ip_type&, char, const std::string&) const {
    return parsers::ip;
  }
};

template <>
struct zeek_parser<subnet_type> {
  auto operator()(const subnet_type&, char, const std::string&) const {
    return parsers::net;
  }
};

template <>
struct zeek_parser<list_type> {
  auto operator()(const list_type& lt, char separator,
                  const std::string& set_separator) const
    -> rule<std::string_view::const_iterator, list> {
    auto f
      = [&]<concrete_type Type>(
          const Type& type) -> rule<std::string_view::const_iterator, list> {
      return (zeek_parser<Type>{}(type, separator, set_separator)
                .then([](type_to_data_t<Type> value) {
                  return data{value};
                })
              % set_separator);
    };
    return match(lt.value_type(), f);
  }
};

// Creates a Tenzir type from an ASCII Zeek type in a log header.
auto parse_type(std::string_view zeek_type) -> caf::expected<type> {
  type t;
  if (zeek_type == "enum" or zeek_type == "string" or zeek_type == "file"
      or zeek_type == "pattern") {
    t = type{string_type{}};
  } else if (zeek_type == "bool") {
    t = type{bool_type{}};
  } else if (zeek_type == "int") {
    t = type{int64_type{}};
  } else if (zeek_type == "count") {
    t = type{uint64_type{}};
  } else if (zeek_type == "double") {
    t = type{double_type{}};
  } else if (zeek_type == "time") {
    t = type{time_type{}};
  } else if (zeek_type == "interval") {
    t = type{duration_type{}};
  } else if (zeek_type == "addr") {
    t = type{ip_type{}};
  } else if (zeek_type == "subnet") {
    t = type{subnet_type{}};
  } else if (zeek_type == "port") {
    // FIXME: once we ship with builtin type aliases, we should reference the
    // port alias type here. Until then, we create the alias manually.
    // See also:
    // - src/format/pcap.cpp
    t = type{"port", uint64_type{}};
  }
  if (not t
      and (zeek_type.starts_with("vector") or zeek_type.starts_with("set")
           or zeek_type.starts_with("table"))) {
    // Zeek's logging framwork cannot log nested vectors/sets/tables, so we can
    // safely assume that we're dealing with a basic type inside the brackets.
    // If this will ever change, we'll have to enhance this simple parser.
    auto open = zeek_type.find('[');
    auto close = zeek_type.rfind(']');
    if (open == std::string::npos or close == std::string::npos) {
      return caf::make_error(ec::format_error, "missing container brackets:",
                             std::string{zeek_type});
    }
    auto elem = parse_type(zeek_type.substr(open + 1, close - open - 1));
    if (not elem) {
      return elem.error();
    }
    // Zeek sometimes logs sets as tables, e.g., represents set[string] as
    // table[string]. In Tenzir, they are all lists.
    t = type{list_type{*elem}};
  }
  if (not t) {
    return caf::make_error(ec::format_error,
                           "failed to parse type: ", std::string{zeek_type});
  }
  return t;
}

struct zeek_metadata {
  using iterator_type = std::string_view::const_iterator;

  auto is_unset(std::string_view field) -> bool {
    return std::equal(unset_field.begin(), unset_field.end(), field.begin(),
                      field.end());
  }

  auto is_empty(std::string_view field) -> bool {
    return std::equal(empty_field.begin(), empty_field.end(), field.begin(),
                      field.end());
  }

  auto make_parser(const auto& type, const auto& set_sep) {
    return make_zeek_parser<iterator_type>(type, set_sep);
  };

  std::string sep{};
  int sep_char{};
  std::string set_sep{};
  std::string empty_field{};
  std::string unset_field{};
  std::string path{};
  std::string fields_str{};
  std::string types_str{};
  std::vector<std::string_view> fields{};
  std::vector<std::string_view> types{};
  std::string name{};
  std::vector<struct record_type::field> record_fields{};
  type output_slice_schema{};
  type temp_slice_schema{};
  std::vector<rule<iterator_type, data>> parsers{};
  std::vector<std::string> parsed_options{};
  std::string header{};
  std::array<std::string_view, 7> prefix_options{
    "#set_separator", "#empty_field", "#unset_field", "#path",
    "#open",          "#fields",      "#types",
  };
};

struct zeek_printer {
  zeek_printer(char set_sep, std::string_view empty = "",
               std::string_view unset = "", bool disable_timestamp_tags = false)
    : set_sep{set_sep},
      empty_field{empty},
      unset_field{unset},
      disable_timestamp_tags{disable_timestamp_tags} {
  }

  auto to_zeek_string(const type& t) const -> std::string {
    auto f = detail::overload{
      [](const null_type&) -> std::string {
        return "none";
      },
      [](const bool_type&) -> std::string {
        return "bool";
      },
      [](const int64_type&) -> std::string {
        return "int";
      },
      [&](const uint64_type&) -> std::string {
        return t.name() == "port" ? "port" : "count";
      },
      [](const double_type&) -> std::string {
        return "double";
      },
      [](const duration_type&) -> std::string {
        return "interval";
      },
      [](const time_type&) -> std::string {
        return "time";
      },
      [](const string_type&) -> std::string {
        return "string";
      },
      [](const blob_type&) -> std::string {
        return "string";
      },
      [](const ip_type&) -> std::string {
        return "addr";
      },
      [](const subnet_type&) -> std::string {
        return "subnet";
      },
      [](const enumeration_type&) -> std::string {
        return "enum";
      },
      [this](const list_type& lt) -> std::string {
        return fmt::format("vector[{}]", to_zeek_string(lt.value_type()));
      },
      [](const map_type&) -> std::string {
        TENZIR_UNREACHABLE();
      },
      [](const secret_type&) -> std::string {
        return "string";
      },
      [](const record_type&) -> std::string {
        return "record";
      },
    };
    return match(t, f);
  }

  auto generate_timestamp() const -> std::string {
    auto now = std::chrono::system_clock::now();
    return fmt::format(timestamp_format, now);
  }

  template <typename It>
  auto print_header(It& out, const type& t) const noexcept -> bool {
    auto header = fmt::format("#separator \\x{0:02x}\n"
                              "#set_separator{0}{1}\n"
                              "#empty_field{0}{2}\n"
                              "#unset_field{0}{3}\n"
                              "#path{0}{4}",
                              sep, set_sep, empty_field, unset_field, t.name());
    if (not disable_timestamp_tags) {
      header.append(fmt::format("\n#open{}{}", sep, generate_timestamp()));
    }
    header.append("\n#fields");
    auto r = as<record_type>(t);
    for (const auto& [_, offset] : r.leaves()) {
      header.append(fmt::format("{}{}", sep, to_string(r.key(offset))));
    }
    header.append("\n#types");
    for (const auto& [field, _] : r.leaves()) {
      header.append(fmt::format("{}{}", sep, to_zeek_string(field.type)));
    }
    out = std::copy(header.begin(), header.end(), out);
    return true;
  }

  template <typename It>
  auto print_values(It& out, const view<record>& x) const noexcept -> bool {
    auto first = true;
    for (const auto& [_, v] : x) {
      if (not first) {
        ++out = sep;
      } else {
        first = false;
      }
      match(v, visitor{out, *this});
    }
    return true;
  }

  template <typename It>
  auto print_closing_line(It& out) const noexcept -> void {
    if (not disable_timestamp_tags) {
      out = fmt::format_to(out, "#close{}{}\n", sep, generate_timestamp());
    }
  }

  template <class Iterator>
  struct visitor {
    visitor(Iterator& out, const zeek_printer& printer)
      : out{out}, printer{printer} {
    }

    auto operator()(caf::none_t) noexcept -> bool {
      out = std::copy(printer.unset_field.begin(), printer.unset_field.end(),
                      out);
      return true;
    }

    auto operator()(auto x) noexcept -> bool {
      make_printer<decltype(x)> p;
      return p.print(out, x);
    }

    auto operator()(view<bool> x) noexcept -> bool {
      return printers::any.print(out, x ? 'T' : 'F');
    }

    auto operator()(view<pattern>) noexcept -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(view<map>) noexcept -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(view<std::string> x) noexcept -> bool {
      if (x.empty()) {
        out = std::copy(printer.empty_field.begin(), printer.empty_field.end(),
                        out);
        return true;
      }
      for (auto c : x) {
        if (std::iscntrl(c) or c == printer.sep or c == printer.set_sep) {
          auto hex = detail::byte_to_hex(c);
          *out++ = '\\';
          *out++ = 'x';
          *out++ = hex.first;
          *out++ = hex.second;
        } else {
          *out++ = c;
        }
      }
      return true;
    }

    auto operator()(view<blob> x) noexcept -> bool {
      if (x.empty()) {
        // TODO: Is this actually correct? An empty blob is not unset.
        out = std::copy(printer.empty_field.begin(), printer.empty_field.end(),
                        out);
        return true;
      }
      // We do not base64 encode it here, because Zeek strings can contain
      // arbitrary binary data (as long as it is escaped).
      for (auto b : x) {
        // We escape a bit too much here (all non-byte UTF-8 code points), but
        // this should be fine for now.
        auto c = static_cast<unsigned char>(b);
        auto high = (c & 0b1000'0000) != 0;
        if (high or std::iscntrl(c) or c == printer.sep
            or c == printer.set_sep) {
          auto hex = detail::byte_to_hex(c);
          *out++ = '\\';
          *out++ = 'x';
          *out++ = hex.first;
          *out++ = hex.second;
        } else {
          *out++ = c;
        }
      }
      return true;
    }

    auto operator()(const view<list>& x) noexcept -> bool {
      if (x.empty()) {
        out = std::copy(printer.empty_field.begin(), printer.empty_field.end(),
                        out);
        return true;
      }
      auto first = true;
      for (const auto& v : x) {
        if (not first) {
          ++out = printer.set_sep;
        } else {
          first = false;
        }
        match(v, *this);
      }
      return true;
    }

    auto operator()(const view<record>&) noexcept -> bool {
      // We flattened before, so this cannot be reached.
      TENZIR_UNREACHABLE();
    }

    Iterator& out;
    const zeek_printer& printer;
  };

  static constexpr auto timestamp_format{"{:%Y-%m-%d-%H-%M-%S}"};
  char sep{'\t'};
  char set_sep{','};
  std::string empty_field{};
  std::string unset_field{};
  bool disable_timestamp_tags{false};
};

struct zeek_log_state {
  /// Optional metadata.
  char separator = '\t';
  std::string set_separator = ",";
  std::string empty_field = "(empty)";
  std::string unset_field = "-";

  // Required metadata.
  std::string path = {};
  std::vector<std::string> fields = {};
  std::vector<std::string> types = {};

  friend auto inspect(auto& f, zeek_log_state& x) -> bool {
    return f.object(x).fields(f.field("separator", x.separator),
                              f.field("set_separator", x.set_separator),
                              f.field("empty_field", x.empty_field),
                              f.field("unset_field", x.unset_field),
                              f.field("path", x.path),
                              f.field("fields", x.fields),
                              f.field("types", x.types));
  }
};

struct zeek_log : zeek_log_state {
  /// A builder generated from the above metadata.
  Option<series_builder> builder = None{};
  Option<record_ref> event = None{};
  std::vector<rule<std::string_view::const_iterator, bool>> parsers = {};
  type target_schema = {};
};

struct ReadZeekTsvArgs {
  location operator_location = location::unknown;
};

struct WriteZeekTsvArgs {
  located<std::string> set_separator = {",", location::unknown};
  std::string empty_field = "(empty)";
  std::string unset_field = "-";
  bool disable_timestamp_tags = false;
};

class ReadZeekTsv final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadZeekTsv(ReadZeekTsvArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
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
      return d;
    });
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    if (failed_) {
      co_return;
    }
    co_await maybe_emit_ready(push);
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    if (failed_) {
      co_return;
    }
    TENZIR_ASSERT(dh_);
    if (not input or input->size() == 0) {
      co_await maybe_emit_ready(push);
      co_return;
    }
    auto& dh = **dh_;
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
      co_await maybe_emit_ready(push);
      if (buffer_.empty()) {
        co_await process_line({begin, current}, push, dh);
      } else {
        buffer_.append(begin, current);
        co_await process_line(buffer_, push, dh);
        buffer_.clear();
      }
      if (failed_) {
        co_return;
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

  auto finalize(Push<table_slice>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    if (failed_) {
      co_return FinalizeBehavior::done;
    }
    TENZIR_ASSERT(dh_);
    if (not buffer_.empty()) {
      co_await process_line(buffer_, push, **dh_);
      buffer_.clear();
    }
    if (failed_) {
      co_return FinalizeBehavior::done;
    }
    co_await emit_finished(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    if (failed_) {
      co_return;
    }
    co_await emit_finished(push);
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("ended_on_carriage_return", ended_on_carriage_return_);
    serde("line_nr", line_nr_);
    serde("failed", failed_);
    serde("log_has_body", log_has_body_);
    auto& log_state = static_cast<zeek_log_state&>(log_);
    serde("log", log_state);
  }

private:
  auto emit_finished(Push<table_slice>& push) -> Task<void> {
    if (not log_.builder or log_.builder->length() == 0) {
      co_return;
    }
    co_await push(unflatten(log_.builder->finish_assert_one_slice(), "."));
  }

  auto maybe_emit_ready(Push<table_slice>& push) -> Task<void> {
    if (not log_.builder) {
      co_return;
    }
    auto result = log_.builder->yield_ready();
    for (auto& slice : result.slices) {
      slice = unflatten(slice, ".");
    }
    co_await pusher_.push(std::move(result), push);
  }

  auto process_line(std::string_view line, Push<table_slice>& push,
                    diagnostic_handler& dh) -> Task<void> {
    ++line_nr_;
    if (line.empty()) {
      co_return;
    }
    if (line.starts_with('#')) {
      auto header = line.substr(1);
      auto const separator = ignore(parsers::chr{log_.separator});
      auto const unescaped_str
        = (+(parsers::any - separator)).then([](std::string separator) {
            return detail::byte_unescape(separator);
          });
      auto const close_parser
        = ("close" >> separator >> unescaped_str).then([](std::string close) {
            (void)close;
          });
      if (close_parser(header, unused)) {
        if (log_has_body_) {
          co_await emit_finished(push);
          log_ = {};
          log_has_body_ = false;
        }
        co_return;
      }
      if (log_has_body_) {
        co_await emit_finished(push);
        log_ = {};
        log_has_body_ = false;
      }
      // clang-format off
      auto const header_parser
        = ("separator" >> ignore(+parsers::space) >> unescaped_str)
            .with([](std::string separator) {
              return separator.length() == 1;
            })
            .then([&](std::string separator) {
              log_.separator = separator[0];
            })
        | ("set_separator" >> separator >> unescaped_str)
            .then([&](std::string set_separator) {
              log_.set_separator = std::move(set_separator);
            })
        | ("empty_field" >> separator >> unescaped_str)
            .then([&](std::string empty_field) {
              log_.empty_field = std::move(empty_field);
            })
        | ("unset_field" >> separator >> unescaped_str)
            .then([&](std::string unset_field) {
              log_.unset_field = std::move(unset_field);
            })
        | ("path" >> separator >> unescaped_str)
            .then([&](std::string path) {
              log_.path = std::move(path);
            })
        | ("open" >> separator >> unescaped_str)
            .then([](std::string open) {
              (void)open;
            })
        | ("fields" >> separator >> (unescaped_str % separator))
            .then([&](std::vector<std::string> fields) {
              log_.fields = std::move(fields);
            })
        | ("types" >> separator >> (unescaped_str % separator))
            .then([&](std::vector<std::string> types) {
              log_.types = std::move(types);
            });
      // clang-format on
      if (not header_parser(header, unused)) {
        diagnostic::warning("invalid Zeek header: {}", line)
          .note("line {}", line_nr_)
          .emit(dh);
      }
      auto sorted_fields = log_.fields;
      std::ranges::sort(sorted_fields);
      if (auto it = std::ranges::adjacent_find(sorted_fields);
          it != sorted_fields.end()) {
        diagnostic::error(
          "failed to parse Zeek log: duplicate #field name `{}`", *it)
          .note("line {}", line_nr_)
          .emit(dh);
        failed_ = true;
      }
      co_return;
    }
    if (not ensure_log_builder(dh)) {
      co_return;
    }
    auto f = line.begin();
    auto const l = line.end();
    auto add_ok = false;
    auto const separator = ignore(parsers::chr{log_.separator});
    log_.event = log_.builder->record();
    for (auto i = size_t{0}; i < log_.parsers.size() - 1; ++i) {
      auto const parse_ok = log_.parsers[i](f, l, add_ok);
      if (not parse_ok) [[unlikely]] {
        diagnostic::error("failed to parse Zeek value at index {} in `{}`", i,
                          line)
          .note("line {}", line_nr_)
          .emit(dh);
        failed_ = true;
        log_.event = None{};
        co_return;
      }
      TENZIR_ASSERT_EXPENSIVE(add_ok);
      auto const separator_ok = separator(f, l, unused);
      if (not separator_ok) [[unlikely]] {
        diagnostic::error("failed to parse Zeek separator at index {} in `{}`",
                          i, line)
          .note("line {}", line_nr_)
          .emit(dh);
        failed_ = true;
        log_.event = None{};
        co_return;
      }
    }
    auto const parse_ok = log_.parsers.back()(f, l, add_ok);
    if (not parse_ok) [[unlikely]] {
      diagnostic::error("failed to parse Zeek value at index {} in `{}`",
                        log_.parsers.size() - 1, line)
        .note("line {}", line_nr_)
        .emit(dh);
      failed_ = true;
      log_.event = None{};
      co_return;
    }
    auto const eoi_ok = parsers::eoi(f, l, unused);
    if (not eoi_ok) [[unlikely]] {
      diagnostic::warning("unparsed values at end of Zeek line: `{}`",
                          std::string_view{f, l})
        .note("line {}", line_nr_)
        .emit(dh);
    }
    log_.event = None{};
  }

  auto ensure_log_builder(diagnostic_handler& dh) -> bool {
    if (log_.builder) {
      return true;
    }
    if (log_.path.empty()) {
      diagnostic::error("failed to parse Zeek log: missing #path")
        .note("line {}", line_nr_)
        .emit(dh);
      failed_ = true;
      return false;
    }
    if (log_.fields.empty()) {
      diagnostic::error("failed to parse Zeek log: missing #fields")
        .note("line {}", line_nr_)
        .emit(dh);
      failed_ = true;
      return false;
    }
    if (log_.fields.size() != log_.types.size()) {
      diagnostic::error("failed to parse Zeek log: mismatching number "
                        "#fields and #types")
        .note("found {} #fields", log_.fields.size())
        .note("found {} #types", log_.types.size())
        .note("line {}", line_nr_)
        .emit(dh);
      failed_ = true;
      return false;
    }
    log_.parsers.clear();
    log_.parsers.reserve(log_.fields.size());
    auto record_fields = std::vector<record_type::field_view>{};
    record_fields.reserve(log_.fields.size());
    for (auto const& [field, zeek_type] :
         std::views::zip(log_.fields, log_.types)) {
      auto parsed_type = parse_type(zeek_type);
      if (not parsed_type) {
        diagnostic::warning("failed to parse Zeek type `{}`", zeek_type)
          .note("line {}", line_nr_)
          .note("falling back to `string`")
          .emit(dh);
        parsed_type = type{string_type{}};
      }
      auto const make_unset_parser = [&, field]() {
        return ignore(parsers::str{log_.unset_field}
                      >> &(parsers::chr{log_.separator} | parsers::eoi))
          .then([&, field]() {
            log_.event->field(field).null();
            return true;
          });
      };
      auto const make_empty_parser
        = [&, field]<concrete_type Type>(Type const& type) {
            return ignore(parsers::str{log_.empty_field}
                          >> &(parsers::chr{log_.separator} | parsers::eoi))
              .then([&, field]() {
                if constexpr (std::same_as<Type, map_type>) {
                  TENZIR_UNREACHABLE();
                } else {
                  log_.event->field(field, std::move(type.construct()));
                }
                return true;
              });
          };
      auto make_field_parser
        = [&]<concrete_type Type>(
            Type const& type) -> rule<std::string_view::const_iterator, bool> {
        return make_unset_parser() | make_empty_parser(type)
               | zeek_parser<Type>{}(type, log_.separator,
                                     std::same_as<Type, list_type>
                                       ? log_.set_separator
                                       : std::string{})
                   .then([&, field](type_to_data_t<Type> value) {
                     if constexpr (std::same_as<Type, map_type>) {
                       TENZIR_UNREACHABLE();
                     } else {
                       log_.event->field(field, std::move(value));
                     }
                     return true;
                   });
      };
      log_.parsers.push_back(match(*parsed_type, make_field_parser));
      record_fields.push_back({field, std::move(*parsed_type)});
    }
    auto const schema_name = fmt::format("zeek.{}", log_.path);
    log_.builder
      = series_builder{type{schema_name, record_type{record_fields}}};
    auto target_schema = modules::get_schema(schema_name);
    log_.target_schema = target_schema ? std::move(*target_schema) : type{};
    log_has_body_ = true;
    return true;
  }

  ReadZeekTsvArgs args_;
  std::string buffer_;
  bool ended_on_carriage_return_ = false;
  Option<Box<transforming_diagnostic_handler>> dh_;
  zeek_log log_;
  SeriesPusher pusher_;
  size_t line_nr_ = 0;
  bool failed_ = false;
  bool log_has_body_ = false;
};

class WriteZeekTsv final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteZeekTsv(WriteZeekTsvArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto printer
      = zeek_printer{resolved_set_separator(), args_.empty_field,
                     args_.unset_field, args_.disable_timestamp_tags};
    auto buffer = std::vector<char>{};
    auto out_iter = std::back_inserter(buffer);
    auto resolved_slice = flatten(resolve_enumerations(input)).slice;
    auto input_schema = resolved_slice.schema();
    auto input_type = as<record_type>(input_schema);
    auto array = check(to_record_batch(resolved_slice)->ToStructArray());
    auto first = true;
    auto is_first_schema = not last_schema_;
    auto did_schema_change = is_first_schema or *last_schema_ != input_schema;
    last_schema_ = input_schema;
    for (auto const& row : values(type{input_type}, *array)) {
      TENZIR_ASSERT(not is<caf::none_t>(row));
      auto const* record_view = try_as<view<record>>(&row);
      TENZIR_ASSERT(record_view);
      if (first) {
        if (did_schema_change) {
          if (not is_first_schema) {
            printer.print_closing_line(out_iter);
          }
          printer.print_header(out_iter, input_schema);
          out_iter = fmt::format_to(out_iter, "\n");
        }
        first = false;
      }
      auto const ok = printer.print_values(out_iter, *record_view);
      TENZIR_ASSERT(ok);
      out_iter = fmt::format_to(out_iter, "\n");
    }
    co_await push(
      chunk::make(std::move(buffer), {.content_type = "application/x-zeek"}));
  }

private:
  auto resolved_set_separator() const -> char {
    auto converted = to_xsv_sep(args_.set_separator.inner);
    TENZIR_ASSERT(converted);
    return *converted;
  }

  WriteZeekTsvArgs args_;
  Option<type> last_schema_;
};

auto is_zeek_separator_header(std::string_view line) -> bool {
  constexpr auto prefix = std::string_view{"#separator"};
  if (not line.starts_with(prefix)) {
    return false;
  }
  auto separator = line.substr(prefix.size());
  if (separator.empty()
      or detail::ascii_whitespace.find(separator.front())
           == std::string_view::npos) {
    return false;
  }
  separator = detail::trim_front(separator);
  if (separator.empty()) {
    return false;
  }
  return detail::byte_unescape(separator).size() == 1;
}

auto is_zeek_tabular_header(std::string_view line, std::string_view prefix)
  -> bool {
  if (not line.starts_with(prefix)) {
    return false;
  }
  auto fields = line.substr(prefix.size());
  return fields.size() > 1 and fields.front() == '\t';
}

auto is_zeek_header(std::string_view line) -> bool {
  line = detail::trim_front(line);
  return is_zeek_separator_header(line)
         or is_zeek_tabular_header(line, "#fields")
         or is_zeek_tabular_header(line, "#types");
}

auto could_be_zeek_header(std::string_view line) -> bool {
  line = detail::trim_front(line);
  auto could_be = [line](std::string_view prefix, char separator) {
    if (prefix.starts_with(line)) {
      return true;
    }
    if (not line.starts_with(prefix)) {
      return false;
    }
    auto rest = line.substr(prefix.size());
    return rest.empty() or rest.front() == separator;
  };
  return could_be("#separator", ' ') or could_be("#separator", '\t')
         or could_be("#fields", '\t') or could_be("#types", '\t');
}

class read_zeek_tsv final : public virtual operator_factory_plugin,
                            public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_zeek_tsv";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadZeekTsvArgs, ReadZeekTsv>{};
    d.operator_location(&ReadZeekTsvArgs::operator_location);
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    return {
      .extensions = {"zeek"},
      .mime_types = {"application/x-zeek"},
    };
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    auto detect = [](read_detection_input input) {
      auto sample = read_detection::sample_lines(
        {
          .bytes = detail::trim_front(input.bytes),
          .eof = input.eof,
        },
        1);
      if (not sample.complete.empty()) {
        return is_zeek_header(sample.complete.front())
                 ? read_detection::match()
                 : read_detection::reject();
      }
      if (sample.partial.empty()) {
        return input.eof ? read_detection::reject()
                         : read_detection::need_more();
      }
      return could_be_zeek_header(sample.partial) ? read_detection::need_more()
                                                  : read_detection::reject();
    };
    return {
      read_detection::candidate("read_zeek_tsv",
                                read_detection::specificity::magic, detect),
    };
  }
};

class write_zeek_tsv final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_zeek_tsv";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteZeekTsvArgs, WriteZeekTsv>{};
    auto set_separator
      = d.named_optional("set_separator", &WriteZeekTsvArgs::set_separator);
    d.named_optional("empty_field", &WriteZeekTsvArgs::empty_field);
    d.named_optional("unset_field", &WriteZeekTsvArgs::unset_field);
    d.named("disable_timestamp_tags",
            &WriteZeekTsvArgs::disable_timestamp_tags);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto sep = ctx.get(set_separator)) {
        auto converted = to_xsv_sep(sep->inner);
        if (not converted) {
          diagnostic::error("`{}` is not a valid separator", sep->inner)
            .primary(
              ctx.get_location(set_separator).value_or(location::unknown))
            .note(fmt::to_string(converted.error()))
            .emit(ctx);
        } else if (*converted == '\t') {
          diagnostic::error("the `\\t` separator is not allowed here",
                            sep->inner)
            .primary(
              ctx.get_location(set_separator).value_or(location::unknown))
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};
} // namespace

} // namespace tenzir::plugins::zeek_tsv

TENZIR_REGISTER_PLUGIN(tenzir::plugins::zeek_tsv::read_zeek_tsv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::zeek_tsv::write_zeek_tsv)
