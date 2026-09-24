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
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/detail/string_literal.hpp"
#include "tenzir/detail/to_xsv_sep.hpp"
#include "tenzir/detail/zeekify.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/events.hpp"
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
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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
  location operator_location = location::unknown;
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

/// The Zeek type of a value whose type is not yet known.
constexpr auto zeek_unknown_type = std::string_view{"none"};

/// Combines two Zeek types of the same column. The left-hand side is the type
/// in the header, the right-hand side the type of a new value. Unknown types
/// (`none`, also nested as in `vector[none]`) are compatible with any type. A
/// `fixed` header was already written, so its unknown types cannot change.
auto refine_zeek_type(std::string_view lhs, std::string_view rhs, bool fixed)
  -> Option<std::string> {
  if (lhs == rhs or rhs == zeek_unknown_type) {
    return std::string{lhs};
  }
  if (lhs == zeek_unknown_type) {
    if (fixed) {
      return None{};
    }
    return std::string{rhs};
  }
  constexpr auto vector_prefix = std::string_view{"vector["};
  if (lhs.starts_with(vector_prefix) and rhs.starts_with(vector_prefix)
      and lhs.ends_with(']') and rhs.ends_with(']')) {
    auto inner = refine_zeek_type(
      lhs.substr(vector_prefix.size(), lhs.size() - vector_prefix.size() - 1),
      rhs.substr(vector_prefix.size(), rhs.size() - vector_prefix.size() - 1),
      fixed);
    if (not inner) {
      return None{};
    }
    return fmt::format("vector[{}]", *inner);
  }
  return None{};
}

/// Returns whether `name` is a flattened field nested below `prefix`.
auto is_nested_field(std::string_view name, std::string_view prefix) -> bool {
  return name.size() > prefix.size() and name.starts_with(prefix)
         and name[prefix.size()] == '.';
}

/// The contents of a Zeek TSV header.
struct ZeekSchema {
  std::string path;
  std::vector<std::string> fields;
  std::vector<std::string> types;

  friend auto inspect(auto& f, ZeekSchema& x) -> bool {
    return f.object(x).fields(f.field("path", x.path),
                              f.field("fields", x.fields),
                              f.field("types", x.types));
  }
};

/// A Zeek TSV header with lookup structures for its fields.
class ZeekHeader {
public:
  explicit ZeekHeader(ZeekSchema schema) : schema_{std::move(schema)} {
    index();
  }

  auto schema() const -> ZeekSchema const& {
    return schema_;
  }

  auto path() const -> std::string_view {
    return schema_.path;
  }

  auto size() const -> size_t {
    return schema_.fields.size();
  }

  auto field(size_t i) const -> std::string_view {
    return schema_.fields[i];
  }

  auto type(size_t i) const -> std::string_view {
    return schema_.types[i];
  }

  /// Returns the column of a flattened field.
  auto find(std::string_view name) const -> Option<size_t> {
    if (auto it = columns_.find(name); it != columns_.end()) {
      return it->second;
    }
    return None{};
  }

  /// Returns whether the header has fields nested below `name`.
  auto has_nested(std::string_view name) const -> bool {
    return prefixes_.contains(name);
  }

  auto assign(std::vector<std::string> fields, std::vector<std::string> types)
    -> void {
    schema_.fields = std::move(fields);
    schema_.types = std::move(types);
    index();
  }

private:
  auto index() -> void {
    TENZIR_ASSERT(schema_.fields.size() == schema_.types.size());
    columns_.clear();
    prefixes_.clear();
    for (auto i = size_t{0}; i < schema_.fields.size(); ++i) {
      auto const& field = schema_.fields[i];
      columns_.emplace(field, i);
      for (auto dot = field.find('.'); dot != std::string::npos;
           dot = field.find('.', dot + 1)) {
        prefixes_.emplace(field.substr(0, dot));
      }
    }
  }

  ZeekSchema schema_;
  detail::heterogeneous_string_hashmap<size_t> columns_;
  detail::heterogeneous_string_hashset prefixes_;
};

/// Writes Nova events as Zeek TSV.
///
/// A Zeek TSV header fixes the path, the flattened field names, and their
/// types, whereas the rows of a Nova batch may differ in all three. We
/// therefore split the active rows into consecutive blocks that share one
/// header, and start a new header whenever a row does not fit the current one:
///
/// - The schema name must match.
/// - A missing field and a `null` value fit any column, and a `null` record
///   fits all columns below it. Field order does not matter.
/// - A header that is not written yet adopts new fields, and a column that is
///   `null` so far adopts the type, or the nested fields, of the first value.
/// - A written header, e.g., one of a previous batch, no longer changes, so a
///   row with a new field or with a value of another type needs a new one.
class WriteZeekTsvEvents final : public Operator<nova::Events, chunk_ptr> {
public:
  explicit WriteZeekTsvEvents(WriteZeekTsvArgs args)
    : args_{std::move(args)},
      printer_{resolved_set_separator(), args_.empty_field, args_.unset_field,
               args_.disable_timestamp_tags} {
  }

  auto process(nova::Events input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    auto buffer = std::string{};
    auto fixed = true;
    auto rows = std::vector<nova::storage::Index>{};
    auto flush = [&] {
      if (rows.empty()) {
        return;
      }
      TENZIR_ASSERT(header_);
      if (not fixed) {
        print_header(buffer);
        fixed = true;
      }
      for (auto row : rows) {
        collect(input.data.get(row), ctx.dh());
        print_row(buffer, ctx.dh());
      }
      rows.clear();
    };
    for (auto row : nova::storage::true_bits(input.mask)) {
      collect(input.data.get(row), ctx.dh());
      auto path = *input.meta.name.get(row);
      if (not header_ or not merge(*header_, path, fixed)) {
        if (not rows.empty()) {
          // Flushing reuses the leaves, so we must collect the row again.
          flush();
          collect(input.data.get(row), ctx.dh());
        }
        header_.emplace(make_schema(path));
        fixed = false;
      }
      rows.push_back(row);
    }
    flush();
    if (buffer.empty()) {
      co_return;
    }
    co_await push(
      chunk::make(std::move(buffer), {.content_type = "application/x-zeek"}));
  }

  auto snapshot(Serde& serde) -> void override {
    // Between two calls to `process`, the current header is always written.
    auto has_header = header_.has_value();
    auto schema = header_ ? header_->schema() : ZeekSchema{};
    serde("has_header", has_header);
    serde("header", schema);
    if (serde.is_loading()) {
      header_ = None{};
      if (has_header) {
        header_.emplace(std::move(schema));
      }
      written_ = has_header;
    }
  }

private:
  /// One flattened field of a row.
  struct Leaf {
    std::string name;
    std::string type;
    nova::RowView<nova::Data> value;
  };

  auto resolved_set_separator() const -> char {
    auto converted = to_xsv_sep(args_.set_separator.inner);
    TENZIR_ASSERT(converted);
    return *converted;
  }

  auto leaves() const -> std::span<Leaf const> {
    return std::span{leaves_}.first(leaf_count_);
  }

  /// Flattens a row into `leaves_`, reusing previously allocated strings.
  auto collect(nova::RowView<nova::Record> row, diagnostic_handler& dh)
    -> void {
    leaf_count_ = 0;
    prefix_.clear();
    collect_record(row, dh);
  }

  auto collect_record(nova::RowView<nova::Record> record,
                      diagnostic_handler& dh) -> void {
    for (auto [key, value] : record) {
      if (auto const* nested = try_as<nova::RowView<nova::Record>>(value)) {
        auto size = prefix_.size();
        prefix_.append(key);
        prefix_.push_back('.');
        collect_record(*nested, dh);
        prefix_.resize(size);
        continue;
      }
      if (leaf_count_ == leaves_.size()) {
        leaves_.emplace_back(std::string{}, std::string{}, value);
      }
      auto& leaf = leaves_[leaf_count_++];
      leaf.name.assign(prefix_);
      leaf.name.append(key);
      leaf.type.clear();
      leaf.value = value;
      append_type(leaf.type, value, dh);
    }
  }

  auto append_type(std::string& out, nova::RowView<nova::Data> const& value,
                   diagnostic_handler& dh) -> void {
    match(
      value,
      [&](nova::RowView<nova::Null>) {
        out.append(zeek_unknown_type);
      },
      [&](nova::RowView<nova::Bool>) {
        out.append("bool");
      },
      [&](nova::RowView<nova::Int>) {
        out.append("int");
      },
      [&](nova::RowView<nova::UInt>) {
        out.append("count");
      },
      [&](nova::RowView<nova::Float>) {
        out.append("double");
      },
      [&](nova::RowView<nova::Duration>) {
        out.append("interval");
      },
      [&](nova::RowView<nova::Time>) {
        out.append("time");
      },
      [&](nova::RowView<nova::String>) {
        out.append("string");
      },
      [&](nova::RowView<nova::Blob>) {
        out.append("string");
      },
      [&](nova::RowView<nova::Ip>) {
        out.append("addr");
      },
      [&](nova::RowView<nova::Subnet>) {
        out.append("subnet");
      },
      [&](nova::RowView<nova::Record> const&) {
        // Only reachable for records in lists, all others are flattened.
        out.append("record");
      },
      [&](nova::RowView<nova::List> const& list) {
        auto element_type = std::string{zeek_unknown_type};
        auto scratch = std::string{};
        for (auto element : list) {
          scratch.clear();
          append_type(scratch, element, dh);
          if (auto refined = refine_zeek_type(element_type, scratch, false)) {
            element_type = std::move(*refined);
          } else if (not std::exchange(warned_mixed_list_, true)) {
            diagnostic::warning("list elements have mixed types")
              .note("using `{}` in the header instead of `{}`", element_type,
                    scratch)
              .primary(args_.operator_location)
              .emit(dh);
          }
        }
        fmt::format_to(std::back_inserter(out), "vector[{}]", element_type);
      });
  }

  /// Creates a new header for the current row.
  auto make_schema(std::string_view path) const -> ZeekSchema {
    auto result = ZeekSchema{};
    result.path = std::string{path};
    result.fields.reserve(leaf_count_);
    result.types.reserve(leaf_count_);
    for (auto const& leaf : leaves()) {
      result.fields.push_back(leaf.name);
      result.types.push_back(leaf.type);
    }
    return result;
  }

  /// Tries to fit the current row into `header`, refining it if not `fixed`.
  auto merge(ZeekHeader& header, std::string_view path, bool fixed) const
    -> bool {
    if (header.path() != path) {
      return false;
    }
    // Fast path: The row fits the header as is.
    auto fits = std::ranges::all_of(leaves(), [&](Leaf const& leaf) {
      if (auto column = header.find(leaf.name)) {
        return leaf.type == zeek_unknown_type
               or leaf.type == header.type(*column);
      }
      return leaf.type == zeek_unknown_type and header.has_nested(leaf.name);
    });
    if (fits) {
      return true;
    }
    if (fixed) {
      // A written header only fits rows that need no refinement, but a value
      // of a nested type may still be compatible, e.g., an empty list.
      return std::ranges::all_of(leaves(), [&](Leaf const& leaf) {
        if (auto column = header.find(leaf.name)) {
          auto refined
            = refine_zeek_type(header.type(*column), leaf.type, true);
          return refined and *refined == header.type(*column);
        }
        return leaf.type == zeek_unknown_type and header.has_nested(leaf.name);
      });
    }
    // Slow path: Refine a copy of the header, and adopt it if all fields fit.
    auto fields = header.schema().fields;
    auto types = header.schema().types;
    auto find = [&](std::string_view name) -> Option<size_t> {
      auto it = std::ranges::find(fields, name);
      if (it == fields.end()) {
        return None{};
      }
      return static_cast<size_t>(it - fields.begin());
    };
    auto has_nested = [&](std::string_view name) {
      return std::ranges::any_of(fields, [&](std::string const& field) {
        return is_nested_field(field, name);
      });
    };
    // New fields go after the last column of their parent record.
    auto end_of_parent
      = [&](this auto const& self, std::string_view name) -> size_t {
      auto dot = name.rfind('.');
      if (dot == std::string_view::npos) {
        return fields.size();
      }
      auto parent = name.substr(0, dot);
      for (auto i = fields.size(); i > 0; --i) {
        if (is_nested_field(fields[i - 1], parent)) {
          return i;
        }
      }
      return self(parent);
    };
    for (auto const& leaf : leaves()) {
      if (auto column = find(leaf.name)) {
        auto refined = refine_zeek_type(types[*column], leaf.type, false);
        if (not refined) {
          return false;
        }
        types[*column] = std::move(*refined);
        continue;
      }
      if (has_nested(leaf.name)) {
        // Only a `null` record fits fields nested below it.
        if (leaf.type != zeek_unknown_type) {
          return false;
        }
        continue;
      }
      // A field that was `null` so far may turn out to be a record.
      auto parent = Option<size_t>{};
      for (auto dot = leaf.name.find('.'); dot != std::string::npos;
           dot = leaf.name.find('.', dot + 1)) {
        parent = find(std::string_view{leaf.name}.substr(0, dot));
        if (parent) {
          break;
        }
      }
      if (parent) {
        if (types[*parent] != zeek_unknown_type) {
          return false;
        }
        fields[*parent] = leaf.name;
        types[*parent] = leaf.type;
        continue;
      }
      auto position = end_of_parent(leaf.name);
      fields.insert(fields.begin() + position, leaf.name);
      types.insert(types.begin() + position, leaf.type);
    }
    header.assign(std::move(fields), std::move(types));
    return true;
  }

  auto print_header(std::string& out) -> void {
    TENZIR_ASSERT(header_);
    auto it = std::back_inserter(out);
    if (std::exchange(written_, true)) {
      printer_.print_closing_line(it);
    }
    it = fmt::format_to(it,
                        "#separator \\x{0:02x}\n"
                        "#set_separator{0}{1}\n"
                        "#empty_field{0}{2}\n"
                        "#unset_field{0}{3}\n"
                        "#path{0}{4}",
                        printer_.sep, printer_.set_sep, printer_.empty_field,
                        printer_.unset_field, header_->path());
    if (not printer_.disable_timestamp_tags) {
      it = fmt::format_to(it, "\n#open{}{}", printer_.sep,
                          printer_.generate_timestamp());
    }
    it = fmt::format_to(it, "\n#fields");
    for (auto i = size_t{0}; i < header_->size(); ++i) {
      it = fmt::format_to(it, "{}{}", printer_.sep, header_->field(i));
    }
    it = fmt::format_to(it, "\n#types");
    for (auto i = size_t{0}; i < header_->size(); ++i) {
      it = fmt::format_to(it, "{}{}", printer_.sep, header_->type(i));
    }
    out.push_back('\n');
  }

  /// Prints the current row, which must fit the current header.
  auto print_row(std::string& out, diagnostic_handler& dh) -> void {
    TENZIR_ASSERT(header_);
    columns_.assign(header_->size(), nullptr);
    auto i = size_t{0};
    for (auto const& leaf : leaves()) {
      if (i < header_->size() and header_->field(i) == leaf.name) {
        columns_[i++] = &leaf;
        continue;
      }
      if (auto column = header_->find(leaf.name)) {
        columns_[*column] = &leaf;
        i = *column + 1;
        continue;
      }
      // A `null` record, which leaves all columns below it unset.
      TENZIR_ASSERT(leaf.type == zeek_unknown_type
                    and header_->has_nested(leaf.name));
    }
    for (auto column = size_t{0}; column < columns_.size(); ++column) {
      if (column > 0) {
        out.push_back(printer_.sep);
      }
      if (auto const* leaf = columns_[column]) {
        print_value(out, leaf->value, dh);
      } else {
        out.append(printer_.unset_field);
      }
    }
    out.push_back('\n');
  }

  auto print_value(std::string& out, nova::RowView<nova::Data> const& value,
                   diagnostic_handler& dh) -> void {
    match(
      value,
      [&](nova::RowView<nova::Null>) {
        out.append(printer_.unset_field);
      },
      [&](nova::RowView<nova::Record> const&) {
        // Only reachable for records in lists, all others are flattened.
        if (not std::exchange(warned_record_in_list_, true)) {
          diagnostic::warning("cannot write records in lists")
            .note("writing `{}` instead", printer_.unset_field)
            .primary(args_.operator_location)
            .emit(dh);
        }
        out.append(printer_.unset_field);
      },
      [&](nova::RowView<nova::List> const& list) {
        if (list.length() == 0) {
          out.append(printer_.empty_field);
          return;
        }
        auto first = true;
        for (auto element : list) {
          if (not std::exchange(first, false)) {
            out.push_back(printer_.set_sep);
          }
          print_value(out, element, dh);
        }
      },
      [&]<class T>(nova::RowView<T> const& x) {
        auto it = std::back_inserter(out);
        auto visitor = zeek_printer::visitor<decltype(it)>{it, printer_};
        visitor(*x);
      });
  }

  WriteZeekTsvArgs args_;
  zeek_printer printer_;
  /// The current header. It is written unless we are within `process`.
  Option<ZeekHeader> header_;
  /// Whether we wrote any header, so that the next one closes it.
  bool written_ = false;
  /// The flattened fields of the current row, of which the first `leaf_count_`
  /// are valid.
  std::vector<Leaf> leaves_;
  size_t leaf_count_ = 0;
  std::string prefix_;
  /// The row's leaf for every column of the header, if any.
  std::vector<Leaf const*> columns_;
  bool warned_mixed_list_ = false;
  bool warned_record_in_list_ = false;
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
    auto d = Describer<WriteZeekTsvArgs, WriteZeekTsv, WriteZeekTsvEvents>{};
    d.operator_location(&WriteZeekTsvArgs::operator_location);
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
