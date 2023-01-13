//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/csv.hpp"

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/view.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/module.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include <caf/settings.hpp>

#include <optional>
#include <ostream>
#include <string_view>
#include <type_traits>

namespace vast::format::csv {

namespace {

/// A parser that passes the result of the qqstr parser to a nested parser.
template <parser NestedParser>
class quoted_parser : public parser_base<quoted_parser<NestedParser>> {
public:
  using attribute = typename NestedParser::attribute;

  explicit quoted_parser(NestedParser parser) : parser_(std::move(parser)) {
    // nop
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    if (std::string buffer; parsers::qqstr(f, l, buffer))
      return parser_(buffer, x);
    return false;
  }

  NestedParser parser_;
};

constexpr std::string_view empty = "\"\"";

using output_iterator = std::back_insert_iterator<std::vector<char>>;

caf::error render(output_iterator&, const view<caf::none_t>&) {
  return caf::none;
}

template <class T>
caf::error render(output_iterator& out, const T& x) {
  make_printer<T>{}.print(out, x);
  return caf::none;
}

caf::error render(output_iterator& out, const view<real>& x) {
  real_printer<real, 6>{}.print(out, x);
  return caf::none;
}

caf::error render(output_iterator& out, const view<std::string>& x) {
  auto escaper = detail::make_double_escaper("\"|");
  auto p = '"' << printers::escape(escaper) << '"';
  p.print(out, x);
  return caf::none;
}

caf::error render(output_iterator& out, const view<data>& x);

template <class ForwardIterator>
caf::error
render(output_iterator& out, ForwardIterator first, ForwardIterator last) {
  if (first == last) {
    for (auto c : empty)
      *out++ = c;
    return caf::none;
  }
  if (auto err = render(out, *first))
    return err;
  for (++first; first != last; ++first) {
    for (auto c : writer::defaults::set_separator)
      *out++ = c;
    if (auto err = render(out, *first))
      return err;
  }
  return caf::none;
}

caf::error render(output_iterator& out, const view<list>& xs) {
  return render(out, xs.begin(), xs.end());
}

caf::error render(output_iterator&, const view<map>&) {
  return caf::make_error(ec::unimplemented, "CSV writer does not support map "
                                            "types");
}

caf::error render(output_iterator& out, const view<data>& x) {
  return caf::visit(
    [&](const auto& y) {
      return render(out, y);
    },
    x);
}

std::optional<char> handle_escaped_separator(char separator) {
  switch (separator) {
    case 't':
      return '\t';
    case 'n':
      return '\n';
  }
  return {};
}

std::optional<char>
get_separator_from_option_string(const std::string& option) {
  if (option.empty())
    return writer::defaults::separator;
  if (option.size() == 1)
    return option.front();
  if (option.size() > 2 && option.starts_with('"') && option.ends_with('"')) {
    const auto sub_str
      = std::string_view{std::cbegin(option) + 1, std::cend(option) - 1};
    if (sub_str.size() == 1)
      return sub_str.front();
    if (sub_str.size() == 2 && sub_str.front() == '\\')
      return handle_escaped_separator(sub_str.back());
  }
  return {};
}

} // namespace

writer::writer(ostream_ptr out, const caf::settings&) : super{std::move(out)} {
  // nop
}

caf::error writer::write(const table_slice& x) {
  constexpr char separator = writer::defaults::separator;
  // Print a new header each time we encounter a new schema.
  const auto& schema = x.schema();
  const auto& rschema = caf::get<record_type>(schema);
  if (last_schema_ != schema.name()) {
    last_schema_ = schema.name();
    append("type");
    for (const auto& [_, index] : rschema.leaves()) {
      append(separator);
      append(rschema.key(index));
    }
    append('\n');
    write_buf();
  }
  // Print the cell contents.
  auto iter = std::back_inserter(buf_);
  for (size_t row = 0; row < x.rows(); ++row) {
    append(last_schema_);
    size_t column = 0;
    for (const auto& [field, _] : rschema.leaves()) {
      append(separator);
      if (auto err = render(iter, to_canonical(field.type,
                                               x.at(row, column, field.type))))
        return err;
      ++column;
    }
    VAST_ASSERT(column == x.columns());
    append('\n');
    write_buf();
  }
  return caf::none;
}

const char* writer::name() const {
  return "csv-writer";
}

using namespace parser_literals;

reader::reader(const caf::settings& options, std::unique_ptr<std::istream> in)
  : super(options) {
  if (in != nullptr)
    reset(std::move(in));
  using defaults = vast::defaults::import::csv;
  opt_.separator = defaults::separator[0];
  auto seperator_option
    = get_or(options, "vast.import.csv.separator", defaults::separator.data());
  if (auto separator = get_separator_from_option_string(seperator_option))
    opt_.separator = *separator;
  else
    VAST_WARN("{} unable to utilize vast.import.csv.separator '{}'. Using "
              "default comma instead",
              detail::pretty_type_name(*this), seperator_option);
  opt_.set_separator = get_or(options, "vast.import.csv.set_separator",
                              defaults::set_separator.data());
  opt_.kvp_separator = get_or(options, "vast.import.csv.kvp_separator",
                              defaults::kvp_separator.data());
}

void reader::reset(std::unique_ptr<std::istream> in) {
  VAST_ASSERT(in != nullptr);
  input_ = std::move(in);
  lines_ = std::make_unique<detail::line_range>(*input_);
}

caf::error reader::module(vast::module m) {
  for (const auto& t : m)
    module_.add(t);
  return caf::none;
}

vast::module reader::module() const {
  return module_;
}

const char* reader::name() const {
  return "csv-reader";
}

caf::optional<type>
reader::make_schema(const std::vector<std::string>& names, bool first_run) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(names));
  for (const auto& t : module_) {
    if (const auto* r = caf::get_if<record_type>(&t)) {
      auto select_fields = [&]() -> caf::optional<type> {
        std::vector<record_type::field_view> result_raw;
        result_raw.reserve(names.size());
        auto matched_once = false;
        for (const auto& name : names) {
          if (auto index = r->resolve_key(name)) {
            matched_once = true;
            result_raw.push_back({
              name,
              r->field(*index).type,
            });
          } else if (!first_run) {
            result_raw.push_back({
              name,
              type{string_type{}, {{"skip"}}},
            });
          } else {
            return caf::none;
          }
        }
        if (!matched_once)
          return caf::none;
        auto result = type{record_type{result_raw}};
        result.assign_metadata(t);
        return result;
      };
      if (auto result = select_fields())
        return result;
    } else if (names.size() == 1 && names[0] == t.name()) {
      // Hoist naked type into record.
      return type{
        t.name(),
        record_type{
          {t.name(), t},
        },
      };
    } // else skip
  }
  if (!first_run)
    return caf::none;
  return make_schema(names, false);
}

namespace {

template <class Iterator, class Attribute>
struct container_parser_builder {
  // In case we run into performance issue with the parsers
  // generated here, `rule` could be replaced by `type_erased_parser`
  // to eliminated one indirection. This requires attribute support
  // in `type_erased_parser`.
  using result_type = rule<Iterator, Attribute>;

  explicit container_parser_builder(options opt) : opt_{std::move(opt)} {
    // nop
  }

  template <concrete_type T>
  result_type operator()(const T& t) const {
    if constexpr (std::is_same_v<T, string_type>) {
      // clang-format off
      return +(parsers::any - opt_.separator - opt_.set_separator - opt_.kvp_separator) ->* [](std::string x) {
        return data{std::move(x)};
      };
    } else if constexpr (std::is_same_v<T, pattern_type>) {
      return +(parsers::any - opt_.separator - opt_.set_separator - opt_.kvp_separator) ->* [](std::string x) {
        return data{pattern{std::move(x)}};
      };
      // clang-format on
    } else if constexpr (std::is_same_v<T, enumeration_type>) {
      auto to_enumeration = [t](std::string s) -> caf::optional<Attribute> {
        for (const auto& [canonical, internal] : t.fields())
          if (s == canonical)
            return detail::narrow_cast<enumeration>(internal);
        return caf::none;
      };
      return (+(parsers::any - opt_.set_separator - opt_.kvp_separator))
        .with(to_enumeration);
    } else if constexpr (std::is_same_v<T, list_type>) {
      auto list_insert = [](std::vector<Attribute> xs) {
        return xs;
      };
      return ('[' >> ~(caf::visit(*this, t.value_type()) % opt_.set_separator)
              >> ']')
               ->*list_insert;
      // clang-format on
    } else if constexpr (std::is_same_v<T, map_type>) {
      auto ws = ignore(*(parsers::space - opt_.separator));
      auto map_insert = [](std::vector<std::pair<Attribute, Attribute>> xs) {
        return map(std::make_move_iterator(xs.begin()),
                   std::make_move_iterator(xs.end()));
      };
      // clang-format off
      auto kvp =
        caf::visit(*this, t.key_type()) >> ws >> opt_.kvp_separator >> ws >> caf::visit(*this, t.value_type());
      return (ws >> '{' >> ws >> (kvp % (ws >> opt_.set_separator >> ws)) >> ws >> '}' >> ws) ->* map_insert;
    } else if constexpr (std::is_same_v<T, double_type>) {
      // The default parser for real's requires the dot, so we special-case the
      // real parser here.
      auto ws = ignore(*parsers::space - opt_.separator);
      return (ws >> parsers::real >> ws) ->* [](real x) {
        return x;
      };
    } else if constexpr (registered_parser_type<type_to_data_t<T>>) {
      using value_type = type_to_data_t<T>;
      auto ws = ignore(*(parsers::space - opt_.separator));
      return (ws >> make_parser<value_type>{} >> ws) ->* [](value_type x) {
        return x;
      };
      // clang-format on
    } else {
      VAST_ERROR("csv parser builder failed to fetch a parser for type "
                 "{}",
                 caf::detail::pretty_type_name(typeid(T)));
      return {};
    }
  }

  options opt_;
};

template <class Iterator>
struct csv_parser_factory {
  using result_type = reader::parser_type;

  csv_parser_factory(options opt, table_slice_builder_ptr bptr)
    : opt_{std::move(opt)}, bptr_{std::move(bptr)} {
    // nop
  }

  template <class T>
  struct add_t {
    bool operator()(const caf::optional<T>& x) const {
      return bptr_->add(make_data_view(x));
    }
    table_slice_builder_ptr bptr_;
  };

  result_type operator()(const type& t) const {
    auto f = [&]<concrete_type U>(const U& u) -> result_type {
      [[maybe_unused]] const auto field
        = parsers::qqstr
          | +(parsers::any - opt_.separator - opt_.set_separator);
      if constexpr (std::is_same_v<U, duration_type>) {
        auto make_duration_parser = [&](auto period) {
          // clang-format off
        return (-parsers::real ->* [](double x) {
          using period_type = decltype(period);
          using double_duration = std::chrono::duration<double, period_type>;
          return std::chrono::duration_cast<duration>(double_duration{x});
        }).with(add_t<duration>{bptr_});
          // clang-format on
        };
        if (auto unit = t.attribute("unit")) {
          if (*unit == "ns")
            return make_duration_parser(std::nano{});
          if (*unit == "us")
            return make_duration_parser(std::micro{});
          if (*unit == "ms")
            return make_duration_parser(std::milli{});
          if (*unit == "s")
            return make_duration_parser(std::ratio<1>{});
          if (*unit == "min")
            return make_duration_parser(std::ratio<60>{});
          if (*unit == "h")
            return make_duration_parser(std::ratio<3600>{});
          if (*unit == "d")
            return make_duration_parser(std::ratio<86400>{});
        }
        // If we do not have an explicit unit given, we require the unit suffix.
        return (-(quoted_parser{parsers::duration} | parsers::duration))
          .with(add_t<duration>{bptr_});
      } else if constexpr (std::is_same_v<U, string_type>) {
        return (-field).with(add_t<std::string>{bptr_});
      } else if constexpr (std::is_same_v<U, pattern_type>) {
        return (-as<pattern>(as<std::string>(field)))
          .with(add_t<pattern>{bptr_});
      } else if constexpr (std::is_same_v<U, enumeration_type>) {
        auto to_enumeration = [u](std::string s) -> caf::optional<enumeration> {
          for (const auto& [canonical, internal] : u.fields())
            if (s == canonical)
              return detail::narrow_cast<enumeration>(internal);
          return caf::none;
        };
        // clang-format off
      return (field ->* to_enumeration).with(add_t<enumeration>{bptr_});
        // clang-format on
      } else if constexpr (detail::is_any_v<U, list_type, map_type>) {
        auto pb = container_parser_builder<Iterator, data>{opt_};
        return (-caf::visit(pb, t)).with(add_t<data>{bptr_});
      } else if constexpr (std::is_same_v<U, double_type>) {
        // The default parser for real's requires the dot, so we special-case
        // the real parser here.
        const auto& p = parsers::real;
        return (-(quoted_parser{p} | p)).with(add_t<real>{bptr_});
      } else if constexpr (registered_parser_type<type_to_data_t<U>>) {
        using value_type = type_to_data_t<U>;
        auto p = make_parser<value_type>{};
        return (-(quoted_parser{p} | p)).with(add_t<value_type>{bptr_});
      } else {
        VAST_ERROR("csv parser builder failed to fetch a parser for type "
                   "{}",
                   caf::detail::pretty_type_name(typeid(U)));
        return {};
      }
    };
    return caf::visit(f, t);
  }

  options opt_;
  table_slice_builder_ptr bptr_;
};

template <class Iterator>
caf::optional<reader::parser_type>
make_csv_parser(const record_type& schema, table_slice_builder_ptr builder,
                const options& opt) {
  auto num_fields = schema.num_fields();
  VAST_ASSERT(num_fields > 0);
  auto factory = csv_parser_factory<Iterator>{opt, builder};
  auto result = factory(schema.field(0).type);
  for (size_t i = 1; i < num_fields; ++i) {
    auto p = factory(schema.field(i).type);
    result = result >> opt.separator >> std::move(p);
  }
  return result;
}

} // namespace

vast::system::report reader::status() const {
  using namespace std::string_literals;
  uint64_t num_lines = num_lines_;
  uint64_t invalid_lines = num_invalid_lines_;
  if (num_invalid_lines_ > 0)
    VAST_WARN("{} failed to parse {} of {} recent lines",
              detail::pretty_type_name(this), num_invalid_lines_, num_lines_);
  num_lines_ = 0;
  num_invalid_lines_ = 0;
  return {.data = {
            {name() + ".num-lines"s, num_lines},
            {name() + ".invalid-lines"s, invalid_lines},
          }};
}

caf::expected<reader::parser_type> reader::read_header(std::string_view line) {
  auto ws = ignore(*(parsers::space - opt_.separator));
  auto column_name = parsers::qqstr | +(parsers::printable - opt_.separator);
  auto p = (ws >> column_name >> ws) % opt_.separator;
  std::vector<std::string> columns;
  const auto* b = line.begin();
  const auto* f = b;
  if (!p(f, line.end(), columns))
    return caf::make_error(ec::parse_error, "unable to parse csv header");
  auto schema = make_schema(columns);
  if (!schema)
    return caf::make_error(ec::parse_error, "unable to derive a schema");
  VAST_DEBUG("csv_reader derived schema {}", *schema);
  if (!reset_builder(*schema))
    return caf::make_error(ec::parse_error, "unable to create a builder for "
                                            "schema");
  auto parser = make_csv_parser<iterator_type>(caf::get<record_type>(*schema),
                                               builder_, opt_);
  if (!parser)
    return caf::make_error(ec::parse_error, "unable to generate a parser");
  return *parser;
}

caf::error reader::read_impl(size_t max_events, size_t max_slice_size,
                             consumer& callback) {
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(max_slice_size > 0);
  auto next_line = [&] {
    auto timed_out = lines_->next_timeout(read_timeout_);
    if (timed_out)
      VAST_DEBUG("{} reached input timeout at line {}",
                 detail::pretty_type_name(this), lines_->line_number());
    return timed_out;
  };
  if (!parser_) {
    bool timed_out = next_line();
    if (timed_out)
      return ec::stalled;
    auto p = read_header(lines_->get());
    if (!p)
      return p.error();
    parser_ = *std::move(p);
  }
  auto& p = *parser_;
  size_t produced = 0;
  while (produced < max_events) {
    // EOF check.
    if (lines_->done())
      return finish(callback, caf::make_error(ec::end_of_input, //
                                              "input exhausted"));
    if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
        && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
      VAST_DEBUG("{} reached batch timeout", detail::pretty_type_name(this));
      return finish(callback, ec::timeout);
    }
    bool timed_out = next_line();
    if (timed_out)
      return ec::stalled;
    auto& line = lines_->get();
    if (line.empty()) {
      // Ignore empty lines.
      VAST_DEBUG("{} ignores empty line at {}", detail::pretty_type_name(this),
                 lines_->line_number());
      continue;
    }
    ++num_lines_;
    if (!p(line)) {
      if (num_invalid_lines_ == 0)
        VAST_WARN("{} failed to parse line {}: {}",
                  detail::pretty_type_name(this), lines_->line_number(), line);
      ++num_invalid_lines_;
      continue;
    }
    ++produced;
    ++batch_events_;
    if (builder_->rows() == max_slice_size)
      if (auto err = finish(callback))
        return err;
  }
  return finish(callback);
}

} // namespace vast::format::csv
