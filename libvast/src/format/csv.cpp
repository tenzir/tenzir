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
#include "vast/concept/printable/vast/legacy_type.hpp"
#include "vast/concept/printable/vast/view.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include <caf/settings.hpp>

#include <ostream>
#include <string_view>
#include <type_traits>

namespace vast::format::csv {

namespace {

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
  render(out, xs.begin(), xs.end());
  return caf::none;
}

caf::error render(output_iterator&, const view<map>&) {
  return caf::make_error(ec::unimplemented, "CSV writer does not support map "
                                            "types");
}

caf::error render(output_iterator& out, const view<data>& x) {
  return caf::visit([&](const auto& y) { return render(out, y); }, x);
}

} // namespace

writer::writer(ostream_ptr out, const caf::settings&) : super{std::move(out)} {
  // nop
}

caf::error writer::write(const table_slice& x) {
  constexpr char separator = writer::defaults::separator;
  // Print a new header each time we encounter a new layout.
  auto&& layout = x.layout();
  if (last_layout_ != layout.name()) {
    last_layout_ = layout.name();
    append("type");
    for (auto& field : legacy_record_type::each(layout)) {
      append(separator);
      append(field.key());
    }
    append('\n');
    write_buf();
  }
  // Print the cell contents.
  auto iter = std::back_inserter(buf_);
  for (size_t row = 0; row < x.rows(); ++row) {
    append(last_layout_);
    size_t column = 0;
    for (const auto& field : legacy_record_type::each{layout}) {
      append(separator);
      if (auto err = render(iter, x.at(row, column, field.type())))
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
  opt_.separator
    = get_or(options, "vast.import.csv.separator", defaults::separator);
  opt_.set_separator
    = get_or(options, "vast.import.csv.set_separator", defaults::set_separator);
  opt_.kvp_separator
    = get_or(options, "vast.import.csv.kvp_separator", defaults::kvp_separator);
}

void reader::reset(std::unique_ptr<std::istream> in) {
  VAST_ASSERT(in != nullptr);
  input_ = std::move(in);
  lines_ = std::make_unique<detail::line_range>(*input_);
}

caf::error reader::schema(vast::schema s) {
  for (auto& t : s) {
    if (auto r = caf::get_if<legacy_record_type>(&t))
      schema_.add(*r);
    else
      schema_.add(t);
  }
  return caf::none;
}

vast::schema reader::schema() const {
  return schema_;
}

const char* reader::name() const {
  return "csv-reader";
}

caf::optional<legacy_record_type>
reader::make_layout(const std::vector<std::string>& names) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(names));
  for (auto& t : schema_) {
    if (auto r = caf::get_if<legacy_record_type>(&t)) {
      auto select_fields = [&]() -> caf::optional<legacy_record_type> {
        std::vector<record_field> result_raw;
        for (auto& name : names) {
          if (auto field = r->at(name))
            result_raw.emplace_back(name, field->type);
          else
            return caf::none;
        }
        return legacy_record_type{std::move(result_raw)}
          .name(r->name())
          .attributes(r->attributes());
      };
      if (auto result = select_fields())
        return result;
    } else if (names.size() == 1 && names[0] == t.name()) {
      // Hoist naked type into record.
      return legacy_record_type{{t.name(), t}}.name(t.name());
    } // else skip
  }
  return caf::none;
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

  template <class T>
  result_type operator()(const T& t) const {
    if constexpr (std::is_same_v<T, legacy_alias_type>) {
      return caf::visit(*this, t.value_type);
    } else if constexpr (std::is_same_v<T, legacy_string_type>) {
      // clang-format off
      return +(parsers::any - opt_.set_separator - opt_.kvp_separator) ->* [](std::string x) {
        return data{std::move(x)};
      };
    } else if constexpr (std::is_same_v<T, legacy_pattern_type>) {
      return +(parsers::any - opt_.set_separator - opt_.kvp_separator) ->* [](std::string x) {
        return data{pattern{std::move(x)}};
      };
      // clang-format on
    } else if constexpr (std::is_same_v<T, legacy_enumeration_type>) {
      auto to_enumeration = [t](std::string s) -> caf::optional<Attribute> {
        auto i = std::find(t.fields.begin(), t.fields.end(), s);
        if (i == t.fields.end()) {
          VAST_WARN("csv reader failed to parse unexpected enum value {}", s);
          return caf::none;
        }
        return detail::narrow_cast<enumeration>(
          std::distance(t.fields.begin(), i));
      };
      return (+(parsers::any - opt_.set_separator - opt_.kvp_separator))
        .with(to_enumeration);
    } else if constexpr (std::is_same_v<T, legacy_list_type>) {
      auto list_insert = [](std::vector<Attribute> xs) { return xs; };
      return ('[' >> ~(caf::visit(*this, t.value_type) % opt_.set_separator)
              >> ']')
               ->*list_insert;
      // clang-format on
    } else if constexpr (std::is_same_v<T, legacy_map_type>) {
      auto ws = ignore(*parsers::space);
      auto map_insert = [](std::vector<std::pair<Attribute, Attribute>> xs) {
        return map(std::make_move_iterator(xs.begin()),
                   std::make_move_iterator(xs.end()));
      };
      // clang-format off
      auto kvp =
        caf::visit(*this, t.key_type) >> ws >> opt_.kvp_separator >> ws >> caf::visit(*this, t.value_type);
      return (ws >> '{' >> ws >> (kvp % (ws >> opt_.set_separator >> ws)) >> ws >> '}' >> ws) ->* map_insert;
    } else if constexpr (registered_parser_type<type_to_data<T>>) {
      using value_type = type_to_data<T>;
      auto ws = ignore(*parsers::space);
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

  template <class T>
  result_type operator()(const T& t) const {
    [[maybe_unused]] const auto field
      = parsers::qqstr | +(parsers::any - opt_.set_separator);
    if constexpr (std::is_same_v<T, legacy_alias_type>) {
      return caf::visit(*this, t.value_type);
    } else if constexpr (std::is_same_v<T, legacy_duration_type>) {
      auto make_duration_parser = [&](auto period) {
        // clang-format off
        return (-parsers::real_opt_dot ->* [](double x) {
          using period_type = decltype(period);
          using double_duration = std::chrono::duration<double, period_type>;
          return std::chrono::duration_cast<duration>(double_duration{x});
        }).with(add_t<duration>{bptr_});
        // clang-format on
      };
      if (auto attr = find_attribute(t, "unit")) {
        if (auto unit = attr->value) {
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
      }
      // If we do not have an explicit unit given, we require the unit suffix.
      return (-parsers::duration).with(add_t<duration>{bptr_});
    } else if constexpr (std::is_same_v<T, legacy_string_type>) {
      return (-field).with(add_t<std::string>{bptr_});
    } else if constexpr (std::is_same_v<T, legacy_pattern_type>) {
      return (-as<pattern>(as<std::string>(+(parsers::any - opt_.separator))))
        .with(add_t<pattern>{bptr_});
    } else if constexpr (std::is_same_v<T, legacy_enumeration_type>) {
      auto to_enumeration = [t](std::string s) -> caf::optional<enumeration> {
        auto i = std::find(t.fields.begin(), t.fields.end(), s);
        if (i == t.fields.end()) {
          VAST_WARN("csv reader failed to parse unexpected enum value {}", s);
          return caf::none;
        }
        return detail::narrow_cast<enumeration>(
          std::distance(t.fields.begin(), i));
      };
      // clang-format off
      return (field ->* to_enumeration).with(add_t<enumeration>{bptr_});
      // clang-format on
    } else if constexpr (detail::is_any_v<T, legacy_list_type, legacy_map_type>) {
      return (-container_parser_builder<Iterator, data>{opt_}(t))
        .with(add_t<data>{bptr_});
    } else if constexpr (registered_parser_type<type_to_data<T>>) {
      using value_type = type_to_data<T>;
      return (-make_parser<value_type>{}).with(add_t<value_type>{bptr_});
    } else {
      VAST_ERROR("csv parser builder failed to fetch a parser for type "
                 "{}",
                 caf::detail::pretty_type_name(typeid(T)));
      return {};
    }
  }

  options opt_;
  table_slice_builder_ptr bptr_;
};

template <class Iterator>
caf::optional<reader::parser_type>
make_csv_parser(const legacy_record_type& layout,
                table_slice_builder_ptr builder, const options& opt) {
  auto num_fields = layout.fields.size();
  VAST_ASSERT(num_fields > 0);
  auto factory = csv_parser_factory<Iterator>{opt, builder};
  auto result = caf::visit(factory, layout.fields[0].type);
  for (size_t i = 1; i < num_fields; ++i) {
    auto p = caf::visit(factory, layout.fields[i].type);
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
  return {
    {name() + ".num-lines"s, num_lines},
    {name() + ".invalid-lines"s, invalid_lines},
  };
}

caf::expected<reader::parser_type> reader::read_header(std::string_view line) {
  auto ws = ignore(*parsers::space);
  auto column_name = parsers::qqstr | +(parsers::printable - opt_.separator);
  auto p = (ws >> column_name >> ws) % opt_.separator;
  std::vector<std::string> columns;
  auto b = line.begin();
  auto f = b;
  if (!p(f, line.end(), columns))
    return caf::make_error(ec::parse_error, "unable to parse csv header");
  auto&& layout = make_layout(columns);
  if (!layout)
    return caf::make_error(ec::parse_error, "unable to derive a layout");
  VAST_DEBUG("csv_reader derived layout {}", to_string(*layout));
  if (!reset_builder(*layout))
    return caf::make_error(ec::parse_error, "unable to create a builder for "
                                            "layout");
  auto parser = make_csv_parser<iterator_type>(*layout, builder_, opt_);
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
