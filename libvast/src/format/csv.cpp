/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/format/csv.hpp"


#include <ostream>
#include <string_view>
#include <type_traits>

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/error.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast::format::csv {

namespace {

constexpr char separator = ',';

// TODO: agree on reasonable values
constexpr std::string_view set_separator = " | ";

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
caf::error render(output_iterator& out, ForwardIterator first,
                  ForwardIterator last) {
  if (first == last) {
    for (auto c : empty)
      *out++ = c;
    return caf::none;
  }
  if (auto err = render(out, *first))
    return err;
  for (++first; first != last; ++first) {
    for (auto c : set_separator)
      *out++ = c;
    if (auto err = render(out, *first))
      return err;
  }
  return caf::none;
}

caf::error render(output_iterator& out, const view<vector>& xs) {
  render(out, xs.begin(), xs.end());
  return caf::none;
}

caf::error render(output_iterator& out, const view<set>& xs) {
  render(out, xs.begin(), xs.end());
  return caf::none;
}

caf::error render(output_iterator&, const view<map>&) {
  return make_error(ec::unimplemented, "CSV writer does not support map types");
}

caf::error render(output_iterator& out, const view<data>& x) {
  return caf::visit([&](const auto& y) { return render(out, y); }, x);
}

} // namespace

caf::error writer::write(const table_slice& x) {
  // Print a new header each time we encounter a new layout.
  if (last_layout_ != x.layout().name()) {
    last_layout_ = x.layout().name();
    append("type");
    for (auto& field : x.layout().fields) {
      append(separator);
      append(field.name);
    }
    append('\n');
    write_buf();
  }
  // Print the cell contents.
  auto iter = std::back_inserter(buf_);
  for (size_t row = 0; row < x.rows(); ++row) {
    append(last_layout_);
    append(separator);
    if (auto err = render(iter, x.at(row, 0)))
      return err;
    for (size_t column = 1; column < x.columns(); ++column) {
      append(separator);
      if (auto err = render(iter, x.at(row, column)))
        return err;
    }
    append('\n');
    write_buf();
  }
  return caf::none;
}

const char* writer::name() const {
  return "csv-writer";
}

using namespace parser_literals;

reader::reader(caf::atom_value table_slice_type,
               std::unique_ptr<std::istream> in)
  : super(table_slice_type) {
  if (in != nullptr)
    reset(std::move(in));
}

void reader::reset(std::unique_ptr<std::istream> in) {
  VAST_ASSERT(in != nullptr);
  input_ = std::move(in);
  lines_ = std::make_unique<detail::line_range>(*input_);
}

caf::error reader::schema(vast::schema s) {
  schema_ = std::move(s);
  for (auto& t : s) {
    auto r = caf::get_if<record_type>(&t);
    if (!r)
      continue;
    schema_.add(flatten(*r));
  }
  return caf::none;
}

vast::schema reader::schema() const {
  return schema_;
}

const char* reader::name() const {
  return "csv-reader";
}

caf::optional<record_type>
reader::make_layout(const std::vector<std::string>& names) {
  for (auto& t : schema_) {
    if (auto r = caf::get_if<record_type>(&t)) {
      auto select_fields = [&]() -> caf::optional<record_type> {
        std::vector<record_field> result_raw;
        for (auto& name : names) {
          if (auto field = r->at(name))
            result_raw.emplace_back(name, *field);
          else
            return caf::none;
        }
        return record_type{result_raw};
      };
      if (auto result = select_fields())
        return result->name(r->name());
    } else if (names.size() == 1 && names[0] == t.name()) {
      // Hoist naked type into record.
      // TODO: Maybe this is actually not a good idea?
      return record_type{{t.name(), t}}.name(t.name());
    } // else skip
  }
  return caf::none;
}

namespace {

template <class Iterator, class Attribute>
struct container_parser_builder {
  using result_type = rule<Iterator, Attribute>;

  explicit container_parser_builder(const std::string& set_separator)
    : set_separator_{set_separator} {
    // nop
  }

  // TODO: support enumeration_type inside of containers too.
  template <class T>
  result_type operator()(const T& t) const {
    if constexpr (std::is_same_v<T, string_type>) {
      return +(parsers::any - set_separator_ - '=')->*[](std::string&& x) {
        return data{std::move(x)};
      };
    } else if constexpr (std::is_same_v<T, pattern_type>) {
      return +(parsers::any - set_separator_ - '=')->*[](std::string&& x) {
        return data{pattern{std::move(x)}};
      };
    } else if constexpr (std::is_same_v<T, set_type>) {
      auto set_insert = [](std::vector<Attribute>&& xs) {
        return set(std::make_move_iterator(xs.begin()),
                   std::make_move_iterator(xs.end()));
      };
      return ('{'_p >> (caf::visit(*this, t.value_type) % set_separator_)
              >> '}'_p)
               ->*set_insert;
    } else if constexpr (std::is_same_v<T, vector_type>) {
      auto vector_insert = [](std::vector<Attribute>&& xs) { return xs; };
      return ('[' >> (caf::visit(*this, t.value_type) % set_separator_) >> ']')
               ->*vector_insert;
    } else if constexpr (std::is_same_v<T, map_type>) {
      auto map_insert = [](std::vector<std::tuple<Attribute, Attribute>>&& xs) {
        auto to_pair = [](auto&& tuple) {
          return std::make_pair(std::get<0>(tuple), std::get<1>(tuple));
        };
        map m;
        for (auto& x : xs)
          m.insert(to_pair(std::move(x)));
        return m;
      };
      auto kvp = caf::visit(*this, t.key_type) >> '='
                 >> caf::visit(*this, t.value_type);
      return (('{'_p >> (kvp % set_separator_) >> '}')->*map_insert);
    } else if constexpr (has_parser_v<type_to_data<T>>) {
      using value_type = type_to_data<T>;
      auto ws = ignore(*parsers::space);
      return (ws >> make_parser<value_type>{} >> ws)->*[](value_type&& x) {
        return x;
      };
    } else {
      VAST_ERROR_ANON("csv parser builder faild to fetch a parser for type",
                      caf::detail::pretty_type_name(typeid(T)));
      return {};
    }
  }

  std::string set_separator_;
};

template <class Iterator>
struct csv_parser_factory {
  using result_type = type_erased_parser<Iterator>;

  csv_parser_factory(const std::string& set_separator,
                     table_slice_builder_ptr bptr)
    : set_separator_{set_separator}, bptr_{std::move(bptr)} {
    // nop
  }

  struct identity {
    template <class T>
    constexpr T operator()(T&& t) const noexcept {
      return std::forward<T>(t);
    }
  };

  template <class T, class F>
  struct add_t {
    void operator()(caf::optional<T>&& x) const {
      if (x) {
        const auto tmp = f_(*x);
        bptr_->add(make_data_view(tmp));
      } else
        bptr_->add(make_data_view(caf::none));
    }
    table_slice_builder_ptr bptr_;
    F f_;
  };

  template <class T>
  static add_t<T, identity> make_add_t(table_slice_builder_ptr bptr) {
    return add_t<T, identity>{std::move(bptr), identity{}};
  }

  template <class T, class F>
  static add_t<T, F> make_add_t(table_slice_builder_ptr bptr, F f) {
    return add_t<T, F>{std::move(bptr), std::move(f)};
  }

  template <class T>
  result_type operator()(const T& t) const {
    if constexpr (std::is_same_v<T, string_type>) {
      return (-+(parsers::any - set_separator_))
               ->*make_add_t<std::string>(bptr_);
    } else if constexpr (std::is_same_v<T, pattern_type>) {
      auto to_pattern = [](const std::string& s) { return pattern{s}; };
      return (-+(parsers::any - set_separator_))
               ->*make_add_t<std::string>(bptr_, to_pattern);
    } else if constexpr (std::is_same_v<T, enumeration_type>) {
      auto to_enumeration =
        [t](const std::string& s) -> caf::optional<enumeration> {
        auto i = std::find(t.fields.begin(), t.fields.end(), s);
        if (i == t.fields.end()) {
          VAST_WARNING_ANON("csv reader failed to parse unexpected enum value",
                            s);
          return caf::none;
        }
        return std::distance(t.fields.begin(), i);
      };
      return (-+(parsers::any - set_separator_))
               ->*make_add_t<std::string>(bptr_, to_enumeration);
    } else if constexpr (detail::contains_v<
                           T, std::tuple<set_type, vector_type, map_type>>) {
      return (-container_parser_builder<Iterator, data>{set_separator_}(t))
               ->*make_add_t<data>(bptr_);
    } else if constexpr (has_parser_v<type_to_data<T>>) {
      using value_type = type_to_data<T>;
      return (-make_parser<value_type>{})->*make_add_t<value_type>(bptr_);
    } else {
      VAST_ERROR_ANON("csv parser builder failed to fetch a parser for type",
                      caf::detail::pretty_type_name(typeid(T)));
      return {};
    }
  }

  std::string set_separator_;
  table_slice_builder_ptr bptr_;
};

template <class Iterator>
caf::optional<type_erased_parser<Iterator>>
make_csv_parser(const record_type& layout, table_slice_builder_ptr builder) {
  auto num_fields = layout.fields.size();
  VAST_ASSERT(num_fields > 0);
  auto factory = csv_parser_factory<Iterator>{",", builder};
  auto result = caf::visit(factory, layout.fields[0].type);
  for (size_t i = 1; i < num_fields; ++i) {
    auto p = (caf::visit(factory, layout.fields[i].type));
    result = (result >> ','
              >> std::move(p)) /*->* [](std::tuple<bool, bool>&& bb) {
return std::get<0>(bb) && std::get<1>(bb);
}*/;
  }
  return result;
}

} // namespace

caf::error reader::read_header(std::string_view line) {
  auto ws = ignore(*parsers::space);
  auto p = (ws >> schema_parser::id >> ws) % ',';
  std::vector<std::string> columns;
  auto b = line.begin();
  auto f = b;
  if (!p(f, line.end(), columns))
    return make_error(ec::parse_error, "unable to parse csv header");
  auto layout = make_layout(columns);
  if (!layout)
    return make_error(ec::parse_error, "unable to derive a layout");
  VAST_DEBUG_ANON("csv_reader derived layout", to_string(*layout));
  if (!reset_builder(*layout))
    return make_error(ec::parse_error, "unable to create a builder for layout");
  parser_ = make_csv_parser<iterator_type>(*layout, builder_);
  if (!parser_)
    return make_error(ec::parse_error, "unable generate a parser");
  return caf::none;
}

caf::error reader::read_impl(size_t max_events, size_t max_slice_size,
                             consumer& cons) {
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(max_slice_size > 0);
  if (!parser_)
    if (auto err = read_header(lines_->get()))
      return err;
  auto& p = *parser_;
  size_t produced = 0;
  lines_->next();
  for (; produced < max_events; lines_->next()) {
    // EOF check.
    if (lines_->done())
      return finish(cons, make_error(ec::end_of_input, "input exhausted"));
    auto& line = lines_->get();
    if (!p(line)) {
      return make_error(ec::type_clash, "unable to parse CSV line");
    }
    ++produced;
    if (builder_->rows() == max_slice_size)
      if (auto err = finish(cons))
        return err;
  }
  return finish(cons);
}

} // namespace vast::format::csv
