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

#pragma once

#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/json.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/format/multi_layout_reader.hpp"
#include "vast/format/printer_writer.hpp"
#include "vast/json.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/view.hpp"

namespace vast::format::json {

struct event_printer : printer<event_printer> {
  using attribute = event;

  template <class Iterator>
  bool print(Iterator& out, const event& e) const {
    vast::json j;
    if (!convert(e.data(), j, e.type()))
      return false;
    return printers::json<policy::oneline>.print(out, j);
  }
};

class writer : public printer_writer<event_printer>{
public:
  using printer_writer<event_printer>::printer_writer;

  const char* name() const {
    return "json-writer";
  }
};

struct default_selector {
  caf::optional<vast::record_type> operator()(const vast::json::object&) {
    return t;
  }

  caf::error schema(vast::schema sch) {
    auto entry = *sch.begin();
    if (!caf::holds_alternative<vast::record_type>(entry))
      return make_error(vast::ec::invalid_configuration,
                        "only record_types supported for json schema");
    t = flatten(caf::get<vast::record_type>(entry));
    return caf::none;
  }

  vast::schema schema() const {
    vast::schema result;
    if (t)
      result.add(*t);
    return result;
  }

  static const char* name() {
    return "json-reader";
  }

  caf::optional<vast::record_type> t = caf::none;
};

template <class Selector = default_selector>
class reader final : public multi_layout_reader {
public:
  using super = multi_layout_reader;

  /// Constructs a json reader.
  /// @param input The stream of logs to read.
  explicit reader(caf::atom_value table_slice_type,
                  std::unique_ptr<std::istream> in = nullptr);

  void reset(std::unique_ptr<std::istream> in);

  caf::error schema(vast::schema sch) override;

  caf::error schema(vast::type, vast::schema);

  vast::schema schema() const override;

  const char* name() const override;

protected:
  caf::error read_impl(size_t max_events, size_t max_slice_size,
                       consumer& f) override;

private:
  using iterator_type = std::string_view::const_iterator;

  // void patch(std::vector<data>& xs);

  Selector selector;
  std::unique_ptr<std::istream> input_;
  std::unique_ptr<vast::detail::line_range> lines_;
  caf::optional<size_t> proto_field_;
  std::vector<size_t> port_fields_;
};

template <class Selector>
reader<Selector>::reader(caf::atom_value table_slice_type,
                         std::unique_ptr<std::istream> in)
  : super(table_slice_type) {
  if (in != nullptr)
    reset(std::move(in));
}

template <class Selector>
void reader<Selector>::reset(std::unique_ptr<std::istream> in) {
  VAST_ASSERT(in != nullptr);
  input_ = std::move(in);
  lines_ = std::make_unique<vast::detail::line_range>(*input_);
}

template <class Selector>
caf::error reader<Selector>::schema(vast::schema s) {
  return selector.schema(std::move(s));
}

template <class Selector>
vast::schema reader<Selector>::schema() const {
  return selector.schema();
}

template <class Selector>
const char* reader<Selector>::name() const {
  return Selector::name();
}

template <class F>
struct convert {
  convert(F&& f) : f_{std::forward<F>(f)} {
  }

  bool operator()(vast::json::number n, const vast::count_type&) const {
    auto c = vast::count(n);
    return f_(std::move(c));
  }
  bool operator()(vast::json::number n, const vast::timespan_type&) const {
    std::chrono::duration<vast::json::number> x{n};
    auto t = std::chrono::duration_cast<vast::timespan>(x);
    return f_(std::move(t));
  }
  bool operator()(vast::json::string s, const vast::timestamp_type&) const {
    vast::timestamp t;
    if (!vast::parsers::timestamp(s, t))
      return false;
    return f_(std::move(t));
  }
  bool operator()(vast::json::boolean b, const vast::boolean_type&) const {
    return f_(std::move(b));
  }
  bool operator()(vast::json::number n, const vast::port_type&) const {
    auto p = vast::port(n);
    return f_(std::move(p));
  }
  bool operator()(vast::json::string s, const vast::address_type&) const {
    vast::address a;
    if (!vast::parsers::addr(s, a))
      return false;
    return f_(std::move(a));
  }
  bool operator()(vast::json::array a, const vast::vector_type& v) const {
    vast::vector d;
    auto run = [&](auto value) {
      d.push_back(std::move(value));
      return true;
    };
    auto c = convert<decltype(run)>{std::move(run)};
    for (auto x : a)
      c(x, v.value_type);
    return f_(std::move(d));
  }
  bool operator()(vast::json::string s, const vast::string_type&) const {
    return f_(std::move(s));
  }
  template <class T, class U>
  bool operator()(T, U) const {
    VAST_ASSERT(!"this line should never be reached");
    return false;
  };

  F f_;
};

template <class Selector>
caf::error reader<Selector>::read_impl(size_t max_events, size_t max_slice_size,
                                       consumer& cons) {
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(max_slice_size > 0);
  size_t produced = 0;
  while (produced < max_events) {
    // EOF check.
    if (lines_->done())
      return finish(cons,
                    make_error(vast::ec::end_of_input, "input exhausted"));
    auto& line = lines_->get();
    vast::json j;
    if (!vast::parsers::json(line, j))
      return make_error(vast::ec::parse_error, "unable to parse json");
    auto xs = caf::get_if<vast::json::object>(&j);
    if (!xs)
      return make_error(vast::ec::parse_error, "not a json object");
    auto layout = selector(*xs);
    if (!layout)
      return make_error(vast::ec::parse_error, "unable to get a layout");
    auto bptr = builder(*layout);
    if (bptr == nullptr)
      return make_error(vast::ec::parse_error, "unable to get a builder");
    for (auto& field : layout->fields) {
      auto i = xs->find(field.name);
      if (i == xs->end()) {
        bptr->add(vast::make_data_view(caf::none));
        continue;
      }
      auto v = i->second;
      auto f = convert{
        [bptr = bptr](auto x) { return bptr->add(vast::make_data_view(x)); }};
      auto res = caf::visit(f, v, field.type);
      if (!res)
        return finish(cons, make_error(vast::ec::convert_error, field.name, ":",
                                       vast::to_string(v), "line",
                                       lines_->line_number()));
    }
    produced++;
    if (bptr->rows() == max_slice_size)
      if (auto err = finish(cons, bptr))
        return err;
    lines_->next();
  }
  return finish(cons);
}

} // namespace vast::format::json
