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

#include "vast/data.hpp"

#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/directory.hpp"
#include "vast/error.hpp"
#include "vast/json.hpp"
#include "vast/path.hpp"

#include <caf/config_value.hpp>

#include <iterator>
#include <stdexcept>

#include <yaml-cpp/yaml.h>

namespace vast {

bool operator==(const data& lhs, const data& rhs) {
  return lhs.data_ == rhs.data_;
}

bool operator<(const data& lhs, const data& rhs) {
  return lhs.data_ < rhs.data_;
}

bool evaluate(const data& lhs, relational_operator op, const data& rhs) {
  auto eval_string_and_pattern = [](const auto& x, const auto& y) {
    return caf::visit(
      detail::overload{
        [](const auto&, const auto&) -> caf::optional<bool> {
          return caf::none;
        },
        [](const std::string& lhs, const pattern& rhs) -> caf::optional<bool> {
          return rhs.match(lhs);
        },
        [](const pattern& lhs, const std::string& rhs) -> caf::optional<bool> {
          return lhs.match(rhs);
        },
      },
      x, y);
  };
  auto eval_match = [](const auto& x, const auto& y) {
    return caf::visit(detail::overload{
                        [](const auto&, const auto&) { return false; },
                        [](const std::string& lhs, const pattern& rhs) {
                          return rhs.match(lhs);
                        },
                      },
                      x, y);
  };
  auto eval_in = [](const auto& x, const auto& y) {
    return caf::visit(
      detail::overload{
        [](const auto&, const auto&) { return false; },
        [](const std::string& lhs, const std::string& rhs) {
          return rhs.find(lhs) != std::string::npos;
        },
        [](const std::string& lhs, const pattern& rhs) {
          return rhs.search(lhs);
        },
        [](const address& lhs, const subnet& rhs) { return rhs.contains(lhs); },
        [](const subnet& lhs, const subnet& rhs) { return rhs.contains(lhs); },
        [](const auto& lhs, const list& rhs) {
          return std::find(rhs.begin(), rhs.end(), lhs) != rhs.end();
        },
      },
      x, y);
  };
  switch (op) {
    default:
      VAST_ASSERT(!"missing case");
      return false;
    case match:
      return eval_match(lhs, rhs);
    case not_match:
      return !eval_match(lhs, rhs);
    case in:
      return eval_in(lhs, rhs);
    case not_in:
      return !eval_in(lhs, rhs);
    case ni:
      return eval_in(rhs, lhs);
    case not_ni:
      return !eval_in(rhs, lhs);
    case equal:
      if (auto x = eval_string_and_pattern(lhs, rhs))
        return *x;
      return lhs == rhs;
    case not_equal:
      if (auto x = eval_string_and_pattern(lhs, rhs))
        return !*x;
      return lhs != rhs;
    case less:
      return lhs < rhs;
    case less_equal:
      return lhs <= rhs;
    case greater:
      return lhs > rhs;
    case greater_equal:
      return lhs >= rhs;
  }
}

bool is_basic(const data& x) {
  return caf::visit(detail::overload{
                      [](const auto&) { return true; },
                      [](const list&) { return false; },
                      [](const map&) { return false; },
                      [](const record&) { return false; },
                    },
                    x);
}

bool is_complex(const data& x) {
  return !is_basic(x);
}

bool is_recursive(const data& x) {
  return caf::visit(detail::overload{
                      [](const auto&) { return false; },
                      [](const list&) { return true; },
                      [](const map&) { return true; },
                      [](const record&) { return true; },
                    },
                    x);
}

bool is_container(const data& x) {
  // TODO: should a record be considered as a container?
  return is_recursive(x);
}

namespace {

template <class Iterator, class Sentinel>
caf::optional<record>
make_record(const record_type& rt, Iterator& begin, Sentinel end) {
  record result;
  for (auto& field : rt.fields) {
    if (begin == end)
      return caf::none;
    if (auto nested = caf::get_if<record_type>(&field.type)) {
      if (auto r = make_record(*nested, begin, end))
        result.emplace(field.name, std::move(*r));
      else
        return caf::none;
    } else {
      result.emplace(field.name, std::move(*begin));
      ++begin;
    }
  }
  return result;
}

} // namespace

caf::optional<record> make_record(const record_type& rt, std::vector<data>&& xs) {
  auto begin = xs.begin();
  auto end = xs.end();
  return make_record(rt, begin, end);
}

record flatten(const record& r) {
  record result;
  for (auto& [k, v] : r) {
    if (auto nested = caf::get_if<record>(&v))
      for (auto& [nk, nv] : flatten(*nested))
        result.emplace(k + '.' + nk, std::move(nv));
    else
      result.emplace(k, v);
  }
  return result;
}

caf::optional<record> flatten(const record& r, const record_type& rt) {
  record result;
  for (auto& [k, v] : r) {
    if (auto ir = caf::get_if<record>(&v)) {
      // Look for a matching field of type record.
      auto field = rt.find(k);
      if (field == nullptr)
        return caf::none;
      auto irt = caf::get_if<record_type>(&field->type);
      if (!irt)
        return caf::none;
      // Recurse.
      auto nested = flatten(*ir, *irt);
      if (!nested)
        return caf::none;
      // Hoist nested record into parent scope by prefixing field names.
      for (auto& [nk, nv] : *nested)
        result.emplace(k + '.' + nk, std::move(nv));
    } else {
      result.emplace(k, v);
    }
  }
  return result;
}

caf::optional<data> flatten(const data& x, const type& t) {
  auto xs = caf::get_if<record>(&x);
  auto rt = caf::get_if<record_type>(&t);
  if (xs && rt)
    return flatten(*xs, *rt);
  return caf::none;
}

namespace {

caf::optional<record> unflatten(const record& r, const record_type* rt) {
  record result;
  for (auto& [k, v] : r) {
    auto nested = &result;
    auto nested_type = rt;
    // Split field name by '.' to obtain intermediate records.
    auto split = detail::split(k, ".");
    if (split.size() == 1) {
      // We have a leaf value.
      VAST_ASSERT(!caf::holds_alternative<record>(v));
    } else {
      // Create intermediate records as needed.
      for (size_t i = 0; i < split.size() - 1; ++i) {
        auto& field_name = split[i];
        if (rt) {
          // Does the record type contain a corresponding field?
          auto field = nested_type->find(field_name);
          if (field == nullptr)
            return caf::none;
          // Is the field a record type?
          nested_type = caf::get_if<record_type>(&field->type);
          if (nested_type == nullptr)
            return caf::none;
        }
        // Do we already have an intermediate record instantiated?
        auto it = nested->find(field_name);
        if (it == nested->end())
          it = nested->emplace(std::string{split[i]}, record{}).first;
        nested = &caf::get<record>(it->second);
      }
    }
    // Insert leaf value into deepest record.
    if (rt) {
      auto field = nested_type->find(split.back());
      if (!(field && type_check(field->type, v)))
        return caf::none;
    }
    nested->emplace(std::string{split.back()}, v);
  }
  return result;
}

} // namespace

record unflatten(const record& r) {
  auto result = unflatten(r, nullptr);
  VAST_ASSERT(result);
  return std::move(*result);
}

caf::optional<record> unflatten(const record& r, const record_type& rt) {
  return unflatten(r, &rt);
}

caf::optional<data> unflatten(const data& x, const type& t) {
  auto r = caf::get_if<record>(&x);
  auto rt = caf::get_if<record_type>(&t);
  if (r && rt)
    return unflatten(*r, *rt);
  return caf::none;
}

void merge(const record& src, record& dst) {
  for (auto& [k, v] : src) {
    if (auto src_rec = caf::get_if<record>(&v)) {
      auto dst_rec = caf::get_if<record>(&dst[k]);
      if (!dst_rec) {
        // Overwrite key with empty record on type mismatch.
        dst[k] = record{};
        dst_rec = caf::get_if<record>(&dst[k]);
      }
      merge(*src_rec, *dst_rec);
    } else {
      dst[k] = v;
    }
  }
}

namespace {

json jsonize(const data& x) {
  return caf::visit(
    detail::overload{
      [&](const auto& y) { return to_json(y); },
      [&](port p) { return json{p.number()}; }, // ignore port type
      [&](caf::none_t) { return json{}; },
      [&](const std::string& str) { return json{str}; },
    },
    x);
}

} // namespace <anonymous>

bool convert(const list& xs, json& j) {
  json::array a(xs.size());
  for (size_t i = 0; i < xs.size(); ++i)
    a[i] = jsonize(xs[i]);
  j = std::move(a);
  return true;
}

bool convert(const map& t, json& j) {
  json::array values;
  for (auto& p : t) {
    json::array a;
    a.emplace_back(jsonize(p.first));
    a.emplace_back(jsonize(p.second));
    values.emplace_back(std::move(a));
  };
  j = std::move(values);
  return true;
}

bool convert(const record& xs, json& j) {
  json::object o;
  for (auto& [k, v] : xs)
    o[k] = jsonize(v);
  j = std::move(o);
  return true;
}

bool convert(const data& d, json& j) {
  j = jsonize(d);
  return true;
}

bool convert(const data& d, json& j, const type& t) {
  auto xs = caf::get_if<list>(&d);
  auto rt = caf::get_if<record_type>(&t);
  if (xs && rt) {
    if (xs->size() != rt->fields.size())
      return false;
    json::object o;
    for (size_t i = 0; i < xs->size(); ++i) {
      auto& f = rt->fields[i];
      if (!convert((*xs)[i], o[f.name], f.type))
        return false;
    }
    j = std::move(o);
    return true;
  }
  return convert(d, j);
}

caf::error convert(const map& xs, caf::dictionary<caf::config_value>& ys) {
  for (auto& [k, v] : xs) {
    caf::config_value x;
    if (auto err = convert(v, x))
      return err;
    ys[to_string(k)] = std::move(x);
  }
  return caf::none;
}

caf::error convert(const record& xs, caf::dictionary<caf::config_value>& ys) {
  for (auto& [k, v] : xs) {
    caf::config_value x;
    if (auto err = convert(v, x))
      return err;
    ys[k] = std::move(x);
  }
  return caf::none;
}

caf::error convert(const record& xs, caf::config_value& cv) {
  caf::config_value::dictionary result;
  if (auto err = convert(xs, result))
    return err;
  cv = std::move(result);
  return caf::none;
}

caf::error convert(const data& d, caf::config_value& cv) {
  auto f = detail::overload{
    [&](const auto& x) -> caf::error {
      using value_type = std::decay_t<decltype(x)>;
      if constexpr (detail::is_any_v<value_type, bool, integer, count, real,
                                     duration, std::string>)
        cv = x;
      else
        cv = to_string(x);
      return caf::none;
    },
    [&](caf::none_t) -> caf::error {
      // A caf::config_value has no notion of "null" value. Converting it to a
      // default-constructed config_value would be wrong, because that's just
      // an integer with value 0. As such, the conversion is a partial function
      // and we must fail at this point. If you trigger this error when
      // converting a record, you can first flatten the record and then delete
      // all null keys. Then this branch will not be triggered.
      return caf::make_error(ec::type_clash, "cannot convert null to "
                                             "config_value");
    },
    [&](const list& xs) -> caf::error {
      caf::config_value::list result;
      result.reserve(xs.size());
      for (auto x : xs) {
        caf::config_value y;
        if (auto err = convert(x, y))
          return err;
        result.push_back(std::move(y));
      }
      cv = std::move(result);
      return caf::none;
    },
    [&](const map& xs) -> caf::error {
      // We treat maps like records.
      caf::dictionary<caf::config_value> result;
      if (auto err = convert(xs, result))
        return err;
      cv = std::move(result);
      return caf::none;
    },
    [&](const record& xs) -> caf::error {
      caf::dictionary<caf::config_value> result;
      if (auto err = convert(xs, result))
        return err;
      cv = std::move(result);
      return caf::none;
    },
  };
  return caf::visit(f, d);
}

namespace {

data parse(const YAML::Node& node) {
  switch (node.Type()) {
    case YAML::NodeType::Undefined:
    case YAML::NodeType::Null:
      return data{};
    case YAML::NodeType::Scalar: {
      auto str = node.as<std::string>();
      data result;
      // Attempt some type inference.
      if (parsers::boolean(str, result))
        return result;
      // Attempt maximum type inference.
      if (parsers::data(str, result))
        return result;
      // Take the input as-is if nothing worked.
      return str;
    }
    case YAML::NodeType::Sequence: {
      list xs;
      xs.reserve(node.size());
      for (size_t i = 0; i < node.size(); ++i)
        xs.push_back(parse(node[i]));
      return xs;
    }
    case YAML::NodeType::Map: {
      record xs;
      xs.reserve(node.size());
      for (auto& pair : node)
        xs.emplace(pair.first.as<std::string>(), parse(pair.second));
      return xs;
    }
  }
  VAST_ASSERT(!"unhandled YAML node type in switch statement");
  throw std::logic_error{"unhandled YAML node type in switch statement"};
}

} // namespace

caf::expected<data> from_yaml(std::string_view str) {
  try {
    // Maybe one glory day in the future it will be possible to perform a
    // single pass over the input without incurring a copy.
    auto node = YAML::Load(std::string{str});
    return parse(node);
  } catch (const YAML::Exception& e) {
    return caf::make_error(ec::parse_error,
                           "failed to parse YAML at line/col/pos", e.mark.line,
                           e.mark.column, e.mark.pos);
  } catch (const std::logic_error& e) {
    return caf::make_error(ec::logic_error, e.what());
  }
}

caf::expected<data> load_yaml(const path& file) {
  auto contents = load_contents(file);
  if (!contents)
    return contents.error();
  if (auto yaml = from_yaml(*contents))
    return yaml;
  else
    return caf::make_error(ec::parse_error, "failed to load YAML file", file,
                           yaml.error().context());
}

caf::expected<std::vector<std::pair<path, data>>>
load_yaml_dir(const path& dir) {
  std::vector<std::pair<path, data>> result;
  for (auto& file : directory{dir}) {
    switch (file.kind()) {
      default:
        continue;
      case path::directory: {
        auto nested = load_yaml_dir(file);
        if (!nested)
          return nested;
        auto begin = std::make_move_iterator(nested->begin());
        auto end = std::make_move_iterator(nested->end());
        result.insert(result.end(), begin, end);
        break;
      }
      case path::regular_file:
      case path::symlink: {
        if (file.extension() == ".yml" || file.extension() == ".yaml") {
          if (auto yaml = load_yaml(file))
            result.emplace_back(file, std::move(*yaml));
          else
            return yaml.error();
        }
        break;
      }
    }
  }
  return result;
}

namespace {

void print(YAML::Emitter& out, const data& x) {
  auto f = detail::overload{
    [&out](caf::none_t) { out << YAML::Null; },
    [&out](bool x) { out << (x ? "true" : "false"); },
    [&out](integer x) { out << x; },
    [&out](count x) { out << x; },
    [&out](real x) { out << to_string(x); },
    [&out](duration x) { out << to_string(x); },
    [&out](time x) { out << to_string(x); },
    [&out](const std::string& x) { out << x; },
    [&out](const pattern& x) { out << to_string(x); },
    [&out](const address& x) { out << to_string(x); },
    [&out](const subnet& x) { out << to_string(x); },
    [&out](const port& x) { out << to_string(x); },
    [&out](const enumeration& x) { out << to_string(x); },
    [&out](const list& xs) {
      out << YAML::BeginSeq;
      for (auto& x : xs)
        print(out, x);
      out << YAML::EndSeq;
    },
    // We treat maps like records.
    [&out](const map& xs) {
      out << YAML::BeginMap;
      for (auto& [k, v] : xs) {
        out << YAML::Key;
        print(out, k);
        out << YAML::Value;
        print(out, v);
      }
      out << YAML::EndMap;
    },
    [&out](const record& xs) {
      out << YAML::BeginMap;
      for (auto& [k, v] : xs) {
        out << YAML::Key << k << YAML::Value;
        print(out, v);
      }
      out << YAML::EndMap;
    },
  };
  caf::visit(f, x);
}

} // namespace

caf::expected<std::string> to_yaml(const data& x) {
  YAML::Emitter out;
  out.SetOutputCharset(YAML::EscapeNonAscii); // restrict to ASCII output
  out.SetIndent(2);
  print(out, x);
  if (out.good())
    return std::string{out.c_str(), out.size()};
  return caf::make_error(ec::parse_error, out.GetLastError());
}

} // namespace vast
