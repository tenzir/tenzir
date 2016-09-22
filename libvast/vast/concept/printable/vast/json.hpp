#ifndef VAST_CONCEPT_PRINTABLE_VAST_JSON_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_JSON_HPP

#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/detail/string.hpp"
#include "vast/json.hpp"

namespace vast {

//struct json_type_printer : printer<json_type_printer> {
//  using attribute = json::type;
//
//  template <typename Iterator>
//  bool print(Iterator& out, json::type const& t) {
//    using namespace printers;
//    switch (t) {
//      default:
//        return str.print(out, "<invalid>");
//      case json::type::null:
//        return str.print(out, "null");
//      case json::type::boolean:
//        return str.print(out, "bool");
//      case json::type::number:
//        return str.print(out, "number");
//      case json::type::string:
//        return str.print(out, "string");
//      case json::type::array:
//        return str.print(out, "array");
//      case json::type::object:
//        return str.print(out, "object");
//    }
//  }
//};

namespace policy {

struct tree {};
struct oneline {};

} // namespace policy

template <typename TreePolicy, int Indent = 2, int Padding = 0>
struct json_printer : printer<json_printer<TreePolicy, Indent, Padding>> {
  using attribute = json;

  static constexpr auto tree = std::is_same<TreePolicy, policy::tree>{};

  static_assert(Padding >= 0, "padding must not be negative");

  template <typename Iterator>
  struct print_visitor {
    print_visitor(Iterator& out) : out_{out} {}

    bool operator()(none const&) {
      return printers::str.print(out_, "null");
    }

    bool operator()(bool b) {
      return printers::str.print(out_, b ? "true" : "false");
    }

    bool operator()(json::number n) {
      auto str = std::to_string(n);
      json::number i;
      if (std::modf(n, &i) == 0.0)
        str.erase(str.find('.'), std::string::npos);
      else
        str.erase(str.find_last_not_of('0') + 1, std::string::npos);
      return printers::str.print(out_, str);
    }

    bool operator()(std::string const& str) {
      return printers::str.print(out_, detail::json_escape(str));
    }

    bool operator()(json::array const& a) {
      using namespace printers;
      if (depth_ == 0 && !pad())
        return false;
      if (!any.print(out_, '['))
        return false;
      if (!a.empty() && tree) {
        ++depth_;
        any.print(out_, '\n');
      }
      auto begin = a.begin();
      auto end = a.end();
      while (begin != end) {
        if (!indent())
          return false;
        if (!visit(*this, *begin))
          return false;
        ;
        ++begin;
        if (begin != end)
          if (!str.print(out_, tree ? ",\n" : ", "))
            return false;
      }
      if (!a.empty() && tree) {
        --depth_;
        any.print(out_, '\n');
        if (!indent())
          return false;
      }
      return any.print(out_, ']');
    }

    bool operator()(json::object const& o) {
      using namespace printers;
      if (depth_ == 0 && !pad())
        return false;
      if (!any.print(out_, '{'))
        return false;
      if (!o.empty() && tree) {
        ++depth_;
        if (!any.print(out_, '\n'))
          return false;
      }
      auto begin = o.begin();
      auto end = o.end();
      while (begin != end) {
        if (!indent())
          return false;
        if (!(*this)(begin->first))
          return false;
        if (!str.print(out_, ": "))
          return false;
        if (!visit(*this, begin->second))
          return false;
        ++begin;
        if (begin != end)
          if (!str.print(out_, tree ? ",\n" : ", "))
            return false;
      }
      if (!o.empty() && tree) {
        --depth_;
        if (!any.print(out_, '\n'))
          return false;
        if (!indent())
          return false;
      }
      return any.print(out_, '}');
    }

    bool pad() {
      if (Padding > 0)
        for (auto i = 0; i < Padding; ++i)
          if (!printers::any.print(out_, ' '))
            return false;
      return true;
    }

    bool indent() {
      if (!pad())
        return false;
      if (!tree)
        return true;
      for (auto i = 0; i < depth_ * Indent; ++i)
        if (!printers::any.print(out_, ' '))
          return false;
      return true;
    }

    Iterator& out_;
    int depth_ = 0;
  };

  template <typename Iterator, typename T>
  auto print(Iterator& out, T const& x) const
    -> std::enable_if_t<!std::is_same<json::jsonize<T>,
                                      std::false_type>::value> {
    return print_visitor<Iterator>{out}(x);
  }

  template <typename Iterator>
  bool print(Iterator& out, json const& j) const {
    return visit(print_visitor<Iterator>{out}, j);
  }
};

//template <>
//struct printer_registry<json::type> {
//  using type = json_type_printer;
//};

template <>
struct printer_registry<json::array> {
  using type = json_printer<policy::tree, 2>;
};

template <>
struct printer_registry<json::object> {
  using type = json_printer<policy::tree, 2>;
};

template <>
struct printer_registry<json> {
  using type = json_printer<policy::tree, 2>;
};

namespace printers {

template <typename Policy>
json_printer<Policy> json;

} // namespace printers
} // namespace vast

#endif
