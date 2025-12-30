//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          , Accessibility
//   |___/_/ |_/___/ /_/       Sdn. Bhd. 2025
//
//   This file is part of Tenzir.
//
//   Tenzir is made available under the Apache License, Version 2.0.

#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/plugin.hpp>

#include <expat.h>
#include <functional>
#include <map>
#include <memory>
#include <stack>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace tenzir::plugins::xml {

namespace {

// Forward declarations
struct xml_element;
using xml_node = std::variant<std::string, std::unique_ptr<xml_element>>;

/// An XML element with tag name, attributes, and children.
struct xml_element {
  std::string name;
  std::vector<std::pair<std::string, std::string>> attributes;
  std::vector<xml_node> children;
};

/// Options for XML to record conversion.
struct xml_options {
  std::string attr_prefix = "@";
  std::string text_key = "#text";
  int64_t max_depth = 10;
  bool strip_namespaces = true;
};

/// Strip the namespace prefix from a name (e.g., "ns:item" -> "item").
auto strip_namespace(std::string_view name) -> std::string {
  auto pos = name.find(':');
  if (pos != std::string_view::npos) {
    return std::string{name.substr(pos + 1)};
  }
  return std::string{name};
}

/// SAX handler state for building a DOM from XML.
struct sax_state {
  std::unique_ptr<xml_element> root;
  std::stack<xml_element*> element_stack;
  bool strip_namespaces = true;

  auto process_name(std::string_view name) -> std::string {
    if (strip_namespaces) {
      return strip_namespace(name);
    }
    return std::string{name};
  }
};

/// RAII wrapper for libexpat XML_Parser.
struct xml_parser_deleter {
  void operator()(XML_Parser p) const noexcept {
    if (p) {
      XML_ParserFree(p);
    }
  }
};
using xml_parser_ptr
  = std::unique_ptr<std::remove_pointer_t<XML_Parser>, xml_parser_deleter>;

/// Expat SAX callbacks.
void XMLCALL start_element(void* user_data, const XML_Char* name,
                           const XML_Char** attrs) {
  auto* state = static_cast<sax_state*>(user_data);
  auto elem = std::make_unique<xml_element>();
  elem->name = state->process_name(name);
  // Parse attributes
  for (int i = 0; attrs[i]; i += 2) {
    auto attr_name = state->process_name(attrs[i]);
    // Skip xmlns declarations when stripping namespaces
    if (state->strip_namespaces) {
      if (attr_name == "xmlns"
          or std::string_view{attrs[i]}.starts_with("xmlns:")) {
        continue;
      }
    }
    elem->attributes.emplace_back(std::move(attr_name),
                                  std::string{attrs[i + 1]});
  }
  auto* elem_ptr = elem.get();
  if (state->element_stack.empty()) {
    state->root = std::move(elem);
  } else {
    state->element_stack.top()->children.push_back(std::move(elem));
  }
  state->element_stack.push(elem_ptr);
}

void XMLCALL end_element(void* user_data, const XML_Char*) {
  auto* state = static_cast<sax_state*>(user_data);
  if (not state->element_stack.empty()) {
    state->element_stack.pop();
  }
}

void XMLCALL character_data(void* user_data, const XML_Char* s, int len) {
  auto* state = static_cast<sax_state*>(user_data);
  if (state->element_stack.empty()) {
    return;
  }
  auto text = std::string{s, static_cast<size_t>(len)};
  // Trim whitespace-only text nodes
  if (std::ranges::all_of(text, [](char c) {
        return c == ' ' or c == '\t' or c == '\n' or c == '\r';
      })) {
    return;
  }
  auto& children = state->element_stack.top()->children;
  // Merge consecutive text nodes
  if (not children.empty()
      and std::holds_alternative<std::string>(children.back())) {
    std::get<std::string>(children.back()) += text;
  } else {
    children.push_back(std::move(text));
  }
}

/// Parse XML string into a DOM tree.
auto parse_xml_dom(std::string_view xml, bool strip_namespaces)
  -> std::unique_ptr<xml_element> {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  auto parser = xml_parser_ptr{XML_ParserCreate(nullptr)};
  if (not parser) {
    return nullptr;
  }
  auto state = sax_state{};
  state.strip_namespaces = strip_namespaces;
  XML_SetUserData(parser.get(), &state);
  XML_SetElementHandler(parser.get(), start_element, end_element);
  XML_SetCharacterDataHandler(parser.get(), character_data);
  auto status = XML_Parse(parser.get(), xml.data(),
                          static_cast<int>(xml.size()), XML_TRUE);
  if (status == XML_STATUS_ERROR) {
    return nullptr;
  }
  return std::move(state.root);
}

/// Evaluate a simple XPath expression and return matching elements.
/// Supports: "/*" (root children), "//name" (all descendants named 'name'),
/// "/root/child" (specific path).
auto evaluate_xpath(const xml_element* root, std::string_view xpath)
  -> std::vector<const xml_element*> {
  std::vector<const xml_element*> results;
  if (not root or xpath.empty()) {
    return results;
  }
  // Handle "/*" - return root children (which is the root element itself)
  if (xpath == "/*") {
    results.push_back(root);
    return results;
  }
  // Handle "//" - descendant axis
  if (xpath.starts_with("//")) {
    auto name = xpath.substr(2);
    std::function<void(const xml_element*)> collect_by_name
      = [&](const xml_element* elem) {
          if (elem->name == name) {
            results.push_back(elem);
          }
          for (const auto& child : elem->children) {
            if (auto* child_elem
                = std::get_if<std::unique_ptr<xml_element>>(&child)) {
              collect_by_name(child_elem->get());
            }
          }
        };
    collect_by_name(root);
    return results;
  }
  // Handle absolute path like "/root/child/..."
  if (xpath.starts_with("/")) {
    auto path = xpath.substr(1);
    std::vector<std::string> parts;
    size_t pos = 0;
    while ((pos = path.find('/')) != std::string_view::npos) {
      parts.emplace_back(path.substr(0, pos));
      path = path.substr(pos + 1);
    }
    if (not path.empty()) {
      parts.emplace_back(path);
    }
    if (parts.empty()) {
      return results;
    }
    // Start from root
    const xml_element* current = root;
    // First part must match root name
    if (parts[0] != root->name) {
      return results;
    }
    // Navigate through path
    for (size_t i = 1; i < parts.size(); ++i) {
      const xml_element* next = nullptr;
      for (const auto& child : current->children) {
        if (auto* child_elem
            = std::get_if<std::unique_ptr<xml_element>>(&child)) {
          if ((*child_elem)->name == parts[i]) {
            next = child_elem->get();
            break;
          }
        }
      }
      if (not next) {
        return results;
      }
      current = next;
    }
    results.push_back(current);
    return results;
  }
  return results;
}

// Forward declaration for mutual recursion.
template <typename RecordBuilder>
void element_to_record(RecordBuilder record, const xml_element& elem,
                       const xml_options& opts, int depth);

/// Convert an XML node (element or text) to data.
template <typename ObjectBuilder>
void node_to_data(ObjectBuilder field, const xml_node& node,
                  const xml_options& opts, int depth) {
  if (auto* text = try_as<std::string>(node)) {
    field.data(*text);
    return;
  }
  auto& elem = as<std::unique_ptr<xml_element>>(node);
  if (depth >= opts.max_depth) {
    field.null();
    return;
  }
  // Check if element has only text content
  if (elem->attributes.empty() and elem->children.size() == 1
      and is<std::string>(elem->children[0])) {
    field.data(as<std::string>(elem->children[0]));
  } else if (elem->children.empty() and elem->attributes.empty()) {
    field.null();
  } else {
    auto record = field.record();
    element_to_record(record, *elem, opts, depth);
  }
}

template <typename RecordBuilder>
void element_to_record(RecordBuilder record, const xml_element& elem,
                       const xml_options& opts, int depth) {
  // Add attributes with prefix
  for (const auto& [name, value] : elem.attributes) {
    auto field_name = opts.attr_prefix + name;
    record.field(field_name).data(value);
  }
  // Group children by name to detect lists
  std::map<std::string, std::vector<const xml_node*>> children_by_name;
  std::vector<const std::string*> text_children;
  for (const auto& child : elem.children) {
    if (auto* text = try_as<std::string>(child)) {
      text_children.push_back(text);
    } else if (auto* child_elem = try_as<std::unique_ptr<xml_element>>(child)) {
      children_by_name[(*child_elem)->name].push_back(&child);
    }
  }
  // Handle text content
  if (not text_children.empty()) {
    std::string combined;
    for (const auto* text : text_children) {
      if (not combined.empty()) {
        combined += " ";
      }
      combined += *text;
    }
    record.field(opts.text_key).data(combined);
  }
  // Add child elements
  for (auto& [child_name, nodes] : children_by_name) {
    // Multiple elements with same name become a list
    if (nodes.size() > 1) {
      auto list = record.field(child_name).list();
      for (const auto* node : nodes) {
        // For list items, handle based on node type
        if (auto* text = try_as<std::string>(*node)) {
          list.data(*text);
        } else {
          auto& elem_ptr = as<std::unique_ptr<xml_element>>(*node);
          if (depth + 1 >= opts.max_depth) {
            list.null();
          } else if (elem_ptr->attributes.empty()
                     and elem_ptr->children.size() == 1
                     and is<std::string>(elem_ptr->children[0])) {
            // Element has only text content
            list.data(as<std::string>(elem_ptr->children[0]));
          } else if (elem_ptr->children.empty()
                     and elem_ptr->attributes.empty()) {
            list.null();
          } else {
            auto nested_record = list.record();
            element_to_record(nested_record, *elem_ptr, opts, depth + 1);
          }
        }
      }
    } else {
      auto field = record.field(child_name);
      node_to_data(field, *nodes[0], opts, depth + 1);
    }
  }
}

class parse_xml_plugin final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parse_xml";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto xpath = std::optional<located<std::string>>{};
    auto attr_prefix = std::optional<located<std::string>>{};
    auto text_key = std::optional<located<std::string>>{};
    auto max_depth = std::optional<located<int64_t>>{};
    auto namespaces = std::optional<located<std::string>>{};
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "string");
    parser.named("xpath", xpath);
    parser.named("attr_prefix", attr_prefix);
    parser.named("text_key", text_key);
    parser.named("max_depth", max_depth);
    parser.named("namespaces", namespaces);
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(parser, true, false);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    // Build options
    auto opts = xml_options{};
    if (xpath) {
      opts.attr_prefix = xpath->inner.empty() ? "" : "@";
    }
    auto xpath_str = xpath ? xpath->inner : std::string{"/*"};
    if (attr_prefix) {
      opts.attr_prefix = attr_prefix->inner;
    }
    if (text_key) {
      opts.text_key = text_key->inner;
    }
    if (max_depth) {
      if (max_depth->inner < 0) {
        diagnostic::error("`max_depth` must be non-negative")
          .primary(*max_depth)
          .emit(ctx);
        return failure::promise();
      }
      opts.max_depth = max_depth->inner;
    }
    if (namespaces) {
      if (namespaces->inner == "strip") {
        opts.strip_namespaces = true;
      } else if (namespaces->inner == "keep") {
        opts.strip_namespaces = false;
      } else {
        diagnostic::error("`namespaces` must be \"strip\" or \"keep\"")
          .primary(*namespaces)
          .emit(ctx);
        return failure::promise();
      }
    }
    return function_use::make(
      [call = inv.call.get_location(), msb_opts = std::move(msb_opts),
       opts = std::move(opts), xpath_str = std::move(xpath_str),
       expr = std::move(expr)](evaluator eval, session ctx) {
        return map_series(eval(expr), [&](series arg) {
          return match(
            *arg.array,
            [&](const arrow::NullArray&) -> multi_series {
              return arg;
            },
            [&](const arrow::StringArray& arr) -> multi_series {
              auto builder = multi_series_builder{msb_opts, ctx};
              for (auto i = int64_t{0}; i < arr.length(); ++i) {
                if (arr.IsNull(i)) {
                  builder.null();
                  continue;
                }
                auto xml_str = arr.Value(i);
                if (xml_str.empty()) {
                  builder.null();
                  continue;
                }
                auto root = parse_xml_dom(xml_str, opts.strip_namespaces);
                if (not root) {
                  diagnostic::warning("failed to parse XML")
                    .primary(call)
                    .emit(ctx);
                  builder.null();
                  continue;
                }
                auto matches = evaluate_xpath(root.get(), xpath_str);
                if (matches.empty()) {
                  builder.null();
                  continue;
                }
                // Return list if multiple matches
                if (matches.size() > 1 or xpath_str == "/*") {
                  auto list = builder.list();
                  for (const auto* elem : matches) {
                    auto record = list.record();
                    element_to_record(record, *elem, opts, 0);
                  }
                } else {
                  auto record = builder.record();
                  element_to_record(record, *matches[0], opts, 0);
                }
              }
              return multi_series{builder.finalize()};
            },
            [&](const auto&) -> multi_series {
              diagnostic::warning("`parse_xml` expected `string`, got `{}`",
                                  arg.type.kind())
                .primary(call)
                .emit(ctx);
              return series::null(null_type{}, arg.length());
            });
        });
      });
  }
};

/// Transform EventData for Windows Event Log:
/// <EventData><Data Name="x">v</Data></EventData> -> {EventData: {x: "v"}}
template <typename RecordBuilder>
void transform_event_data(RecordBuilder record, const xml_element& event_data) {
  // Look for Data elements with Name attribute
  for (const auto& child : event_data.children) {
    if (auto* elem = try_as<std::unique_ptr<xml_element>>(child)) {
      if ((*elem)->name == "Data") {
        // Find Name attribute
        std::string field_name;
        for (const auto& [attr, val] : (*elem)->attributes) {
          if (attr == "Name") {
            field_name = val;
            break;
          }
        }
        if (field_name.empty()) {
          // Unnamed Data element - use index
          continue;
        }
        // Get text content
        if (not(*elem)->children.empty()) {
          if (auto* text = try_as<std::string>((*elem)->children[0])) {
            record.field(field_name).data(*text);
          }
        } else {
          record.field(field_name).null();
        }
      }
    }
  }
}

/// Convert Windows Event to record with special EventData handling.
template <typename RecordBuilder>
void winlog_to_record(RecordBuilder record, const xml_element& event,
                      const xml_options& opts) {
  for (const auto& child : event.children) {
    if (auto* elem_ptr = try_as<std::unique_ptr<xml_element>>(child)) {
      auto& elem = *elem_ptr;
      if (elem->name == "EventData" or elem->name == "UserData") {
        // Special handling for EventData/UserData
        auto event_data_record = record.field(elem->name).record();
        transform_event_data(event_data_record, *elem);
      } else {
        // Regular element handling
        auto field = record.field(elem->name);
        if (elem->attributes.empty() and elem->children.size() == 1
            and is<std::string>(elem->children[0])) {
          field.data(as<std::string>(elem->children[0]));
        } else if (elem->children.empty() and elem->attributes.empty()) {
          field.null();
        } else {
          auto nested = field.record();
          element_to_record(nested, *elem, opts, 0);
        }
      }
    }
  }
}

class parse_winlog_plugin final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parse_winlog";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "string");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(parser, true, false);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    // Fixed options for Windows Event Log
    auto opts = xml_options{};
    opts.attr_prefix = "";
    opts.strip_namespaces = true;
    return function_use::make(
      [call = inv.call.get_location(), msb_opts = std::move(msb_opts),
       opts = std::move(opts),
       expr = std::move(expr)](evaluator eval, session ctx) {
        return map_series(eval(expr), [&](series arg) {
          return match(
            *arg.array,
            [&](const arrow::NullArray&) -> multi_series {
              return arg;
            },
            [&](const arrow::StringArray& arr) -> multi_series {
              auto builder = multi_series_builder{msb_opts, ctx};
              for (auto i = int64_t{0}; i < arr.length(); ++i) {
                if (arr.IsNull(i)) {
                  builder.null();
                  continue;
                }
                auto xml_str = arr.Value(i);
                if (xml_str.empty()) {
                  builder.null();
                  continue;
                }
                auto root = parse_xml_dom(xml_str, opts.strip_namespaces);
                if (not root) {
                  diagnostic::warning("failed to parse Windows Event XML")
                    .primary(call)
                    .emit(ctx);
                  builder.null();
                  continue;
                }
                // Find Event element (might be root or nested)
                const xml_element* event = nullptr;
                if (root->name == "Event") {
                  event = root.get();
                } else {
                  // Search for Event in children
                  for (const auto& child : root->children) {
                    if (auto* elem
                        = try_as<std::unique_ptr<xml_element>>(child)) {
                      if ((*elem)->name == "Event") {
                        event = elem->get();
                        break;
                      }
                    }
                  }
                }
                if (not event) {
                  diagnostic::warning("no Event element found in Windows XML")
                    .primary(call)
                    .emit(ctx);
                  builder.null();
                  continue;
                }
                auto record = builder.record();
                winlog_to_record(record, *event, opts);
              }
              return multi_series{builder.finalize()};
            },
            [&](const auto&) -> multi_series {
              diagnostic::warning("`parse_winlog` expected `string`, got `{}`",
                                  arg.type.kind())
                .primary(call)
                .emit(ctx);
              return series::null(null_type{}, arg.length());
            });
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::xml

TENZIR_REGISTER_PLUGIN(tenzir::plugins::xml::parse_xml_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xml::parse_winlog_plugin)
