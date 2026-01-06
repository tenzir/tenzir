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
#include <tenzir/collect.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/stable_map.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <expat.h>
#include <memory>
#include <ranges>
#include <stack>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace tenzir::plugins::xml {

namespace {

/// Maximum nesting depth during SAX parsing to prevent stack overflow.
constexpr size_t max_sax_depth = 256;

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
  std::string key_attr; // If set, use this attribute's value as field name
  int64_t max_depth = 10;
  bool strip_namespaces = true;
};

/// Strip the namespace prefix from a name (e.g., "ns:item" -> "item").
auto strip_namespace(std::string_view name) -> std::string_view {
  auto pos = name.find(':');
  if (pos != std::string_view::npos) {
    return name.substr(pos + 1);
  }
  return name;
}

/// SAX handler state for building a DOM from XML.
struct sax_state {
  std::unique_ptr<xml_element> root;
  std::stack<xml_element*> element_stack;
  bool strip_namespaces = true;
  size_t current_depth = 0;
  size_t max_depth = 256;
  bool depth_exceeded = false;

  auto process_name(std::string_view name) -> std::string {
    if (strip_namespaces) {
      return std::string{strip_namespace(name)};
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

/// Result of XML parsing: either a parsed element or an error message.
using xml_parse_result
  = std::expected<std::unique_ptr<xml_element>, std::string>;

/// Expat SAX callbacks.
void XMLCALL start_element(void* user_data, const XML_Char* name,
                           const XML_Char** attrs) {
  auto* state = static_cast<sax_state*>(user_data);
  // Check depth limit to prevent stack overflow on deeply nested XML
  if (state->current_depth >= state->max_depth) {
    state->depth_exceeded = true;
    return;
  }
  ++state->current_depth;
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
  if (state->current_depth > 0) {
    --state->current_depth;
  }
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
auto parse_xml_dom(std::string_view xml, bool strip_namespaces,
                   size_t max_depth = 256) -> xml_parse_result {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  auto parser = xml_parser_ptr{XML_ParserCreate(nullptr)};
  if (not parser) {
    return std::unexpected{"failed to create XML parser"};
  }
  auto state = sax_state{};
  state.strip_namespaces = strip_namespaces;
  state.max_depth = max_depth;
  XML_SetUserData(parser.get(), &state);
  XML_SetElementHandler(parser.get(), start_element, end_element);
  XML_SetCharacterDataHandler(parser.get(), character_data);
  auto status = XML_Parse(parser.get(), xml.data(),
                          detail::narrow<int>(xml.size()), XML_TRUE);
  if (status == XML_STATUS_ERROR) {
    auto line = XML_GetCurrentLineNumber(parser.get());
    auto column = XML_GetCurrentColumnNumber(parser.get());
    auto error_code = XML_GetErrorCode(parser.get());
    auto error_str = XML_ErrorString(error_code);
    return std::unexpected{
      fmt::format("line {}:{}: {}", line, column, error_str)};
  }
  if (state.depth_exceeded) {
    return std::unexpected{
      fmt::format("maximum nesting depth of {} exceeded", max_depth)};
  }
  return std::move(state.root);
}

/// Parsed XPath predicate.
struct xpath_predicate {
  enum class type { none, position, last, attr_exists, attr_equals };
  type kind = type::none;
  int64_t position = 0;   // For position predicates (1-indexed)
  std::string attr_name;  // For attribute predicates
  std::string attr_value; // For [@attr='value']
};

/// Parse an XPath predicate from "[...]" syntax.
auto parse_predicate(std::string_view pred_str) -> xpath_predicate {
  xpath_predicate result;
  if (pred_str.empty() or pred_str.front() != '[' or pred_str.back() != ']') {
    return result;
  }
  auto inner = pred_str.substr(1, pred_str.size() - 2);
  // Handle [last()]
  if (inner == "last()") {
    result.kind = xpath_predicate::type::last;
    return result;
  }
  // Handle [@attr] or [@attr='value']
  if (inner.starts_with("@")) {
    auto attr_part = inner.substr(1);
    auto eq_pos = attr_part.find('=');
    if (eq_pos == std::string_view::npos) {
      // [@attr] - attribute exists
      result.kind = xpath_predicate::type::attr_exists;
      result.attr_name = std::string{attr_part};
    } else {
      // [@attr='value'] or [@attr="value"]
      result.kind = xpath_predicate::type::attr_equals;
      result.attr_name = std::string{attr_part.substr(0, eq_pos)};
      auto val = attr_part.substr(eq_pos + 1);
      // Strip quotes
      if (val.size() >= 2
          and ((val.front() == '\'' and val.back() == '\'')
               or (val.front() == '"' and val.back() == '"'))) {
        val = val.substr(1, val.size() - 2);
      }
      result.attr_value = std::string{val};
    }
    return result;
  }
  // Handle [n] - position predicate (1-indexed)
  auto pos = int64_t{0};
  auto [ptr, ec]
    = std::from_chars(inner.data(), inner.data() + inner.size(), pos);
  if (ec == std::errc{} and ptr == inner.data() + inner.size() and pos > 0) {
    result.kind = xpath_predicate::type::position;
    result.position = pos;
  }
  return result;
}

/// Split element name from predicate: "name[pred]" -> {"name", "[pred]"}
auto split_name_predicate(std::string_view step)
  -> std::pair<std::string_view, std::string_view> {
  auto bracket = step.find('[');
  if (bracket == std::string_view::npos) {
    return {step, {}};
  }
  return {step.substr(0, bracket), step.substr(bracket)};
}

/// Check if element has the specified attribute.
auto has_attribute(const xml_element& elem, std::string_view name) -> bool {
  for (const auto& [attr_name, _] : elem.attributes) {
    if (attr_name == name) {
      return true;
    }
  }
  return false;
}

/// Get attribute value, or nullptr if not present.
auto get_attribute(const xml_element& elem, std::string_view name)
  -> const std::string* {
  for (const auto& [attr_name, attr_val] : elem.attributes) {
    if (attr_name == name) {
      return &attr_val;
    }
  }
  return nullptr;
}

/// Apply predicate filter to a list of elements.
auto apply_predicate(std::vector<const xml_element*> elems,
                     const xpath_predicate& pred)
  -> std::vector<const xml_element*> {
  if (pred.kind == xpath_predicate::type::none or elems.empty()) {
    return elems;
  }
  std::vector<const xml_element*> result;
  switch (pred.kind) {
    case xpath_predicate::type::position:
      if (pred.position > 0
          and static_cast<size_t>(pred.position) <= elems.size()) {
        result.push_back(elems[pred.position - 1]); // 1-indexed
      }
      break;
    case xpath_predicate::type::last:
      result.push_back(elems.back());
      break;
    case xpath_predicate::type::attr_exists:
      for (const auto* elem : elems) {
        if (has_attribute(*elem, pred.attr_name)) {
          result.push_back(elem);
        }
      }
      break;
    case xpath_predicate::type::attr_equals:
      for (const auto* elem : elems) {
        if (const auto* val = get_attribute(*elem, pred.attr_name)) {
          if (*val == pred.attr_value) {
            result.push_back(elem);
          }
        }
      }
      break;
    case xpath_predicate::type::none:
      break;
  }
  return result;
}

/// Collect all descendant elements with a given name.
auto collect_descendants_by_name(const xml_element* elem, std::string_view name)
  -> generator<const xml_element*> {
  if (elem->name == name) {
    co_yield elem;
  }
  for (const auto& child : elem->children) {
    if (auto* child_elem = std::get_if<std::unique_ptr<xml_element>>(&child)) {
      for (auto* descendant :
           collect_descendants_by_name(child_elem->get(), name)) {
        co_yield descendant;
      }
    }
  }
}

/// Evaluate a simple XPath expression and return matching elements.
/// Supports: "/*" (root), "//name" (descendants), "//name[pred]" (with
/// predicate),
/// "/root/child" (path). Predicates: [n], [last()], [@attr], [@attr='value'].
auto evaluate_xpath(const xml_element* root, std::string_view xpath)
  -> std::vector<const xml_element*> {
  std::vector<const xml_element*> results;
  if (not root or xpath.empty()) {
    return results;
  }
  // Handle "/*" - return root element
  if (xpath == "/*") {
    results.push_back(root);
    return results;
  }
  // Handle "//" - descendant axis with optional predicate
  if (xpath.starts_with("//")) {
    auto expr = xpath.substr(2);
    auto [name, pred_str] = split_name_predicate(expr);
    results = collect(collect_descendants_by_name(root, name));
    if (not pred_str.empty()) {
      auto pred = parse_predicate(pred_str);
      results = apply_predicate(std::move(results), pred);
    }
    return results;
  }
  // Handle absolute path like "/root/child/..." with optional predicates
  if (xpath.starts_with("/")) {
    auto parts = detail::split(xpath.substr(1), "/");
    if (parts.empty()) {
      return results;
    }
    // Start from root - check first part (may have predicate)
    auto [root_name, root_pred_str] = split_name_predicate(parts[0]);
    if (root_name != root->name) {
      return results;
    }
    const xml_element* current = root;
    // Navigate through path
    for (size_t i = 1; i < parts.size(); ++i) {
      auto [step_name, step_pred_str] = split_name_predicate(parts[i]);
      // Collect all matching children
      std::vector<const xml_element*> matches;
      for (const auto& child : current->children) {
        if (auto* child_elem
            = std::get_if<std::unique_ptr<xml_element>>(&child)) {
          if ((*child_elem)->name == step_name) {
            matches.push_back(child_elem->get());
          }
        }
      }
      // Apply predicate if present
      if (not step_pred_str.empty()) {
        auto pred = parse_predicate(step_pred_str);
        matches = apply_predicate(std::move(matches), pred);
      }
      if (matches.empty()) {
        return results;
      }
      // For path navigation, take the first match
      current = matches[0];
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

/// Get the value of key_attr from an element, or nullptr if not present.
auto get_key_attr_value(const xml_element& elem, const std::string& key_attr)
  -> const std::string* {
  if (key_attr.empty()) {
    return nullptr;
  }
  for (const auto& [attr_name, attr_val] : elem.attributes) {
    if (attr_name == key_attr) {
      return &attr_val;
    }
  }
  return nullptr;
}

/// Convert element content to data, used when key_attr extracts field name.
/// Outputs the element's value (text or nested record) without the key_attr.
template <typename ObjectBuilder>
void element_value_to_data(ObjectBuilder field, const xml_element& elem,
                           const xml_options& opts, int depth) {
  if (depth >= opts.max_depth) {
    field.null();
    return;
  }
  // Count non-key_attr attributes
  size_t other_attrs = 0;
  for (const auto& [attr_name, _] : elem.attributes) {
    if (attr_name != opts.key_attr) {
      ++other_attrs;
    }
  }
  // If only text content and no other attributes, return just the text
  if (other_attrs == 0 and elem.children.size() == 1
      and is<std::string>(elem.children[0])) {
    field.data(as<std::string>(elem.children[0]));
    return;
  }
  // If empty element with no other attributes
  if (other_attrs == 0 and elem.children.empty()) {
    field.null();
    return;
  }
  // Build a record with remaining attributes and children
  auto record = field.record();
  // Add non-key_attr attributes
  for (const auto& [name, value] : elem.attributes) {
    if (name != opts.key_attr) {
      auto field_name = opts.attr_prefix + name;
      record.field(field_name).data(value);
    }
  }
  // Process children (recursively, so key_attr applies at all levels)
  detail::stable_map<std::string, std::vector<const xml_node*>> children_by_name;
  std::vector<const std::string*> text_children;
  for (const auto& child : elem.children) {
    if (auto* text = try_as<std::string>(child)) {
      text_children.push_back(text);
    } else if (auto* child_elem = try_as<std::unique_ptr<xml_element>>(child)) {
      // Check if child has key_attr - if so, use that value as the key
      if (auto* key_val = get_key_attr_value(**child_elem, opts.key_attr)) {
        children_by_name[*key_val].push_back(&child);
      } else {
        children_by_name[(*child_elem)->name].push_back(&child);
      }
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
  for (auto& [child_key, nodes] : children_by_name) {
    if (nodes.size() > 1) {
      auto list = record.field(child_key).list();
      for (const auto* node : nodes) {
        auto& child_elem = as<std::unique_ptr<xml_element>>(*node);
        if (get_key_attr_value(*child_elem, opts.key_attr)) {
          element_value_to_data(list, *child_elem, opts, depth + 1);
        } else {
          node_to_data(list, *node, opts, depth + 1);
        }
      }
    } else {
      auto& child_elem_ptr = as<std::unique_ptr<xml_element>>(*nodes[0]);
      if (get_key_attr_value(*child_elem_ptr, opts.key_attr)) {
        element_value_to_data(record.field(child_key), *child_elem_ptr, opts,
                              depth + 1);
      } else {
        node_to_data(record.field(child_key), *nodes[0], opts, depth + 1);
      }
    }
  }
}

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
  // Add attributes with prefix (skip key_attr if set)
  for (const auto& [name, value] : elem.attributes) {
    if (not opts.key_attr.empty() and name == opts.key_attr) {
      continue;
    }
    auto field_name = opts.attr_prefix + name;
    record.field(field_name).data(value);
  }
  // Group children by name (or key_attr value if present)
  detail::stable_map<std::string, std::vector<const xml_node*>> children_by_key;
  std::vector<const std::string*> text_children;
  for (const auto& child : elem.children) {
    if (auto* text = try_as<std::string>(child)) {
      text_children.push_back(text);
    } else if (auto* child_elem = try_as<std::unique_ptr<xml_element>>(child)) {
      // Check if child has key_attr - if so, use that value as the key
      if (auto* key_val = get_key_attr_value(**child_elem, opts.key_attr)) {
        children_by_key[*key_val].push_back(&child);
      } else {
        children_by_key[(*child_elem)->name].push_back(&child);
      }
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
  for (auto& [child_key, nodes] : children_by_key) {
    if (nodes.size() > 1) {
      // Multiple elements with same key become a list
      auto list = record.field(child_key).list();
      for (const auto* node : nodes) {
        auto& child_elem = as<std::unique_ptr<xml_element>>(*node);
        if (get_key_attr_value(*child_elem, opts.key_attr)) {
          element_value_to_data(list, *child_elem, opts, depth + 1);
        } else {
          node_to_data(list, *node, opts, depth + 1);
        }
      }
    } else {
      auto* child_elem_ptr = try_as<std::unique_ptr<xml_element>>(*nodes[0]);
      if (child_elem_ptr
          and get_key_attr_value(**child_elem_ptr, opts.key_attr)) {
        element_value_to_data(record.field(child_key), **child_elem_ptr, opts,
                              depth + 1);
      } else {
        node_to_data(record.field(child_key), *nodes[0], opts, depth + 1);
      }
    }
  }
}

/// Helper to create XML parsing functions with common boilerplate.
/// The Processor is called for each successfully parsed XML document with:
///   (builder, root, call, ctx) where builder is multi_series_builder&
template <typename Processor>
auto make_xml_function(location call, multi_series_builder::options msb_opts,
                       xml_options opts, ast::expression expr,
                       std::string_view fn_name, Processor&& process)
  -> function_ptr {
  return function_use::make(
    [call, msb_opts = std::move(msb_opts), opts = std::move(opts),
     expr = std::move(expr), fn_name = std::string{fn_name},
     process = std::forward<Processor>(process)](function_use::evaluator eval,
                                                 session ctx) mutable {
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
              auto result
                = parse_xml_dom(xml_str, opts.strip_namespaces, max_sax_depth);
              if (not result) {
                diagnostic::warning("failed to parse XML: {}", result.error())
                  .primary(call)
                  .emit(ctx);
                builder.null();
                continue;
              }
              process(builder, std::move(*result), opts, call, ctx);
            }
            return multi_series{builder.finalize()};
          },
          [&](const auto&) -> multi_series {
            diagnostic::warning("`{}` expected `string`, got `{}`", fn_name,
                                arg.type.kind())
              .primary(call)
              .emit(ctx);
            return series::null(null_type{}, arg.length());
          });
      });
    });
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
    auto key_attr = std::optional<located<std::string>>{};
    auto max_depth = std::optional<located<int64_t>>{};
    auto namespaces = std::optional<located<std::string>>{};
    auto parser = argument_parser2::function(name());
    parser.positional("input", expr, "string");
    parser.named("xpath", xpath);
    parser.named("attr_prefix", attr_prefix);
    parser.named("text_key", text_key);
    parser.named("key_attr", key_attr);
    parser.named("max_depth", max_depth);
    parser.named("namespaces", namespaces);
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(parser, true, false);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    // Build options
    auto opts = xml_options{};
    auto xpath_str = xpath ? xpath->inner : std::string{"/*"};
    if (attr_prefix) {
      opts.attr_prefix = attr_prefix->inner;
    }
    if (text_key) {
      opts.text_key = text_key->inner;
    }
    if (key_attr) {
      opts.key_attr = key_attr->inner;
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
    return make_xml_function(
      inv.call.get_location(), std::move(msb_opts), std::move(opts),
      std::move(expr), name(),
      [xpath_str = std::move(xpath_str)](
        multi_series_builder& builder, std::unique_ptr<xml_element> root,
        const xml_options& opts, location /*call*/, session /*ctx*/) {
        // Fast path: /* returns the root element directly
        if (xpath_str == "/*") {
          auto list = builder.list();
          auto record = list.record();
          element_to_record(record, *root, opts, 0);
          return;
        }
        // General XPath evaluation
        auto matches = evaluate_xpath(root.get(), xpath_str);
        if (matches.empty()) {
          builder.null();
          return;
        }
        if (matches.size() > 1) {
          auto list = builder.list();
          for (const auto* elem : matches) {
            auto record = list.record();
            element_to_record(record, *elem, opts, 0);
          }
        } else {
          auto record = builder.record();
          element_to_record(record, *matches[0], opts, 0);
        }
      });
  }
};

/// Return the Name attribute value for a <Data> element, if present.
auto data_name_attr(const xml_element& data_elem) -> const std::string* {
  for (const auto& [attr, val] : data_elem.attributes) {
    if (attr == "Name") {
      return &val;
    }
  }
  return nullptr;
}

template <typename Builder>
void append_data_value(Builder builder, const xml_element& data_elem) {
  if (not data_elem.children.empty()) {
    if (auto* text = try_as<std::string>(data_elem.children[0])) {
      builder.data(*text);
      return;
    }
  }
  builder.null();
}

/// Transform EventData for Windows Event Log.
/// Named Data elements become record fields: {x: "v"}
/// Unnamed Data elements get numeric keys: {"0": "v1", "1": "v2"}
template <typename RecordBuilder>
void transform_event_data(RecordBuilder record,
                          const std::vector<const xml_element*>& data_elems) {
  size_t unnamed_index = 0;
  for (const auto* elem : data_elems) {
    if (const auto* name = data_name_attr(*elem)) {
      append_data_value(record.field(*name), *elem);
    } else {
      append_data_value(record.field(std::to_string(unnamed_index++)), *elem);
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
      if (elem->name == "EventData") {
        std::vector<const xml_element*> data_elems;
        bool has_named = false;
        bool has_unnamed = false;
        for (const auto& event_data_child : elem->children) {
          if (auto* data_elem
              = try_as<std::unique_ptr<xml_element>>(event_data_child)) {
            if ((*data_elem)->name != "Data") {
              continue;
            }
            data_elems.push_back(data_elem->get());
            if (data_name_attr(*data_elem->get())) {
              has_named = true;
            } else {
              has_unnamed = true;
            }
          }
        }
        if (data_elems.empty()) {
          record.field(elem->name).record();
          continue;
        }
        if (not has_named and has_unnamed) {
          auto list = record.field(elem->name).list();
          for (const auto* data_elem : data_elems) {
            append_data_value(list, *data_elem);
          }
        } else {
          auto event_data_record = record.field(elem->name).record();
          transform_event_data(event_data_record, data_elems);
        }
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
    parser.positional("input", expr, "string");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(parser, true, false);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    // Fixed options for Windows Event Log
    auto opts = xml_options{};
    opts.attr_prefix = "";
    opts.strip_namespaces = true;
    return make_xml_function(
      inv.call.get_location(), std::move(msb_opts), std::move(opts),
      std::move(expr), name(),
      [](multi_series_builder& builder, std::unique_ptr<xml_element> root,
         const xml_options& opts, location call, session ctx) {
        // Find Event element (might be root or nested)
        const xml_element* event = nullptr;
        if (root->name == "Event") {
          event = root.get();
        } else {
          for (const auto& child : root->children) {
            if (auto* elem = try_as<std::unique_ptr<xml_element>>(child)) {
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
          return;
        }
        auto record = builder.record();
        winlog_to_record(record, *event, opts);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::xml

TENZIR_REGISTER_PLUGIN(tenzir::plugins::xml::parse_xml_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xml::parse_winlog_plugin)
