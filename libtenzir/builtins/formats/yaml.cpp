//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/error.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <yaml-cpp/yaml.h>

namespace tenzir::plugins::yaml {
namespace {

constexpr auto document_end_marker = "...";
constexpr auto document_start_marker = "---";

auto parse_node(auto& guard, const YAML::Node& node, exec_ctx ctx) -> void {
  switch (node.Type()) {
    case YAML::NodeType::Undefined:
    case YAML::NodeType::Null: {
      guard.null();
      break;
    }
    case YAML::NodeType::Scalar: {
      if (auto as_bool = bool{}; YAML::convert<bool>::decode(node, as_bool)) {
        guard.data(as_bool);
        break;
      }
      // TODO: Do not attempt to parse pattern, map, list, and record here.
      if (auto as_data = data{}; parsers::data(node.Scalar(), as_data)) {
        guard.data(make_data_view(as_data));
        break;
      }
      guard.data(make_view(node.Scalar()));
      break;
    }
    case YAML::NodeType::Sequence: {
      auto list = guard.list();
      for (const auto& element : node) {
        parse_node(list, element, ctrl);
      }
      break;
    }
    case YAML::NodeType::Map: {
      auto record = guard.record();
      for (const auto& element : node) {
        const auto& name = element.first.as<std::string>();
        auto field = record.field(name);
        parse_node(field, element.second, ctrl);
      }
      break;
    }
  }
};

auto load_document(series_builder& builder, std::string&& document,
                   exec_ctx ctx) -> void {
  auto record = builder.record();
  try {
    auto node = YAML::Load(document);
    if (not node.IsMap()) {
      diagnostic::error("document is not a map").emit(ctrl.diagnostics());
      return;
    }
    for (const auto& element : node) {
      const auto& name = element.first.as<std::string>();
      auto field = record.field(name);
      parse_node(field, element.second, ctrl);
    }
  } catch (const YAML::Exception& err) {
    diagnostic::error("failed to load YAML document: {}", err.what())
      .emit(ctrl.diagnostics());
    builder.remove_last();
  }
};

template <class View>
auto print_node(auto& out, const View& value) -> void {
  if constexpr (std::is_same_v<View, data_view>) {
    return caf::visit(
      [&](const auto& value) {
        return print_node(out, value);
      },
      value);
  } else if constexpr (std::is_same_v<View, caf::none_t>) {
    out << YAML::Null;
  } else if constexpr (std::is_same_v<View, view<bool>>) {
    out << (value ? "true" : "false");
  } else if constexpr (detail::is_any_v<View, view<int64_t>, view<uint64_t>>) {
    out << value;
  } else if constexpr (std::is_same_v<View, view<std::string>>) {
    out << std::string{value};
  } else if constexpr (std::is_same_v<View, view<blob>>) {
    out << detail::base64::encode(value);
  } else if constexpr (detail::is_any_v<View, view<double>, view<duration>,
                                        view<time>, view<pattern>, view<ip>,
                                        view<subnet>, view<enumeration>>) {
    out << fmt::to_string(data_view{value});
  } else if constexpr (std::is_same_v<View, view<list>>) {
    out << YAML::BeginSeq;
    for (const auto& element : value) {
      print_node(out, element);
    }
    out << YAML::EndSeq;
  } else if constexpr (std::is_same_v<View, view<record>>) {
    out << YAML::BeginMap;
    for (const auto& [key, element] : value) {
      out << YAML::Key;
      print_node(out, key);
      out << YAML::Value;
      print_node(out, element);
    }
    out << YAML::EndMap;
  } else if constexpr (detail::is_any_v<View, view<pattern>, view<map>>) {
    TENZIR_UNREACHABLE();
  } else {
    static_assert(detail::always_false_v<View>, "missing overload");
  }
};

auto print_document(YAML::Emitter& out, const view<record>& row) -> void {
  out << YAML::BeginDoc;
  print_node(out, row);
  out << YAML::EndDoc;
};

class yaml_parser final : public plugin_parser {
public:
  yaml_parser() = default;

  auto name() const -> std::string override {
    return "yaml";
  }

  auto instantiate(generator<chunk_ptr> input, exec_ctx ctx) const
    -> std::optional<generator<table_slice>> override {
    return std::invoke(
      [](generator<std::optional<std::string_view>> lines,
         exec_ctx ctx) -> generator<table_slice> {
        auto builder = series_builder{};
        auto document = std::string{};
        for (auto&& line : lines) {
          if (not line) {
            co_yield {};
            continue;
          }
          if (*line == document_end_marker) {
            if (document.empty()) {
              continue;
            }
            load_document(builder, std::exchange(document, {}), ctrl);
            continue;
          }
          if (*line == document_start_marker) {
            if (not document.empty()) {
              load_document(builder, std::exchange(document, {}), ctrl);
            }
            continue;
          }
          fmt::format_to(std::back_inserter(document), "{}\n", *line);
        }
        if (not document.empty()) {
          load_document(builder, std::exchange(document, {}), ctrl);
        }
        for (auto&& slice : builder.finish_as_table_slice("tenzir.yaml")) {
          co_yield std::move(slice);
        }
      },
      to_lines(std::move(input)), ctrl);
  }

  friend auto inspect(auto& f, yaml_parser& x) -> bool {
    return f.object(x).fields();
  }
};

class yaml_printer final : public plugin_printer {
public:
  yaml_printer() = default;

  auto name() const -> std::string override {
    return "yaml";
  }

  auto instantiate([[maybe_unused]] type input_schema, exec_ctx ctx) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    return printer_instance::make(
      [&ctrl](table_slice slice) -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto input_type = caf::get<record_type>(slice.schema());
        auto resolved_slice = resolve_enumerations(slice);
        auto array
          = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
        auto out = std::make_unique<YAML::Emitter>();
        out->SetOutputCharset(YAML::EscapeNonAscii); // restrict to ASCII output
        out->SetNullFormat(YAML::LowerNull);
        out->SetIndent(2);
        for (const auto& row :
             values(caf::get<record_type>(resolved_slice.schema()), *array)) {
          TENZIR_ASSERT(row);
          print_document(*out, *row);
        }
        // If the output failed, then we either failed to allocate memory or had
        // a mismatch between BeginSeq and EndSeq or BeginMap and EndMap; all of
        // these we cannot recover from.
        if (not out->good()) {
          diagnostic::error("failed to format YAML document")
            .emit(ctrl.diagnostics());
          co_return;
        }
        const auto* data = reinterpret_cast<const std::byte*>(out->c_str());
        auto size = out->size();
        auto meta = chunk_metadata{.content_type = "application/x-yaml"};
        auto chunk = chunk::make(
          data, size,
          [emitter = std::move(out)]() noexcept {
            static_cast<void>(emitter);
          },
          std::move(meta));
        co_yield chunk;
      });
  }

  auto allows_joining() const -> bool override {
    return true;
  };

  auto prints_utf8() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, yaml_printer& x) -> bool {
    return f.object(x).fields();
    return true;
  }
};

class plugin final : public virtual parser_plugin<yaml_parser>,
                     public virtual printer_plugin<yaml_printer> {
  auto name() const -> std::string override {
    return "yaml";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"yaml", "https://docs.tenzir.com/"
                                          "formats/yaml"};
    parser.parse(p);
    return std::make_unique<yaml_parser>();
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{"yaml", "https://docs.tenzir.com/"
                                          "formats/yaml"};
    parser.parse(p);
    return std::make_unique<yaml_printer>();
  }
};

class read_yaml final
  : public virtual operator_plugin2<parser_adapter<yaml_parser>> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("read_yaml").parse(inv, ctx).ignore();
    return std::make_unique<parser_adapter<yaml_parser>>();
  }
};

} // namespace
} // namespace tenzir::plugins::yaml

TENZIR_REGISTER_PLUGIN(tenzir::plugins::yaml::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::yaml::read_yaml)
