//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/adaptive_table_slice_builder.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/to_lines.hpp>
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

auto parse_node(auto& guard, const YAML::Node& node,
                operator_control_plane& ctrl) -> void {
  switch (node.Type()) {
    case YAML::NodeType::Undefined:
    case YAML::NodeType::Null: {
      break;
    }
    case YAML::NodeType::Scalar: {
      if (auto as_bool = bool{}; YAML::convert<bool>::decode(node, as_bool)) {
        if (auto err = guard.add(as_bool)) [[unlikely]] {
          diagnostic::error("failed to append value: {}", err)
            .emit(ctrl.diagnostics());
        }
        break;
      }
      // TODO: Do not attempt to parse pattern, map, list, and record here.
      if (auto as_data = data{}; parsers::data(node.Scalar(), as_data)) {
        if (auto err = guard.add(make_data_view(as_data))) [[unlikely]] {
          diagnostic::error("failed to append value: {}", err)
            .emit(ctrl.diagnostics());
        }
        break;
      }
      if (auto err = guard.add(make_view(node.Scalar()))) [[unlikely]] {
        diagnostic::error("failed to append value: {}", err)
          .emit(ctrl.diagnostics());
      }
      break;
    }
    case YAML::NodeType::Sequence: {
      auto list = guard.push_list();
      for (const auto& element : node) {
        parse_node(list, element, ctrl);
      }
      break;
    }
    case YAML::NodeType::Map: {
      auto record = guard.push_record();
      for (const auto& element : node) {
        auto field = record.push_field(element.first.Scalar());
        parse_node(field, element.second, ctrl);
      }
      break;
    }
  }
};

auto load_document(adaptive_table_slice_builder& builder,
                   std::string&& document, operator_control_plane& ctrl)
  -> void {
  auto row = builder.push_row();
  try {
    auto node = YAML::Load(document);
    if (not node.IsMap()) {
      diagnostic::error("document is not a map").emit(ctrl.diagnostics());
      return;
    }
    for (const auto& element : node) {
      auto field = row.push_field(element.first.as<std::string>());
      parse_node(field, element.second, ctrl);
    }
  } catch (const YAML::Exception& err) {
    diagnostic::error("failed to load YAML document: {}", err.what())
      .emit(ctrl.diagnostics());
    row.cancel();
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
    die("unreachable");
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

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return std::invoke(
      [](generator<std::optional<std::string_view>> lines,
         operator_control_plane& ctrl) -> generator<table_slice> {
        auto builder = adaptive_table_slice_builder{};
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
        co_yield builder.finish();
      },
      to_lines(std::move(input)), ctrl);
  }

  friend auto inspect(auto& f, yaml_parser& x) -> bool {
    (void)f;
    (void)x;
    return true;
  }
};

class yaml_printer final : public plugin_printer {
public:
  yaml_printer() = default;

  auto name() const -> std::string override {
    return "yaml";
  }

  auto instantiate([[maybe_unused]] type input_schema,
                   operator_control_plane& ctrl) const
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
        for (const auto& row : values(input_type, *array)) {
          TENZIR_ASSERT_CHEAP(row);
          print_document(*out, *row);
        }
        // If the output failed, then we either failed to allocate memory or had
        // a mismatch between BeginSeq and EndSeq or BeginMap and EndMap; all of
        // these we cannot recover from.
        if (not out->good()) {
          ctrl.abort(
            caf::make_error(ec::logic_error, "failed to format YAML document"));
          co_return;
        }
        // We create a chunk out of the emitter directly, thus avoiding needing
        // to copy out the string from the emitter's stream object. The
        // ownership of the emitter is thus transferred into the deleter of the
        // chunk.
        const auto* data = out->c_str();
        const auto size = out->size();
        co_yield chunk::make(data, size, [out = std::move(out)]() noexcept {
          (void)out;
        });
      });
  }

  auto allows_joining() const -> bool override {
    return true;
  };

  friend auto inspect(auto& f, yaml_printer& x) -> bool {
    (void)f;
    (void)x;
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
    auto parser = argument_parser{"yaml", "https://docs.tenzir.com/next/"
                                          "formats/yaml"};
    parser.parse(p);
    return std::make_unique<yaml_parser>();
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{"yaml", "https://docs.tenzir.com/next/"
                                          "formats/yaml"};
    parser.parse(p);
    return std::make_unique<yaml_printer>();
  }
};

} // namespace
} // namespace tenzir::plugins::yaml

TENZIR_REGISTER_PLUGIN(tenzir::plugins::yaml::plugin)
