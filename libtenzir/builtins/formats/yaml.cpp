//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series_builder_argument_parser.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
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
#include <tenzir/view3.hpp>

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <string_view>

#include <yaml-cpp/yaml.h>

namespace tenzir::plugins::yaml {
namespace {

constexpr auto document_end_marker = "...";
constexpr auto document_start_marker = "---";

template <typename T>
auto try_as(const YAML::Node& node, auto&& guard) -> bool {
  auto value = T{};
  if (YAML::convert<T>::decode(node, value)) {
    guard.data(value);
    return true;
  }
  return false;
}

auto parse_node(auto&& guard, const YAML::Node& node, diagnostic_handler& diag)
  -> void {
  switch (node.Type()) {
    case YAML::NodeType::Undefined: {
      diagnostic::warning("yaml parser encountered undefined field").emit(diag);
      [[fallthrough]];
    }
    case YAML::NodeType::Null: {
      guard.null();
      return;
    }
    case YAML::NodeType::Scalar: {
      if (try_as<bool>(node, guard)) {
        return;
      }
      if (try_as<int64_t>(node, guard)) {
        return;
      }
      if (try_as<uint64_t>(node, guard)) {
        return;
      }
      if (try_as<double>(node, guard)) {
        return;
      }
      const auto& value_str = node.Scalar();
      guard.data_unparsed(value_str);
      return;
    }
    case YAML::NodeType::Sequence: {
      auto list = guard.list();
      for (const auto& element : node) {
        parse_node(list, element, diag);
      }
      return;
    }
    case YAML::NodeType::Map: {
      auto record = guard.record();
      for (const auto& element : node) {
        const auto& name = element.first.as<std::string>();
        parse_node(record.unflattened_field(name), element.second, diag);
      }
      return;
    }
  }
};

auto load_document(multi_series_builder& msb, const std::string& document,
                   bool must_be_map, diagnostic_handler& diag) -> void {
  bool added_event = false;
  try {
    auto node = YAML::Load(document);
    if (not node.IsDefined()) {
      diagnostic::warning("document is not valid").emit(diag);
      return;
    }
    if (must_be_map and not node.IsMap()) {
      diagnostic::warning("document is not a map").emit(diag);
      return;
    }
    return parse_node(msb, node, diag);
  } catch (const YAML::Exception& err) {
    diagnostic::warning("failed to load YAML document: {}", err.what())
      .emit(diag);
    if (added_event) {
      msb.remove_last();
    }
  }
};

auto parse_loop(generator<std::optional<std::string_view>> lines,
                diagnostic_handler& diag, multi_series_builder::options options)
  -> generator<table_slice> {
  auto dh = transforming_diagnostic_handler{
    diag,
    [&](diagnostic d) {
      d.message = fmt::format("yaml parser: {}", d.message);
      return d;
    },
  };
  auto msb = multi_series_builder{
    std::move(options),
    dh,
    modules::get_schema,
    detail::data_builder::non_number_parser,
  };
  auto document = std::string{};
  for (auto&& line : lines) {
    for (auto& v : msb.yield_ready_as_table_slice()) {
      co_yield std::move(v);
    }
    if (not line) {
      co_yield {};
      continue;
    }
    if (*line == document_end_marker) {
      if (document.empty()) {
        continue;
      }
      load_document(msb, document, true, diag);
      document.clear();
      continue;
    }
    if (*line == document_start_marker) {
      if (not document.empty()) {
        load_document(msb, document, true, diag);
        document.clear();
      }
      continue;
    }
    fmt::format_to(std::back_inserter(document), "{}\n", *line);
  }
  if (not document.empty()) {
    load_document(msb, document, true, diag);
    document.clear();
  }
  for (auto& slice : msb.finalize_as_table_slice()) {
    co_yield std::move(slice);
  }
}

template <class View>
auto print_node(auto& out, const View& value) -> void {
  if constexpr (std::is_same_v<View, data_view>) {
    return match(value, [&](const auto& value) {
      return print_node(out, value);
    });
  } else if constexpr (std::same_as<View, data_view3>) {
    return match(value, [&](const auto& value) {
      return print_node(out, value);
    });
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
  } else if constexpr (std::is_same_v<View, view<secret>>) {
    out << to_string(value);
  } else if constexpr (detail::is_any_v<View, view<double>, view<duration>,
                                        view<time>, view<pattern>, view<ip>,
                                        view<subnet>, view<enumeration>>) {
    out << fmt::to_string(data_view{value});
  } else if constexpr (std::same_as<View, view<list>>
                       or std::same_as<View, view3<list>>) {
    out << YAML::BeginSeq;
    for (const auto& element : value) {
      print_node(out, element);
    }
    out << YAML::EndSeq;
  } else if constexpr (std::same_as<View, view<record>>
                       or std::same_as<View, view3<record>>) {
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

template <class View>
auto print_document(YAML::Emitter& out, const View& row) -> void {
  out << YAML::BeginDoc;
  print_node(out, row);
  out << YAML::EndDoc;
};

class yaml_parser final : public plugin_parser {
public:
  yaml_parser() = default;

  explicit yaml_parser(multi_series_builder::options options)
    : options_{std::move(options)} {
  }

  auto name() const -> std::string override {
    return "yaml";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_loop(to_lines(std::move(input)), ctrl.diagnostics(), options_);
  }

  friend auto inspect(auto& f, yaml_parser& x) -> bool {
    return f.apply(x.options_);
  }

  multi_series_builder::options options_;
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
        auto input_type = as<record_type>(slice.schema());
        auto resolved_slice = resolve_enumerations(slice);
        auto array = check(to_record_batch(resolved_slice)->ToStructArray());
        auto out = std::make_unique<YAML::Emitter>();
        out->SetOutputCharset(YAML::EscapeNonAscii); // restrict to ASCII output
        out->SetNullFormat(YAML::LowerNull);
        out->SetIndent(2);
        for (const auto& row :
             values(as<record_type>(resolved_slice.schema()), *array)) {
          TENZIR_ASSERT(row);
          print_document(*out, *row);
        }
        // If the output failed, then we either failed to allocate memory or
        // had a mismatch between BeginSeq and EndSeq or BeginMap and EndMap;
        // all of these we cannot recover from.
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
  }
};

class yaml_plugin final : public virtual parser_plugin<yaml_parser>,
                          public virtual printer_plugin<yaml_printer> {
  auto name() const -> std::string override {
    return "yaml";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"yaml", "https://docs.tenzir.com/"
                                          "formats/yaml"};
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto opts = msb_parser.get_options(dh);
    for (auto& d : std::move(dh).collect()) {
      if (d.severity == severity::error) {
        throw std::move(d);
      }
    }
    TENZIR_ASSERT(opts);
    return std::make_unique<yaml_parser>(std::move(*opts));
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
    auto parser = argument_parser2::operator_("read_yaml");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    auto res = parser.parse(inv, ctx);
    TRY(res);
    TRY(auto opts, msb_parser.get_options(ctx.dh()));
    return std::make_unique<parser_adapter<yaml_parser>>(
      yaml_parser{std::move(opts)});
  }

  auto read_properties() const -> read_properties_t override {
    return {
      .extensions = {"yaml"},
      .mime_types = {"application/yaml", "text/yaml", "text/x-yaml"},
    };
  }
};

class parse_yaml final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parse_yaml";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    // TODO: Consider adding a `many` option to expect multiple yaml values.
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "string");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(
      parser, true, multi_series_builder_argument_parser::merge_option::hidden);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    return function_use::make(
      [call = inv.call.get_location(), msb_opts = std::move(msb_opts),
       expr = std::move(expr)](evaluator eval, session ctx) {
        return map_series(eval(expr), [&](series arg) {
          auto f = detail::overload{
            [&](const arrow::NullArray&) -> multi_series {
              return arg;
            },
            [&](const arrow::StringArray& arg) -> multi_series {
              auto builder = multi_series_builder{
                msb_opts,
                ctx,
                modules::get_schema,
                detail::data_builder::non_number_parser,
              };
              for (int64_t i = 0; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  builder.null();
                  continue;
                }
                load_document(builder, arg.GetString(i), false, ctx);
              }
              return multi_series{builder.finalize()};
            },
            [&](const auto&) -> multi_series {
              diagnostic::warning("`parse_yaml` expected `string`, got `{}`",
                                  arg.type.kind())
                .primary(call)
                .emit(ctx);
              return series::null(null_type{}, arg.length());
            },
          };
          return match(*arg.array, f);
        });
      });
  }
};

class write_yaml final
  : public virtual operator_plugin2<writer_adapter<yaml_printer>> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<writer_adapter<yaml_printer>>(yaml_printer{});
  }

  auto write_properties() const -> write_properties_t override {
    return {.extensions = {"yaml"}};
  }
};

class print_yaml final : public virtual function_plugin {
  auto name() const -> std::string override {
    return "print_yaml";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto include_document_markers = std::optional<location>{};
    auto parser = argument_parser2::function(name());
    parser.positional("input", expr, "any");
    parser.named("include_document_markers", include_document_markers);
    TRY(parser.parse(inv, ctx));
    return function_use::make(
      [call = inv.call.get_location(), expr = std::move(expr),
       include_document_markers](evaluator eval, session) -> multi_series {
        return map_series(eval(expr), [&](series values) -> multi_series {
          if (values.type.kind().is<null_type>()) {
            auto builder = type_to_arrow_builder_t<string_type>{};
            for (int64_t i = 0; i < values.length(); ++i) {
              check(builder.Append("null"));
            }
            return series{string_type{}, check(builder.Finish())};
          }
          const auto work = [&](const auto& arr) -> multi_series {
            YAML::Emitter out;
            out.SetNullFormat(YAML::LowerNull);
            auto builder = type_to_arrow_builder_t<string_type>{};
            for (auto row : values3(arr)) {
              if (not row) {
                check(builder.Append("null"));
                continue;
              }
              if (include_document_markers) {
                print_document(out, *row);
              } else {
                print_node(out, *row);
              }
              auto str = std::string_view{out.c_str(), out.size()};
              check(builder.Append(str));
            }
            return series{string_type{}, check(builder.Finish())};
          };
          const auto resolved = resolve_enumerations(std::move(values));
          return match(*resolved.array, work);
        });
      });
  }
};

} // namespace
} // namespace tenzir::plugins::yaml

TENZIR_REGISTER_PLUGIN(tenzir::plugins::yaml::yaml_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::yaml::read_yaml)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::yaml::parse_yaml)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::yaml::write_yaml)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::yaml::print_yaml)
