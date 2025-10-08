//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors

#include "tenzir/tql2/parser.hpp"

#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/numeric/bool.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/load_contents.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/package.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <algorithm>
#include <string_view>

namespace tenzir {

namespace {

// Similar to the regular TQL identifier parser, but it also allows dashes
// because they are used in many package names in the library before we
// introduced user defined operators in packages.
auto is_valid_package_identifier(std::string_view value) -> bool {
  constexpr auto package_identifier_tail = parsers::alnum | '_' | '-';
  constexpr auto package_identifier_head = parsers::alpha | '_';
  constexpr auto package_identifier
    = package_identifier_head >> *package_identifier_tail;
  return package_identifier(value);
}

} // namespace

#define TRY_CONVERT_TO_STRING(name, field)                                     \
  if (key == #name) {                                                          \
    const auto* null = try_as<caf::none_t>(&value);                            \
    if (null) {                                                                \
      result.field = std::nullopt;                                             \
      continue;                                                                \
    }                                                                          \
    /* Convert back to yaml because we don't have access to the raw input      \
       here, and that has the highest chance of round-tripping correctly. */   \
    auto maybe_string = to_yaml(materialize(value));                           \
    if (not maybe_string) {                                                    \
      return diagnostic::error("failed to convert " #name " to string")        \
        .note("due to error: {}", maybe_string.error())                        \
        .to_error();                                                           \
    }                                                                          \
    result.field = *maybe_string;                                              \
    continue;                                                                  \
  }

#define TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT_2(name, name_key)                 \
  if (key == std::string_view{name_key}) {                                     \
    const auto* null = try_as<caf::none_t>(&value);                            \
    if (null) {                                                                \
      result.name = std::nullopt;                                              \
      continue;                                                                \
    }                                                                          \
    const auto* id = try_as<std::string_view>(&value);                         \
    if (not id) {                                                              \
      return diagnostic::error(#name " must be a string")                      \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    result.name = std::string{*id};                                            \
    continue;                                                                  \
  }

#define TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(name)                             \
  TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT_2(name, #name)

#define TRY_ASSIGN_STRING_TO_RESULT(name)                                      \
  if (key == #name) {                                                          \
    const auto* id = try_as<std::string_view>(&value);                         \
    if (not id) {                                                              \
      return diagnostic::error(#name " must be a string")                      \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    result.name = std::string{*id};                                            \
    continue;                                                                  \
  }

#define TRY_ASSIGN_BOOL_TO_RESULT(name)                                        \
  if (key == #name) {                                                          \
    const auto* x = try_as<view<bool>>(&value);                                \
    if (not x) {                                                               \
      return diagnostic::error(#name " must be a bool").to_error();            \
    }                                                                          \
    result.name = *x;                                                          \
    continue;                                                                  \
  }

#define TRY_ASSIGN_RECORD_TO_RESULT(name)                                      \
  if (key == #name) {                                                          \
    auto const* x = try_as<view<record>>(&value);                              \
    if (not x) {                                                               \
      return diagnostic::error(#name " must be a record")                      \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    result.name = materialize(*x);                                             \
    continue;                                                                  \
  }

#define TRY_ASSIGN_MAP_TO_RESULT(name, value_type)                             \
  if (key == #name) {                                                          \
    const auto* x = try_as<view<record>>(&value);                              \
    if (not x) {                                                               \
      return diagnostic::error(#name " must be a record")                      \
        .note("got {}", value)                                                 \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    for (auto const& [key, value] : *x) {                                      \
      auto const* value_record = try_as<view<record>>(&value);                 \
      if (not value_record) {                                                  \
        return diagnostic::error(#name " values must be records")              \
          .note("while parsing key {} for field " #name, key)                  \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      auto parsed_value = value_type::parse(*value_record);                    \
      if (not parsed_value) {                                                  \
        return diagnostic::error(parsed_value.error())                         \
          .note("while parsing key {} for field " #name, key)                  \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      result.name[std::string{key}] = *parsed_value;                           \
    }                                                                          \
    continue;                                                                  \
  }

#define TRY_ASSIGN_NESTED_MAP_TO_RESULT(name, value_type)                      \
  if (key == #name) {                                                          \
    const auto* x = try_as<view<record>>(&value);                              \
    if (not x) {                                                               \
      return diagnostic::error(#name " must be a record")                      \
        .note("got {}", value)                                                 \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    for (auto const& [key, value] : *x) {                                      \
      auto const* value_record = try_as<view<record>>(&value);                 \
      if (not value_record) {                                                  \
        return diagnostic::error(#name " values must be records")              \
          .note("while parsing key {} for field " #name, key)                  \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      auto parsed_value = value_type::parse(*value_record);                    \
      if (not parsed_value) {                                                  \
        return diagnostic::error(parsed_value.error())                         \
          .note("while parsing key {} for field " #name, key)                  \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      auto components = detail::split(key, "::");                              \
      TENZIR_ASSERT(not components.empty());                                   \
      auto path = std::vector<std::string>{};                                  \
      std::transform(components.begin(), components.end(),                     \
                     std::back_inserter(path), [](auto component) {            \
                       return std::string{component};                          \
                     });                                                       \
      result.name[std::move(path)] = *parsed_value;                            \
    }                                                                          \
    continue;                                                                  \
  }

#define TRY_ASSIGN_STRINGMAP_TO_RESULT(name)                                   \
  if (key == #name) {                                                          \
    const auto* x = try_as<view<record>>(&value);                              \
    if (not x) {                                                               \
      return diagnostic::error(#name " must be a record")                      \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    for (auto const& [key, value] : *x) {                                      \
      auto const* value_string = try_as<std::string_view>(&value);             \
      if (not value_string) {                                                  \
        return diagnostic::error(#name " values must be strings")              \
          .note("while parsing key {} for field " #name, key)                  \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      result.name[std::string{key}] = std::string{*value_string};              \
    }                                                                          \
    continue;                                                                  \
  }

#define TRY_ASSIGN_STRINGMAP_CONVERSION_TO_RESULT(name)                        \
  if (key == #name) {                                                          \
    const auto* x = try_as<view<record>>(&value);                              \
    if (not x) {                                                               \
      return diagnostic::error(#name " must be a record")                      \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    for (auto const& [key, value] : *x) {                                      \
      /* Convert back to yaml because we don't have access to the raw input    \
         here, and that has the highest chance of round-tripping correctly. */ \
      auto maybe_string = to_yaml(materialize(value));                         \
      if (not maybe_string) {                                                  \
        return diagnostic::error("failed to convert " #name " to string")      \
          .note("due to error: {}", maybe_string.error())                      \
          .to_error();                                                         \
      }                                                                        \
      result.name[std::string{key}] = *maybe_string;                           \
    }                                                                          \
    continue;                                                                  \
  }

#define REQUIRED_FIELD(path)                                                   \
  if (result.path.empty()) {                                                   \
    return diagnostic::error(#path " must be provided")                        \
      .note("invalid package definition")                                      \
      .to_error();                                                             \
  }

#define TRY_ASSIGN_STRUCTURE_TO_RESULT(name, type)                             \
  if (key == #name) {                                                          \
    const auto* x = try_as<view<record>>(&value);                              \
    if (not x) {                                                               \
      return diagnostic::error(#name " values must be records")                \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    auto parsed = type::parse(*x);                                             \
    if (not parsed) {                                                          \
      return diagnostic::error(parsed.error())                                 \
        .note("while parsing key {} for field " #name, key)                    \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    result.name = *parsed;                                                     \
    continue;                                                                  \
  }

#define TRY_ASSIGN_LIST(name, inner_type, target)                              \
  if (key == #name) {                                                          \
    const auto* item_list = try_as<view<list>>(&value);                        \
    if (not item_list) {                                                       \
      return diagnostic::error(#name " must be a list")                        \
        .note("got a {} instead", type::infer(materialize(value)))             \
        .to_error();                                                           \
    }                                                                          \
    size_t pos = 0;                                                            \
    for (auto item_view : *item_list) {                                        \
      const auto* item_record = try_as<view<record>>(&item_view);              \
      if (not item_record) {                                                   \
        return diagnostic::error("list item must be a record")                 \
          .note("while trying to parse item {} of list " #name, pos)           \
          .note("got a {} instead", type::infer(materialize(item_view)))       \
          .to_error();                                                         \
      }                                                                        \
      auto item = inner_type::parse(*item_record);                             \
      if (not item) {                                                          \
        return diagnostic::error(item.error())                                 \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      (target).push_back(*item);                                               \
      ++pos;                                                                   \
    }                                                                          \
    continue;                                                                  \
  }

#define TRY_ASSIGN_LIST_TO_RESULT(name, inner_type)                            \
  TRY_ASSIGN_LIST(name, inner_type, result.name)

#define TRY_ASSIGN_VECTOR_OF_STRING(name)                                      \
  if (key == #name) {                                                          \
    const auto* null = try_as<caf::none_t>(&value);                            \
    if (null) {                                                                \
      continue;                                                                \
    }                                                                          \
    const auto* lst = try_as<view<list>>(&value);                              \
    if (not lst) {                                                             \
      return diagnostic::error(#name " must be a list")                        \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    for (const auto& elem : *lst) {                                            \
      const auto* str = try_as<std::string_view>(elem);                        \
      if (not str) {                                                           \
        return diagnostic::error(#name " must be a list of strings")           \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      result.name.emplace_back(*str);                                          \
    }                                                                          \
    continue;                                                                  \
  }

auto package_input::parse(const view<record>& data)
  -> caf::expected<package_input> {
  auto result = package_input{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(name)
    TRY_ASSIGN_STRING_TO_RESULT(type)
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(description)
    TRY_CONVERT_TO_STRING(default, default_);
    TENZIR_WARN("ignoring unknown key `{}` in `input` entry in package "
                "definition",
                key);
  }
  REQUIRED_FIELD(name);
  return result;
}

auto package_source::parse(const view<record>& data)
  -> caf::expected<package_source> {
  auto result = package_source{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(repository)
    TRY_ASSIGN_STRING_TO_RESULT(directory)
    TRY_ASSIGN_STRING_TO_RESULT(revision)
    TENZIR_WARN("ignoring unknown key `{}` in `source` entry in package "
                "definition",
                key);
  }
  REQUIRED_FIELD(repository)
  REQUIRED_FIELD(directory)
  REQUIRED_FIELD(revision)
  return result;
}

auto package_config::parse(const view<record>& data)
  -> caf::expected<package_config> {
  auto result = package_config{};
  for (const auto& [key, value] : data) {
    TRY_CONVERT_TO_STRING(version, version);
    TRY_ASSIGN_STRUCTURE_TO_RESULT(source, package_source);
    TRY_ASSIGN_STRINGMAP_CONVERSION_TO_RESULT(inputs);
    TRY_ASSIGN_RECORD_TO_RESULT(overrides);
    TRY_ASSIGN_RECORD_TO_RESULT(metadata);
    TRY_ASSIGN_BOOL_TO_RESULT(disabled);
    TENZIR_WARN("ignoring unknown key `{}` in `config` entry in package "
                "definition",
                key);
  }
  return result;
}

namespace {

auto load_tql_with_frontmatter(std::string_view input)
  -> caf::expected<record> {
  auto rec = record{};
  // Search optional frontmatter, but allow a `#!` shebang lines before that.
  auto input_ = input;
  while (input_.starts_with("#!")) {
    auto after_shebang = input_.find('\n');
    if (after_shebang == std::string_view::npos) {
      // Unexpected, but we let the real parser handle this.
      break;
    }
    input_.remove_prefix(after_shebang + 1);
  }
  if (input_.starts_with("---\n")) {
    auto frontmatter_end = input_.find("\n---\n");
    if (frontmatter_end == std::string_view::npos) {
      return caf::make_error(ec::parse_error,
                             "missing end marker of tql frontmatter");
    }
    auto frontmatter = input_.substr(4, frontmatter_end);
    auto metadata = from_yaml(frontmatter);
    if (not metadata) {
      return metadata.error();
    }
    auto* maybe_rec = try_as<record>(*metadata);
    if (not maybe_rec) {
      return caf::make_error(ec::parse_error,
                             "tql frontmatter is not a record");
    }
    rec = *maybe_rec;
    input_.remove_prefix(frontmatter_end + 5);
    input_ = detail::trim_front(input_);
  }
  rec.emplace("definition", std::string{input_});
  return rec;
}

} // namespace

auto package_operator::parse(const view<record>& data)
  -> caf::expected<package_operator> {
  auto result = package_operator{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(definition);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(description);
    TENZIR_WARN("ignoring unknown key `{}` in `operator` entry in package "
                "definition",
                key);
  }
  REQUIRED_FIELD(definition)
  return result;
}

auto package_operator::parse(std::string_view input)
  -> caf::expected<package_operator> {
  TRY(auto rec, load_tql_with_frontmatter(input));
  return parse(make_view(rec));
}

auto package_pipeline::parse(const view<record>& data)
  -> caf::expected<package_pipeline> {
  auto result = package_pipeline{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(definition);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(name);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(description);
    TRY_ASSIGN_BOOL_TO_RESULT(disabled);
    TRY_ASSIGN_BOOL_TO_RESULT(unstoppable);
    if (key == "restart-on-error") {
      if (is<caf::none_t>(value)) {
        continue;
      }
      // As a convenience for users, we also allow a string here and try to
      // parse the inner value in that case.
      auto value_copy = materialize(value);
      if (const auto* as_string = try_as<std::string_view>(&value)) {
        auto inner_value = from_yaml(*as_string);
        if (not inner_value) {
          return diagnostic::error("failed to parse `restart-on-error` field")
            .note("error {}", inner_value.error())
            .to_error();
        }
        value_copy = *inner_value;
      }
      const auto* on_off = try_as<bool>(&value_copy);
      const auto* retry_delay = try_as<duration>(&value_copy);
      if (not on_off and not retry_delay) {
        return diagnostic::error("`restart-on-error` must be a "
                                 "be a "
                                 "bool or a positive duration")
          .note("got `{}`", value)
          .to_error();
      }
      if (on_off) {
        result.restart_on_error
          = *on_off ? std::optional<
                        duration>{defaults::packaged_pipeline_restart_on_error}
                    : std::optional<duration>{std::nullopt};
        continue;
      }
      TENZIR_ASSERT(retry_delay);
      if (*retry_delay < duration::zero()) {
        return diagnostic::error("`restart-on-error` cannot be negative")
          .to_error();
      }
      result.restart_on_error = *retry_delay;
      continue;
    }
    // Hack: Ignore the `labels` key so we can reuse this function to
    // parse configured pipelines as well.
    if (key == "labels") {
      continue;
    }
    TENZIR_WARN("ignoring unknown key `{}` in `pipeline` entry in package "
                "definition",
                key);
  }
  REQUIRED_FIELD(definition)
  return result;
}

auto package_pipeline::parse(std::string_view input)
  -> caf::expected<package_pipeline> {
  TRY(auto rec, load_tql_with_frontmatter(input));
  return parse(make_view(rec));
}

auto package_context::parse(const view<record>& data)
  -> caf::expected<package_context> {
  auto result = package_context{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(type);
    TRY_ASSIGN_BOOL_TO_RESULT(disabled);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(description);
    TRY_ASSIGN_STRINGMAP_TO_RESULT(arguments);
  }
  REQUIRED_FIELD(type)
  return result;
}

auto package_example::parse(const view<record>& data)
  -> caf::expected<package_example> {
  auto result = package_example{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(definition);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(name);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(description);
  }
  REQUIRED_FIELD(definition)
  return result;
}

auto package_example::parse(std::string_view input)
  -> caf::expected<package_example> {
  TRY(auto rec, load_tql_with_frontmatter(input));
  return parse(make_view(rec));
}

auto package::parse(const view<record>& data) -> caf::expected<package> {
  auto result = package{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(id);
    TRY_ASSIGN_STRING_TO_RESULT(name);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(author);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(description);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(package_icon);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(author_icon);
    TRY_ASSIGN_VECTOR_OF_STRING(categories);
    TRY_ASSIGN_MAP_TO_RESULT(inputs, package_input);
    TRY_ASSIGN_NESTED_MAP_TO_RESULT(operators, package_operator);
    TRY_ASSIGN_MAP_TO_RESULT(pipelines, package_pipeline);
    TRY_ASSIGN_MAP_TO_RESULT(contexts, package_context);
    TRY_ASSIGN_STRUCTURE_TO_RESULT(config, package_config);
    TRY_ASSIGN_LIST_TO_RESULT(examples, package_example);
    TENZIR_WARN("ignoring unknown key `{}` in `package` entry in package "
                "definition",
                key);
  }
  REQUIRED_FIELD(id)
  REQUIRED_FIELD(name)
  if (not is_valid_package_identifier(result.id)) {
    return diagnostic::error("invalid package id '{}' (must match "
                             "[A-Za-z_][A-Za-z0-9_-]*)",
                             result.id)
      .to_error();
  }
  if (result.config) {
    for (auto& [name, _] : result.config->inputs) {
      if (not result.inputs.contains(name)) {
        return diagnostic::error("undeclared input value")
          .note("input {} is not part of package {}", name, result.id)
          .to_error();
      }
    }
    if (result.config->disabled) {
      for (auto& [_, pipeline] : result.pipelines) {
        pipeline.disabled = true;
      }
      for (auto& [_, context] : result.contexts) {
        context.disabled = true;
      }
    }
  }
  return result;
}

auto package_module_name(std::string_view package_id) -> std::string {
  auto sanitized = std::string{package_id};
  std::replace(sanitized.begin(), sanitized.end(), '-', '_');
  return sanitized;
}

namespace {

auto yaml_file_to_record(const std::filesystem::path& file, auto& dh)
  -> failure_or<record> {
  auto yaml = detail::load_contents(file);
  if (not yaml) {
    diagnostic::error("failed to load file")
      .note("trying to load {}", file)
      .note("error: {}", yaml.error())
      .emit(dh);
    return failure::promise();
  }
  auto data = from_yaml(*yaml);
  if (not data) {
    diagnostic::error("failed to parse yaml")
      .note("trying to load {}", file)
      .note("error: {}", data.error())
      .emit(dh);
    return failure::promise();
  }
  auto* record = try_as<tenzir::record>(&*data);
  if (not record) {
    diagnostic::error("expected a record")
      .note("trying to load {}", file)
      .emit(dh);
    return failure::promise();
  }
  return *record;
}

template <class Type>
auto load_package_part(const std::filesystem::path& file, auto& dh)
  -> failure_or<Type> {
  auto content = detail::load_contents(file);
  if (not content) {
    diagnostic::error(content.error()).note("trying to load {}", file).emit(dh);
    return failure::promise();
  }
  auto result = Type::parse(*content);
  if (not result) {
    diagnostic::error("{}", result.error()).note("from file: {}", file).emit(dh);
    return failure::promise();
  }
  return *result;
}

auto make_id(const std::filesystem::path& pkg_part,
             const std::filesystem::path& parts_base, std::string_view sep)
  -> failure_or<std::string> {
  auto ec = std::error_code{};
  auto relative = std::filesystem::relative(pkg_part, parts_base, ec);
  TENZIR_ASSERT(not ec);
  const auto* path_separator = "/";
  auto strrep = relative.string();
  auto components = detail::split(strrep, path_separator);
  auto ss = std::stringstream{};
  for (size_t i = 0; i < components.size() - 1; i++) {
    ss << components[i] << sep;
  }
  ss << relative.stem().string();
  return ss.str();
}

} // namespace

auto package::load(const std::filesystem::path& dir, diagnostic_handler& dh,
                   bool only_entities) -> failure_or<package> {
  bool had_errors = false;
  auto package_file = dir / "package.yaml";
  std::error_code ec;
  if (not std::filesystem::exists(package_file, ec)) {
    if (ec) {
      diagnostic::error("{}", ec).note("for file {}", package_file).emit(dh);
    } else {
      diagnostic::error("package.yaml does not exist")
        .note("in package directory {}", dir)
        .emit(dh);
    }
    return failure::promise();
  }
  TRY(auto package_record, yaml_file_to_record(package_file, dh));
  if (only_entities) {
    package_record.erase("pipelines");
    package_record.erase("contexts");
  }
  auto parsed_package = package::parse(make_view(package_record));
  if (not parsed_package) {
    diagnostic::error("failed to parse package.yaml")
      .note("in package directory {}", dir)
      .note("encountered error {}", parsed_package.error())
      .emit(dh);
    return failure::promise();
  }
  if (parsed_package->id.find('-') != std::string::npos) {
    diagnostic::warning("package id '{}' contains '-' characters; normalized "
                        "module name will be '{}'",
                        parsed_package->id,
                        package_module_name(parsed_package->id))
      .emit(dh);
  }
  if (parsed_package->id != dir.filename()) {
    diagnostic::warning("name mismatch: found package {} in directory {}/",
                        parsed_package->id, dir)
      .emit(dh);
  }
  // Support storing the `config` part in a separate file in the same
  // directory.
  auto config_file = dir / "config.yaml";
  if (not std::filesystem::exists(config_file, ec)) {
    if (ec) {
      diagnostic::error("{}", ec).note("for file {}", config_file).emit(dh);
      had_errors = true;
    }
  } else {
    if (parsed_package->config.has_value()) {
      diagnostic::error(
        "invalid package definition: found both an inline config "
        "section and a separate 'config.yaml'")
        .note("in package directory {}/", dir)
        .emit(dh);
      had_errors = true;
    }
    TRY(auto config_record, yaml_file_to_record(config_file, dh));
    auto parsed_config = package_config::parse(make_view(config_record));
    if (not parsed_config) {
      diagnostic::error("failed to parse package config")
        .note("in package directory {}/", dir)
        .note("got error: {}", parsed_config.error())
        .emit(dh);
      had_errors = true;
    } else {
      parsed_package->config = std::move(*parsed_config);
    }
  }
  if (not only_entities) {
    auto pipelines_dir = dir / "pipelines";
    if (not std::filesystem::exists(pipelines_dir, ec)) {
      if (ec) {
        diagnostic::error("{}", ec)
          .note("while trying to load {}", pipelines_dir)
          .emit(dh);
        had_errors = true;
      }
    } else {
      try {
        auto pipelines
          = std::filesystem::recursive_directory_iterator{pipelines_dir}
            | std::views::filter([&](const std::filesystem::path& path) {
                return path.extension() == ".tql";
              });
        for (const auto& pipeline_file : pipelines) {
          if (auto pipeline
              = load_package_part<package_pipeline>(pipeline_file, dh)) {
            TRY(auto id, make_id(pipeline_file.path(), pipelines_dir, "/"));
            parsed_package->pipelines.emplace(std::move(id),
                                              std::move(*pipeline));
          }
        }
      } catch (const std::exception& e) {
        diagnostic::error("{}", e)
          .note("while trying to load pipelines in {}", pipelines_dir)
          .emit(dh);
        return failure::promise();
      }
    }
  }
  auto operators_dir = dir / "operators";
  if (not std::filesystem::exists(operators_dir, ec)) {
    if (ec) {
      diagnostic::error("{}", ec)
        .note("while trying to load {}", operators_dir)
        .emit(dh);
      had_errors = true;
    }
  } else {
    try {
      auto operators
        = std::filesystem::recursive_directory_iterator{operators_dir}
          | std::views::filter([&](const std::filesystem::path& path) {
              return path.extension() == ".tql";
            });
      for (const auto& operator_file : operators) {
        if (auto operator_
            = load_package_part<package_operator>(operator_file, dh)) {
          auto ec = std::error_code{};
          auto relative
            = std::filesystem::relative(operator_file, operators_dir, ec);
          TENZIR_ASSERT(not ec);
          const auto* path_separator = "/";
          auto strrep = relative.replace_extension("").generic_string();
          auto components = detail::split(strrep, path_separator);
          TENZIR_ASSERT(not components.empty());
          auto path = std::vector<std::string>{};
          std::transform(components.begin(), components.end(),
                         std::back_inserter(path), [](auto component) {
                           return std::string{component};
                         });
          bool valid = true;
          for (const auto& seg : path) {
            if (not token::parsers::identifier(seg)) {
              diagnostic::error("invalid operator path segment '{}' (must "
                                "match [A-Za-z_][A-Za-z0-9_]*)",
                                seg)
                .note("in operator file {}", operator_file.path())
                .emit(dh);
              valid = false;
              break;
            }
          }
          if (! valid) {
            had_errors = true;
            continue;
          }
          parsed_package->operators.emplace(std::move(path),
                                            std::move(*operator_));
        }
      }
    } catch (const std::exception& e) {
      diagnostic::error("{}", e)
        .note("while trying to load operators in {}", operators_dir)
        .emit(dh);
      return failure::promise();
    }
  }
  auto examples_dir = dir / "examples";
  if (not std::filesystem::exists(examples_dir, ec)) {
    if (ec) {
      diagnostic::error("{}", ec)
        .note("while trying to load {}", examples_dir)
        .emit(dh);
      had_errors = true;
    }
  } else {
    try {
      auto examples
        = std::filesystem::directory_iterator{examples_dir}
          | std::views::filter([&](const std::filesystem::path& path) {
              return path.extension() == ".tql";
            });
      for (const auto& example_file : examples) {
        if (auto example_
            = load_package_part<package_example>(example_file, dh)) {
          parsed_package->examples.push_back(std::move(*example_));
        }
      }
    } catch (const std::exception& e) {
      diagnostic::error("{}", e)
        .note("while trying to load examples in {}", examples_dir)
        .emit(dh);
      return failure::promise();
    }
  }
  if (had_errors) {
    return failure::promise();
  }
  return *parsed_package;
}

auto package_source::to_record() const -> record {
  return record{
    {"repository", repository},
    {"directory", directory},
    {"revision", revision},
  };
}

auto package_config::to_record() const -> record {
  auto inputs_map = record{};
  for (auto [key, value] : inputs) {
    inputs_map[key] = value;
  }
  auto result = record{
    {"inputs", std::move(inputs_map)},
    {"overrides", overrides},
    {"disabled", disabled},
  };
  if (source) {
    result["source"] = source->to_record();
  }
  if (version) {
    result["version"] = *version;
  }
  if (not metadata.empty()) {
    result["metadata"] = metadata;
  }
  return result;
}

auto package_input::to_record() const -> record {
  auto result = record{
    {"name", name},
    {"description", description},
    {"type", type},
  };
  if (default_) {
    result["default"] = default_;
  }
  return result;
}

auto package_context::to_record() const -> record {
  auto arguments_record = record{};
  for (auto const& [key, value] : arguments) {
    arguments_record[key] = value;
  }
  return record{
    {"type", type},
    {"description", description},
    {"arguments", arguments_record},
    {"disabled", disabled},
  };
}

auto package_example::to_record() const -> record {
  return record{
    {"name", name},
    {"description", description},
    {"definition", definition},
  };
}

auto package_operator::to_record() const -> record {
  auto result = record{
    {"description", description},
    {"definition", definition},
  };
  return result;
}

auto package_pipeline::to_record() const -> record {
  auto result = record{
    {"name", name},
    {"description", description},
    {"definition", definition},
    {"disabled", disabled},
    {"unstoppable", unstoppable},
  };
  if (restart_on_error) {
    result["restart-on-error"] = restart_on_error;
  }
  return result;
}

auto package::to_record() const -> record {
  record info_record;
  info_record["id"] = id;
  info_record["name"] = name;
  info_record["author"] = author;
  info_record["description"] = description;
  info_record["author_icon"] = author_icon;
  info_record["package_icon"] = package_icon;
  auto cats = list{};
  cats.reserve(categories.size());
  for (const auto& cat : categories) {
    cats.emplace_back(cat);
  }
  info_record["categories"] = std::move(cats);
  if (config) {
    info_record["config"] = config->to_record();
  }
  auto operators_record = record{};
  for (auto const& [operator_path, operator_] : operators) {
    auto operator_id = fmt::format("{}", fmt::join(operator_path, "::"));
    operators_record[operator_id] = operator_.to_record();
  }
  info_record["operators"] = std::move(operators_record);
  auto pipelines_record = record{};
  for (auto const& [pipeline_id, pipeline] : pipelines) {
    pipelines_record[pipeline_id] = pipeline.to_record();
  }
  info_record["pipelines"] = std::move(pipelines_record);
  auto contexts_record = record{};
  for (auto const& [context_id, context] : contexts) {
    contexts_record[context_id] = context.to_record();
  }
  info_record["contexts"] = std::move(contexts_record);
  auto examples_list = list{};
  examples_list.reserve(examples.size());
  for (auto const& example : examples) {
    examples_list.emplace_back(example.to_record());
  }
  info_record["examples"] = std::move(examples_list);
  auto inputs_record = record{};
  for (auto const& [input_name, input] : inputs) {
    inputs_record[input_name] = input.to_record();
  }
  info_record["inputs"] = std::move(inputs_record);
  return info_record;
}

auto build_package_operator_module(const package& pkg, diagnostic_handler& dh)
  -> failure_or<std::unique_ptr<module_def>> {
  auto module = std::make_unique<module_def>();
  auto module_name = package_module_name(pkg.id);
  auto& pkg_entry = module->defs[module_name];
  TENZIR_ASSERT(! pkg_entry.mod);
  pkg_entry.mod = std::make_unique<module_def>();
  auto* pkg_mod = pkg_entry.mod.get();
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto ensure_module
    = [&](module_def& root_mod, const auto& path) -> module_def* {
    module_def* current = &root_mod;
    for (const auto& path_segment : path) {
      auto& set = current->defs[std::string{path_segment}];
      if (! set.mod) {
        set.mod = std::make_unique<module_def>();
      }
      current = set.mod.get();
    }
    return current;
  };
  for (const auto& [op_name, op] : pkg.operators) {
    auto parsed = parse_pipeline_with_bad_diagnostics(op.definition, ctx);
    if (! parsed) {
      diagnostic::error("failed to parse operator '{}' in package '{}'",
                        fmt::join(op_name, "::"), pkg.id)
        .emit(dh);
      return failure::promise();
    }
    auto pipe = std::move(*parsed);
    auto start_idx = size_t{0};
    if (op_name.size() - start_idx < 1) {
      diagnostic::error("invalid operator path in package '{}': {}", pkg.id,
                        fmt::join(op_name, "::"))
        .emit(dh);
      return failure::promise();
    }
    auto head_span = std::span<const std::string>{op_name}.subspan(
      start_idx, op_name.size() - start_idx - 1);
    auto* parent = ensure_module(*pkg_mod, head_span);
    auto& set = parent->defs[op_name.back()];
    set.op = operator_def{user_defined_operator{std::move(pipe)}};
  }
  if (pkg_mod->defs.empty()) {
    pkg_entry.mod.reset();
    module->defs.erase(module_name);
  }
  return module;
}

} // namespace tenzir
