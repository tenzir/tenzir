//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors

#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/numeric/bool.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/package.hpp>
#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <string_view>

namespace tenzir {

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
      target.push_back(*item);                                                 \
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
      result.name.push_back(std::string{*str});                                \
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
  auto [frontmatter, definition] = detail::split_once(input, "\n---\n");
  auto metadata = from_yaml(frontmatter);
  if (not metadata) {
    return metadata.error();
  }
  record* rec = try_as<record>(*metadata);
  if (not rec) {
    return caf::make_error(ec::parse_error,
                           "pipeline frontmatter is not a record");
  }
  rec->emplace("definition", std::string{definition});
  return parse(make_view(*rec));
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
  auto [frontmatter, definition] = detail::split_once(input, "\n---\n");
  auto metadata = from_yaml(frontmatter);
  if (not metadata) {
    return metadata.error();
  }
  record* rec = try_as<record>(*metadata);
  if (not rec) {
    return caf::make_error(ec::parse_error,
                           "pipeline frontmatter is not a record");
  }
  rec->emplace("definition", std::string{definition});
  return parse(make_view(*rec));
}

auto package::parse(const view<record>& data) -> caf::expected<package> {
  auto result = package{};
  // Briefly support both `snippets` and `examples` to enable a smooth transition
  auto legacy_snippets = std::vector<package_example>{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(id);
    TRY_ASSIGN_STRING_TO_RESULT(name);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(author);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(description);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(package_icon);
    TRY_ASSIGN_OPTIONAL_STRING_TO_RESULT(author_icon);
    TRY_ASSIGN_VECTOR_OF_STRING(categories);
    TRY_ASSIGN_MAP_TO_RESULT(inputs, package_input);
    TRY_ASSIGN_MAP_TO_RESULT(pipelines, package_pipeline);
    TRY_ASSIGN_MAP_TO_RESULT(contexts, package_context);
    TRY_ASSIGN_STRUCTURE_TO_RESULT(config, package_config);
    TRY_ASSIGN_LIST_TO_RESULT(examples, package_example);
    TRY_ASSIGN_LIST(snippets, package_example, legacy_snippets);
    TENZIR_WARN("ignoring unknown key `{}` in `package` entry in package "
                "definition",
                key);
  }
  REQUIRED_FIELD(id)
  REQUIRED_FIELD(name)
  if (not legacy_snippets.empty()) {
    if (not result.examples.empty()) {
      return diagnostic::error("found both `snippets` and `examples`")
        .note("the `snippets` key is deprecated, use `examples` instead")
        .to_error();
    }
    result.examples = std::move(legacy_snippets);
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

} // namespace tenzir
