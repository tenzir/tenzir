//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors

#include "tenzir/package.hpp"

#include <tenzir/type.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

#define TRY_ASSIGN_STRING_TO_RESULT2(name, field)                              \
  if (key == #name) {                                                          \
    const auto* id = caf::get_if<std::string_view>(&value);                    \
    if (not id) {                                                              \
      return diagnostic::error(#name " must be a string")                      \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    result.field = std::string{*id};                                           \
    continue;                                                                  \
  }

#define TRY_ASSIGN_STRING_TO_RESULT(name)                                      \
  TRY_ASSIGN_STRING_TO_RESULT2(name, name)

#define TRY_ASSIGN_MAP_TO_RESULT(name, value_type)                             \
  if (key == #name) {                                                          \
    const auto* x = caf::get_if<view<record>>(&value);                         \
    if (not x) {                                                               \
      return diagnostic::error(#name " must be a record")                      \
        .note("got {}", value)                                                 \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    for (auto const& [key, value] : *x) {                                      \
      auto const* value_record = caf::get_if<view<record>>(&value);            \
      if (not value_record) {                                                  \
        return diagnostic::error(#name " values must be records")              \
          .note("while parsing key {} for field" #name, key)                   \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      auto parsed_value = value_type::parse(*value_record);                    \
      if (not parsed_value) {                                                  \
        return diagnostic::error(parsed_value.error())                         \
          .note("while parsing key {} for field" #name, key)                   \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      result.name[std::string{key}] = *parsed_value;                           \
    }                                                                          \
    continue;                                                                  \
  }

#define TRY_ASSIGN_STRINGMAP_TO_RESULT(name)                                   \
  if (key == #name) {                                                          \
    const auto* x = caf::get_if<view<record>>(&value);                         \
    if (not x) {                                                               \
      return diagnostic::error(#name " must be a record")                      \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    for (auto const& [key, value] : *x) {                                      \
      auto const* value_string = caf::get_if<std::string_view>(&value);        \
      if (not value_string) {                                                  \
        return diagnostic::error(#name " values must be strings")              \
          .note("while parsing key {} for field" #name, key)                   \
          .note("invalid package definition")                                  \
          .to_error();                                                         \
      }                                                                        \
      result.name[std::string{key}] = std::string{*value_string};              \
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
    const auto* x = caf::get_if<view<record>>(&value);                         \
    if (not x) {                                                               \
      return diagnostic::error(#name " values must be records")                \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    auto parsed = type::parse(*x);                                             \
    if (not parsed) {                                                          \
      return diagnostic::error(parsed.error())                                 \
        .note("while parsing key {} for field" #name, key)                     \
        .note("invalid package definition")                                    \
        .to_error();                                                           \
    }                                                                          \
    result.name = *parsed;                                                     \
    continue;                                                                  \
  }

#define TRY_ASSIGN_LIST_TO_RESULT(name, inner_type)                            \
  if (key == #name) {                                                          \
    const auto* item_list = caf::get_if<view<list>>(&value);                   \
    if (not item_list) {                                                       \
      return diagnostic::error(#name " must be a list")                        \
        .note("got a {} instead", type::infer(materialize(value)))             \
        .to_error();                                                           \
    }                                                                          \
    size_t pos = 0;                                                            \
    for (auto item_view : *item_list) {                                        \
      const auto* item_record = caf::get_if<view<record>>(&item_view);         \
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
      result.name.push_back(*item);                                            \
      ++pos;                                                                   \
    }                                                                          \
    continue;                                                                  \
  }

auto package_input::parse(const view<record>& data)
  -> caf::expected<package_input> {
  auto result = package_input{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(name)
    TRY_ASSIGN_STRING_TO_RESULT(description)
    TRY_ASSIGN_STRING_TO_RESULT(type)
    TRY_ASSIGN_STRING_TO_RESULT2(default, default_);
    return diagnostic::error("unknown key '{}'", key)
      .note("while trying to parse 'input'")
      .note("invalid package source definition")
      .to_error();
  }
  REQUIRED_FIELD(name);
  REQUIRED_FIELD(type);
  return result;
}

auto package_source::parse(const view<record>& data)
  -> caf::expected<package_source> {
  auto result = package_source{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(repository)
    TRY_ASSIGN_STRING_TO_RESULT(directory)
    TRY_ASSIGN_STRING_TO_RESULT(revision)
    return diagnostic::error("unknown key '{}'", key)
      .note("while trying to parse 'source' entry")
      .note("invalid package source definition")
      .to_error();
  }
  REQUIRED_FIELD(repository)
  REQUIRED_FIELD(directory)
  REQUIRED_FIELD(revision)
  return result;
}

auto package_deployment::parse(const view<record>& data)
  -> caf::expected<package_deployment> {
  auto result = package_deployment{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRUCTURE_TO_RESULT(source, package_source);
    TRY_ASSIGN_STRINGMAP_TO_RESULT(inputs);
  }
  return result;
}

auto package_pipeline::parse(const view<record>& data)
  -> caf::expected<package_pipeline> {
  auto result = package_pipeline{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(name)
    TRY_ASSIGN_STRING_TO_RESULT(description)
    TRY_ASSIGN_STRING_TO_RESULT(definition)
    if (key == "disabled") {
      const auto* disabled = caf::get_if<view<bool>>(&value);
      if (not disabled) {
        return diagnostic::error("'disabled' must be a bool")
          .note("invalid package definition")
          .to_error();
      }
      result.disabled = *disabled;
      continue;
    }
    return diagnostic::error("unknown key '{}'", key)
      .note("while trying to parse 'pipeline' entry")
      .note("invalid package source definition")
      .to_error();
  }
  REQUIRED_FIELD(definition)
  return result;
}

auto package_context::parse(const view<record>& data)
  -> caf::expected<package_context> {
  auto result = package_context{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(type);
    TRY_ASSIGN_STRING_TO_RESULT(description);
    TRY_ASSIGN_STRINGMAP_TO_RESULT(arguments);
  }
  REQUIRED_FIELD(type)
  return result;
}

auto package_snippet::parse(const view<record>& data)
  -> caf::expected<package_snippet> {
  auto result = package_snippet{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(name);
    TRY_ASSIGN_STRING_TO_RESULT(description);
    TRY_ASSIGN_STRING_TO_RESULT(definition);
  }
  REQUIRED_FIELD(definition)
  return result;
}

auto package::parse(const view<record>& data) -> caf::expected<package> {
  auto result = package{};
  for (const auto& [key, value] : data) {
    TRY_ASSIGN_STRING_TO_RESULT(id);
    TRY_ASSIGN_STRING_TO_RESULT(name);
    TRY_ASSIGN_STRING_TO_RESULT(author);
    TRY_ASSIGN_STRING_TO_RESULT(description);
    TRY_ASSIGN_MAP_TO_RESULT(inputs, package_input);
    TRY_ASSIGN_MAP_TO_RESULT(pipelines, package_pipeline);
    TRY_ASSIGN_MAP_TO_RESULT(contexts, package_context);
    TRY_ASSIGN_STRUCTURE_TO_RESULT(deployment, package_deployment);
    TRY_ASSIGN_LIST_TO_RESULT(snippets, package_snippet);
    // Reject unknown keys in the package definition.
    return diagnostic::error("unknown key '{}'", key)
      .note("while trying to parse 'package' entry")
      .note("invalid package definition")
      .to_error();
  }
  // Post-parsing checks.
  REQUIRED_FIELD(id)
  REQUIRED_FIELD(name)
  return result;
}

} // namespace tenzir
