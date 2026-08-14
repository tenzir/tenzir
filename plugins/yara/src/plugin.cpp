//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

#include <tenzir/arc.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/detail/hex_encode.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/hash/sha.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/time.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>
#include <tenzir/uuid.hpp>

extern "C" {
#include <yara_x.h>
}

#include <arrow/record_batch.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tenzir::plugins::yara {

TENZIR_ENUM(yara_format, ocsf, plain);

namespace {

using namespace std::chrono_literals;
using namespace si_literals;

constexpr auto default_timeout = duration{1min};
constexpr auto default_max_input_size = uint64_t{1_Gi};
constexpr auto default_max_matches_per_pattern = uint64_t{1'000};
constexpr auto yara_x_max_stored_matches_per_pattern = uint64_t{1'000'000};
constexpr auto max_stored_matches_per_scan = uint64_t{10'000};
constexpr auto max_encoded_match_bytes_per_scan = uint64_t{16_Mi};

struct YaraArgs {
  Option<located<data>> path;
  Option<located<data>> rules;
  bool fast_scan = false;
  Option<located<duration>> timeout;
  Option<located<uint64_t>> max_input_size;
  Option<located<uint64_t>> max_matches_per_pattern;
  Option<located<data>> include_dirs;
  Option<located<std::string>> format;
  location operator_location = location::unknown;
};

struct YaraSources {
  std::vector<std::string> paths;
  std::vector<std::string> rules;
  std::vector<std::string> include_dirs;
  location source = location::unknown;
};

struct ScanConfig {
  bool fast_scan = false;
  duration timeout = default_timeout;
  uint64_t max_input_size = default_max_input_size;
  uint64_t max_matches_per_pattern = default_max_matches_per_pattern;
  yara_format format = yara_format::ocsf;
};

struct CompilerMessage {
  bool warning = false;
  std::string code;
  std::string title;
  std::string text;
};

using RulesPtr = std::unique_ptr<YRX_RULES, decltype(&yrx_rules_destroy)>;
using CompilerPtr
  = std::unique_ptr<YRX_COMPILER, decltype(&yrx_compiler_destroy)>;

class Rules {
public:
  explicit Rules(RulesPtr rules) : rules_{std::move(rules)} {
    TENZIR_ASSERT(rules_ != nullptr);
  }

  auto get() const -> YRX_RULES const* {
    return rules_.get();
  }

private:
  RulesPtr rules_;
};

struct CompileOutcome {
  Option<Arc<Rules>> rules;
  std::vector<CompilerMessage> messages;
  std::string error;
};

struct PatternMatch {
  std::string pattern;
  size_t offset = 0;
  size_t length = 0;
  bool include_data = true;
};

struct RuleMatch {
  std::string identifier;
  std::string namespace_;
  std::vector<std::string> tags;
  record metadata;
  std::vector<std::string> patterns;
  std::vector<PatternMatch> matches;
  std::vector<std::string> truncated_patterns;
  bool evidence_truncated = false;
};

struct ScanOutcome {
  std::vector<table_slice> slices;
  std::vector<std::string> warnings;
  std::string error;
};

auto last_error() -> std::string {
  auto const* message = yrx_last_error();
  return message ? std::string{message} : std::string{"unknown YARA-X error"};
}

auto to_string_list(located<data> const& value, std::string_view name,
                    bool allow_empty = false)
  -> Result<std::vector<std::string>, diagnostic> {
  if (auto const* str = try_as<std::string>(&value.inner)) {
    return std::vector<std::string>{*str};
  }
  auto const* elements = try_as<list>(&value.inner);
  if (not elements) {
    return Err{
      diagnostic::error("`{}` expected `string` or `list<string>`", name)
        .primary(value.source)
        .done()};
  }
  if (elements->empty() and not allow_empty) {
    return Err{diagnostic::error("`{}` must not be an empty list", name)
                 .primary(value.source)
                 .done()};
  }
  auto result = std::vector<std::string>{};
  result.reserve(elements->size());
  for (auto const& element : *elements) {
    auto const* str = try_as<std::string>(&element);
    if (not str) {
      return Err{
        diagnostic::error("`{}` expected `string` or `list<string>`", name)
          .primary(value.source)
          .done()};
    }
    result.push_back(*str);
  }
  return result;
}

auto normalize_sources(Option<located<data>> const& path,
                       Option<located<data>> const& rules,
                       Option<located<data>> const& include_dirs,
                       location operator_location)
  -> Result<YaraSources, diagnostic> {
  auto const source_count = (path ? 1 : 0) + (rules ? 1 : 0);
  if (source_count != 1) {
    return Err{diagnostic::error("`yara` requires exactly one rule source")
                 .primary(operator_location)
                 .hint("pass `path=` for rule files and directories or "
                       "`rules=` for inline YARA rules")
                 .done()};
  }
  auto result = YaraSources{};
  result.source = operator_location;
  if (path) {
    TRY(result.paths, to_string_list(*path, "path"));
    result.source = path->source;
  }
  if (rules) {
    TRY(result.rules, to_string_list(*rules, "rules"));
    result.source = rules->source;
  }
  if (include_dirs) {
    TRY(result.include_dirs,
        to_string_list(*include_dirs, "include_dirs", true));
  }
  return result;
}

auto normalize_format(Option<located<std::string>> const& format)
  -> Result<yara_format, diagnostic> {
  if (not format) {
    return yara_format::ocsf;
  }
  auto result = from_string<yara_format>(format->inner);
  if (not result) {
    return Err{diagnostic::error("unsupported format")
                 .primary(format->source)
                 .note("available formats: `ocsf`, `plain`")
                 .done()};
  }
  return *result;
}

auto scan_config(YaraArgs const& args, yara_format format) -> ScanConfig {
  return ScanConfig{
    .fast_scan = args.fast_scan,
    .timeout = args.timeout ? args.timeout->inner : default_timeout,
    .max_input_size
    = args.max_input_size ? args.max_input_size->inner : default_max_input_size,
    .max_matches_per_pattern = args.max_matches_per_pattern
                                 ? args.max_matches_per_pattern->inner
                                 : default_max_matches_per_pattern,
    .format = format,
  };
}

auto read_file(std::filesystem::path const& path)
  -> Result<std::string, std::string> {
  auto input = std::ifstream{path, std::ios::binary};
  if (not input) {
    return Err{fmt::format("failed to open rule file '{}'", path.string())};
  }
  auto content = std::string{std::istreambuf_iterator<char>{input},
                             std::istreambuf_iterator<char>{}};
  if (input.bad()) {
    return Err{fmt::format("failed to read rule file '{}'", path.string())};
  }
  return content;
}

auto canonical_key(std::filesystem::path const& path) -> std::string {
  auto ec = std::error_code{};
  auto result = std::filesystem::weakly_canonical(path, ec);
  return ec ? path.lexically_normal().string() : result.string();
}

auto collect_rule_files(std::vector<std::string> const& paths,
                        std::vector<std::string>& warnings)
  -> Result<std::vector<std::filesystem::path>, std::string> {
  auto result = std::vector<std::filesystem::path>{};
  auto seen = std::unordered_set<std::string>{};
  for (auto const& input : paths) {
    auto const root = std::filesystem::path{input};
    auto ec = std::error_code{};
    auto const status = std::filesystem::symlink_status(root, ec);
    if (ec or not std::filesystem::exists(status)) {
      return Err{fmt::format("rule path '{}' does not exist", input)};
    }
    if (std::filesystem::is_symlink(status)) {
      return Err{fmt::format("rule path '{}' is a symbolic link", input)};
    }
    if (std::filesystem::is_regular_file(status)) {
      if (seen.insert(canonical_key(root)).second) {
        result.push_back(root);
      }
      continue;
    }
    if (not std::filesystem::is_directory(status)) {
      return Err{
        fmt::format("rule path '{}' is not a file or directory", input)};
    }
    auto directory_files = std::vector<std::filesystem::path>{};
    auto iterator = std::filesystem::recursive_directory_iterator{
      root, std::filesystem::directory_options::none, ec};
    auto const end = std::filesystem::recursive_directory_iterator{};
    while (not ec and iterator != end) {
      auto const entry_status = iterator->symlink_status(ec);
      if (ec) {
        break;
      }
      if (std::filesystem::is_symlink(entry_status)) {
        return Err{
          fmt::format("rule directory '{}' contains symbolic link '{}'", input,
                      iterator->path().string())};
      }
      auto const& path = iterator->path();
      if (std::filesystem::is_regular_file(entry_status)
          and (path.extension() == ".yar" or path.extension() == ".yara")) {
        directory_files.push_back(path);
      }
      iterator.increment(ec);
    }
    if (ec) {
      return Err{fmt::format("failed to enumerate rule directory '{}': {}",
                             input, ec.message())};
    }
    if (directory_files.empty()) {
      warnings.push_back(fmt::format(
        "rule directory '{}' contains no .yar or .yara files", input));
    }
    for (auto const& file : directory_files) {
      if (seen.insert(canonical_key(file)).second) {
        result.push_back(file);
      }
    }
  }
  std::ranges::sort(result);
  if (result.empty()) {
    return Err{std::string{"no YARA rule files found"}};
  }
  return result;
}

auto add_include_dir(std::filesystem::path const& path,
                     std::unordered_set<std::string>& seen,
                     std::vector<std::string>& result) -> void {
  if (seen.insert(canonical_key(path)).second) {
    result.push_back(path.string());
  }
}

auto compiler_messages(YRX_COMPILER* compiler, bool warnings)
  -> std::vector<CompilerMessage> {
  auto* buffer = static_cast<YRX_BUFFER*>(nullptr);
  auto const status = warnings ? yrx_compiler_warnings_json(compiler, &buffer)
                               : yrx_compiler_errors_json(compiler, &buffer);
  if (status != YRX_SUCCESS or buffer == nullptr) {
    return {};
  }
  auto const owner = std::unique_ptr<YRX_BUFFER, decltype(&yrx_buffer_destroy)>{
    buffer, &yrx_buffer_destroy};
  auto const json = std::string_view{
    reinterpret_cast<char const*>(buffer->data), buffer->length};
  auto parsed = from_json(json);
  if (not parsed or not is<list>(*parsed)) {
    return {CompilerMessage{warnings, {}, {}, std::string{json}}};
  }
  auto result = std::vector<CompilerMessage>{};
  for (auto const& item : as<list>(*parsed)) {
    auto const* fields = try_as<record>(&item);
    if (not fields) {
      continue;
    }
    auto get = [&](std::string_view name) -> std::string {
      auto const entry = fields->find(name);
      if (entry == fields->end()) {
        return {};
      }
      if (auto const* value = try_as<std::string>(&entry->second)) {
        return *value;
      }
      return to_string(entry->second);
    };
    result.push_back(
      CompilerMessage{warnings, get("code"), get("title"), get("text")});
  }
  return result;
}

auto compile_sources(YaraSources const& sources) -> CompileOutcome {
  auto outcome = CompileOutcome{};
  auto files = std::vector<std::filesystem::path>{};
  if (not sources.paths.empty()) {
    auto warnings = std::vector<std::string>{};
    auto collected = collect_rule_files(sources.paths, warnings);
    if (collected.is_err()) {
      outcome.error = std::move(collected).unwrap_err();
      return outcome;
    }
    files = std::move(collected).unwrap();
    for (auto& warning : warnings) {
      outcome.messages.push_back(
        CompilerMessage{true, {}, std::move(warning), {}});
    }
  }
  auto include_dirs = std::vector<std::string>{};
  auto seen_dirs = std::unordered_set<std::string>{};
  for (auto const& file : files) {
    add_include_dir(file.parent_path(), seen_dirs, include_dirs);
  }
  for (auto const& directory : sources.include_dirs) {
    add_include_dir(directory, seen_dirs, include_dirs);
  }
  auto* raw_compiler = static_cast<YRX_COMPILER*>(nullptr);
  auto status
    = yrx_compiler_create(YRX_ENABLE_CONDITION_OPTIMIZATION, &raw_compiler);
  if (status != YRX_SUCCESS) {
    outcome.error
      = fmt::format("failed to create YARA-X compiler: {}", last_error());
    return outcome;
  }
  auto compiler = CompilerPtr{raw_compiler, &yrx_compiler_destroy};
  for (auto const* module : {"cuckoo", "vt"}) {
    status = yrx_compiler_ban_module(
      compiler.get(), module, "unsupported YARA-X module",
      "Tenzir does not provide the runtime data required by this module");
    if (status != YRX_SUCCESS) {
      outcome.error = fmt::format("failed to ban YARA-X module '{}': {}",
                                  module, last_error());
      return outcome;
    }
  }
  for (auto const& include_dir : include_dirs) {
    status = yrx_compiler_add_include_dir(compiler.get(), include_dir.c_str());
    if (status != YRX_SUCCESS) {
      outcome.error = fmt::format("failed to add YARA include directory '{}': "
                                  "{}",
                                  include_dir, last_error());
      return outcome;
    }
  }
  auto add_failed = false;
  if (not files.empty()) {
    for (auto const& file : files) {
      auto content = read_file(file);
      if (content.is_err()) {
        outcome.error = std::move(content).unwrap_err();
        return outcome;
      }
      auto const origin = file.string();
      status = yrx_compiler_add_source_with_origin(
        compiler.get(), content.unwrap().c_str(), origin.c_str());
      add_failed = status != YRX_SUCCESS or add_failed;
    }
  } else {
    for (auto index = size_t{0}; index < sources.rules.size(); ++index) {
      auto const origin = fmt::format("<rules[{}]>", index);
      status = yrx_compiler_add_source_with_origin(
        compiler.get(), sources.rules[index].c_str(), origin.c_str());
      add_failed = status != YRX_SUCCESS or add_failed;
    }
  }
  auto errors = compiler_messages(compiler.get(), false);
  auto compiler_warnings = compiler_messages(compiler.get(), true);
  outcome.messages.insert(outcome.messages.end(),
                          std::make_move_iterator(errors.begin()),
                          std::make_move_iterator(errors.end()));
  outcome.messages.insert(outcome.messages.end(),
                          std::make_move_iterator(compiler_warnings.begin()),
                          std::make_move_iterator(compiler_warnings.end()));
  if (add_failed or not errors.empty()) {
    outcome.error = "failed to compile YARA rules";
    return outcome;
  }
  auto* built = yrx_compiler_build(compiler.get());
  if (built == nullptr) {
    outcome.error = fmt::format("failed to build YARA rules: {}", last_error());
    return outcome;
  }
  auto rules = RulesPtr{built, &yrx_rules_destroy};
  outcome.rules = Arc<Rules>{Rules{std::move(rules)}};
  return outcome;
}

auto emit_compiler_messages(std::vector<CompilerMessage> const& messages,
                            location source, diagnostic_handler& dh) -> void {
  for (auto const& message : messages) {
    auto const title = message.title.empty()
                         ? (message.warning ? "YARA-X compiler warning"
                                            : "YARA-X compiler error")
                         : message.title;
    auto builder = message.warning ? diagnostic::warning("{}", title)
                                   : diagnostic::error("{}", title);
    if (source != location::unknown) {
      builder = std::move(builder).primary(source.subloc(0, 1));
    }
    if (not message.code.empty()) {
      builder = std::move(builder).note("diagnostic code: {}", message.code);
    }
    if (not message.text.empty()) {
      builder = std::move(builder).note("{}", message.text);
    }
    std::move(builder).emit(dh);
  }
}

auto metadata_value(YRX_METADATA const& metadata) -> data {
  switch (metadata.value_type) {
    case YRX_I64:
      return metadata.value.i64;
    case YRX_F64:
      return metadata.value.f64;
    case YRX_BOOLEAN:
      return metadata.value.boolean;
    case YRX_STRING:
      return std::string{metadata.value.string};
    case YRX_BYTES:
      return record{{"encoding", "base64"},
                    {"value", detail::base64::encode(std::span<const std::byte>{
                                reinterpret_cast<std::byte const*>(
                                  metadata.value.bytes.data),
                                metadata.value.bytes.length})}};
  }
  TENZIR_UNREACHABLE();
}

struct EvidenceBudget {
  uint64_t remaining_matches = max_stored_matches_per_scan;
  uint64_t remaining_encoded_bytes = max_encoded_match_bytes_per_scan;
  bool matches_exhausted = false;
  bool encoded_bytes_exhausted = false;
};

struct PatternContext {
  RuleMatch* rule = nullptr;
  EvidenceBudget* budget = nullptr;
  uint64_t max_matches_per_pattern = 0;
  bool collect_matches = true;
  bool failed = false;
};

struct MatchContext {
  std::string pattern;
  std::vector<PatternMatch>* matches = nullptr;
  EvidenceBudget* budget = nullptr;
  uint64_t max_matches = 0;
  bool truncated = false;
  bool evidence_truncated = false;
};

auto match_callback(YRX_MATCH const* match, void* user_data) -> void {
  auto& context = *static_cast<MatchContext*>(user_data);
  if (context.matches->size() >= context.max_matches) {
    context.truncated = true;
    return;
  }
  if (context.budget->remaining_matches == 0) {
    context.budget->matches_exhausted = true;
    context.evidence_truncated = true;
    return;
  }
  auto const raw_size = detail::narrow<uint64_t>(match->length);
  auto encoded_size = uint64_t{0};
  if (raw_size <= context.budget->remaining_encoded_bytes) {
    encoded_size = uint64_t{4} * ((raw_size + 2) / 3);
  }
  auto include_data = true;
  if (raw_size > context.budget->remaining_encoded_bytes
      or encoded_size > context.budget->remaining_encoded_bytes) {
    context.budget->remaining_encoded_bytes = 0;
    context.budget->encoded_bytes_exhausted = true;
    context.evidence_truncated = true;
    include_data = false;
  } else {
    context.budget->remaining_encoded_bytes -= encoded_size;
  }
  context.matches->push_back(
    PatternMatch{context.pattern, match->offset, match->length, include_data});
  --context.budget->remaining_matches;
}

auto pattern_callback(YRX_PATTERN const* pattern, void* user_data) -> void {
  auto& context = *static_cast<PatternContext*>(user_data);
  auto const* identifier = static_cast<uint8_t const*>(nullptr);
  auto length = size_t{0};
  if (yrx_pattern_identifier(pattern, &identifier, &length) != YRX_SUCCESS) {
    context.failed = true;
    return;
  }
  auto name = std::string{reinterpret_cast<char const*>(identifier), length};
  context.rule->patterns.push_back(name);
  if (not context.collect_matches) {
    return;
  }
  auto matches = std::vector<PatternMatch>{};
  auto match_context = MatchContext{name, &matches, context.budget,
                                    context.max_matches_per_pattern};
  if (yrx_pattern_iter_matches(pattern, match_callback, &match_context)
      != YRX_SUCCESS) {
    context.failed = true;
    return;
  }
  if (match_context.truncated) {
    context.rule->truncated_patterns.push_back(name);
  }
  context.rule->evidence_truncated
    = context.rule->evidence_truncated or match_context.evidence_truncated;
  context.rule->matches.insert(context.rule->matches.end(),
                               std::make_move_iterator(matches.begin()),
                               std::make_move_iterator(matches.end()));
}

auto tag_callback(char const* tag, void* user_data) -> void {
  static_cast<RuleMatch*>(user_data)->tags.emplace_back(tag);
}

auto metadata_callback(YRX_METADATA const* metadata, void* user_data) -> void {
  static_cast<RuleMatch*>(user_data)->metadata.emplace(
    metadata->identifier, metadata_value(*metadata));
}

struct RuleContext {
  std::vector<RuleMatch>* rules = nullptr;
  uint64_t max_matches_per_pattern = 0;
  bool collect_matches = true;
  bool failed = false;
  EvidenceBudget budget;
};

auto string_from_rule(YRX_RULE const* rule, bool namespace_value)
  -> Option<std::string> {
  auto const* data = static_cast<uint8_t const*>(nullptr);
  auto length = size_t{0};
  auto const status = namespace_value
                        ? yrx_rule_namespace(rule, &data, &length)
                        : yrx_rule_identifier(rule, &data, &length);
  if (status != YRX_SUCCESS) {
    return None{};
  }
  return std::string{reinterpret_cast<char const*>(data), length};
}

auto rule_callback(YRX_RULE const* rule, void* user_data) -> void {
  auto& context = *static_cast<RuleContext*>(user_data);
  auto identifier = string_from_rule(rule, false);
  auto namespace_ = string_from_rule(rule, true);
  if (not identifier or not namespace_) {
    context.failed = true;
    return;
  }
  auto result = RuleMatch{};
  result.identifier = std::move(*identifier);
  result.namespace_ = std::move(*namespace_);
  if (yrx_rule_iter_tags(rule, tag_callback, &result) != YRX_SUCCESS
      or yrx_rule_iter_metadata(rule, metadata_callback, &result)
           != YRX_SUCCESS) {
    context.failed = true;
    return;
  }
  auto pattern_context
    = PatternContext{&result, &context.budget, context.max_matches_per_pattern,
                     context.collect_matches};
  if (yrx_rule_iter_patterns(rule, pattern_callback, &pattern_context)
        != YRX_SUCCESS
      or pattern_context.failed) {
    context.failed = true;
    return;
  }
  std::ranges::sort(result.patterns);
  std::ranges::sort(result.matches, {}, [](PatternMatch const& match) {
    return std::tuple{match.offset, match.pattern, match.length};
  });
  context.rules->push_back(std::move(result));
}

auto console_callback(char const* message) -> void {
  TENZIR_DEBUG("YARA-X: {}", message);
}

auto sha256_hex(std::span<const std::byte> bytes) -> std::string {
  auto hash = sha256{};
  hash.add(bytes);
  return detail::hexify(hash.finish());
}

auto make_rule_identity(RuleMatch const& rule) -> std::string {
  return fmt::format("yara:{}:{}", rule.namespace_, rule.identifier);
}

auto data_list(std::vector<std::string> const& values) -> list {
  auto result = list{};
  result.reserve(values.size());
  for (auto const& value : values) {
    result.emplace_back(value);
  }
  return result;
}

auto make_rule_descriptor(RuleMatch const& rule) -> record {
  return record{
    {"identifier", rule.identifier},        {"namespace", rule.namespace_},
    {"tags", data_list(rule.tags)},         {"meta", rule.metadata},
    {"patterns", data_list(rule.patterns)},
  };
}

auto build_plain_matches(std::vector<RuleMatch> const& matches,
                         std::span<const std::byte> input)
  -> std::vector<table_slice> {
  if (matches.empty()) {
    return {};
  }
  auto input_series
    = data_to_series(blob_view{input.data(), input.size()}, int64_t{1});
  auto result = std::vector<table_slice>{};
  result.reserve(matches.size());
  for (auto const& rule : matches) {
    auto rule_series = data_to_series(make_rule_descriptor(rule), 1);
    auto result_schema = type{
      "tenzir.yara",
      record_type{
        {"input", input_series.type},
        {"rule", rule_series.type},
      },
    };
    auto batch = arrow::RecordBatch::Make(result_schema.to_arrow_schema(), 1,
                                          {input_series.array,
                                           std::move(rule_series.array)});
    result.emplace_back(std::move(batch), std::move(result_schema));
  }
  return result;
}

auto build_findings(std::vector<RuleMatch> const& matches,
                    std::span<const std::byte> input, bool fast_scan)
  -> std::vector<table_slice> {
  if (matches.empty()) {
    return {};
  }
  auto const input_digest = sha256_hex(input);
  auto const now = time::clock::now();
  auto builder = series_builder{};
  for (auto const& rule : matches) {
    auto const analytic_uid = make_rule_identity(rule);
    auto const finding_uid = fmt::format("{}", uuid::random());
    auto description = Option<std::string>{};
    if (auto entry = rule.metadata.find("description");
        entry != rule.metadata.end()) {
      if (auto const* value = try_as<std::string>(&entry->second)) {
        description = *value;
      }
    }
    auto policy_data = make_rule_descriptor(rule);
    auto finding = builder.record();
    finding.field("time").data(now);
    finding.field("class_uid").data(int64_t{2004});
    finding.field("category_uid").data(int64_t{2});
    finding.field("activity_id").data(int64_t{1});
    finding.field("type_uid").data(int64_t{200401});
    finding.field("status_id").data(int64_t{1});
    finding.field("severity_id").data(int64_t{0});
    finding.field("confidence_id").data(int64_t{0});
    finding.field("action_id").data(int64_t{3});
    finding.field("disposition_id").data(int64_t{15});
    auto metadata = finding.field("metadata").record();
    metadata.field("uid").data(fmt::format("{}", uuid::random()));
    metadata.field("version").data("1.9.0");
    auto product = metadata.field("product").record();
    product.field("name").data("Tenzir");
    product.field("vendor_name").data("Tenzir");
    metadata.field("profiles").list().data("security_control");
    if (not rule.tags.empty()) {
      metadata.field("labels").data(data_list(rule.tags));
    }
    auto info = finding.field("finding_info").record();
    info.field("uid").data(finding_uid);
    info.field("title").data(fmt::format("YARA match: {}", rule.identifier));
    if (description) {
      info.field("desc").data(*description);
    }
    info.field("created_time").data(now);
    auto analytic = info.field("analytic").record();
    analytic.field("uid").data(analytic_uid);
    analytic.field("name").data(rule.identifier);
    analytic.field("type_id").data(int64_t{1});
    analytic.field("type").data("Rule");
    auto policy = finding.field("policy").record();
    policy.field("uid").data(analytic_uid);
    policy.field("name").data(rule.identifier);
    policy.field("type").data("YARA rule");
    policy.field("is_applied").data(true);
    policy.field("data").data(policy_data);
    auto evidences = finding.field("evidences").list();
    auto evidence = evidences.record();
    evidence.field("uid").data(fmt::format("sha256:{}", input_digest));
    evidence.field("name").data("Scanned byte stream");
    auto evidence_data = evidence.field("data").record();
    auto input_data = evidence_data.field("input").record();
    input_data.field("size").data(detail::narrow<uint64_t>(input.size()));
    input_data.field("sha256").data(input_digest);
    evidence_data.field("matches_complete")
      .data(not fast_scan and rule.truncated_patterns.empty()
            and not rule.evidence_truncated);
    auto evidence_matches = evidence_data.field("matches").list();
    for (auto const& match : rule.matches) {
      auto item = evidence_matches.record();
      item.field("pattern").data(match.pattern);
      item.field("offset").data(detail::narrow<uint64_t>(match.offset));
      item.field("length").data(detail::narrow<uint64_t>(match.length));
      if (match.include_data) {
        auto encoded = item.field("data").record();
        encoded.field("encoding").data("base64");
        encoded.field("value").data(
          detail::base64::encode(input.subspan(match.offset, match.length)));
      }
    }
  }
  return builder.finish_as_table_slice("ocsf.detection_finding");
}

auto scan(Rules const& rules, std::span<const std::byte> input,
          ScanConfig const& config) -> ScanOutcome {
  auto outcome = ScanOutcome{};
  auto* scanner = static_cast<YRX_SCANNER*>(nullptr);
  auto status = yrx_scanner_create(rules.get(), &scanner);
  if (status != YRX_SUCCESS) {
    outcome.error
      = fmt::format("failed to create YARA-X scanner: {}", last_error());
    return outcome;
  }
  auto const scanner_owner
    = std::unique_ptr<YRX_SCANNER, decltype(&yrx_scanner_destroy)>{
      scanner, &yrx_scanner_destroy};
  auto const timeout
    = std::chrono::duration_cast<std::chrono::seconds>(config.timeout);
  status = yrx_scanner_set_timeout(scanner,
                                   detail::narrow<uint64_t>(timeout.count()));
  if (status == YRX_SUCCESS) {
    status = yrx_scanner_fast_scan(scanner, config.fast_scan);
  }
  auto matches = std::vector<RuleMatch>{};
  auto context
    = RuleContext{&matches, config.max_matches_per_pattern,
                  config.format == yara_format::ocsf, false, EvidenceBudget{}};
  if (status == YRX_SUCCESS) {
    status = yrx_scanner_on_matching_rule(scanner, rule_callback, &context);
  }
  if (status == YRX_SUCCESS) {
    status = yrx_scanner_on_console_log(scanner, console_callback);
  }
  if (status == YRX_SUCCESS) {
    status = yrx_scanner_scan(
      scanner, reinterpret_cast<uint8_t const*>(input.data()), input.size());
  }
  if (status == YRX_SCAN_TIMEOUT) {
    outcome.error
      = fmt::format("YARA-X scan exceeded the timeout of {}", config.timeout);
    return outcome;
  }
  if (status != YRX_SUCCESS) {
    outcome.error = fmt::format("YARA-X scan failed: {}", last_error());
    return outcome;
  }
  if (context.failed) {
    outcome.error = "YARA-X returned invalid rule or match metadata";
    return outcome;
  }
  for (auto const& rule : matches) {
    for (auto const& match : rule.matches) {
      if (match.offset > input.size()
          or match.length > input.size() - match.offset) {
        outcome.error
          = fmt::format("YARA-X returned an invalid match range for rule '{}' "
                        "and pattern '{}'",
                        rule.identifier, match.pattern);
        return outcome;
      }
    }
    for (auto const& pattern : rule.truncated_patterns) {
      outcome.warnings.push_back(fmt::format(
        "YARA-X match evidence is truncated for rule '{}' and pattern '{}' "
        "because more than `max_matches_per_pattern={}` matches were found",
        rule.identifier, pattern, config.max_matches_per_pattern));
    }
  }
  if (context.budget.matches_exhausted) {
    outcome.warnings.push_back(fmt::format("YARA-X match evidence is truncated "
                                           "because more than {} matches were "
                                           "found across the scanned input",
                                           max_stored_matches_per_scan));
  }
  if (context.budget.encoded_bytes_exhausted) {
    outcome.warnings.push_back(fmt::format("YARA-X match data is omitted after "
                                           "its Base64 encoding exceeds {} "
                                           "bytes across the scanned input",
                                           max_encoded_match_bytes_per_scan));
  }
  std::ranges::sort(matches, {}, [](RuleMatch const& match) {
    return std::tuple{match.namespace_, match.identifier};
  });
  switch (config.format) {
    case yara_format::ocsf:
      outcome.slices = build_findings(matches, input, config.fast_scan);
      break;
    case yara_format::plain:
      outcome.slices = build_plain_matches(matches, input);
      break;
  }
  return outcome;
}

class Yara final : public Operator<chunk_ptr, table_slice> {
public:
  explicit Yara(YaraArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto normalized_sources = normalize_sources(
      args_.path, args_.rules, args_.include_dirs, args_.operator_location);
    TENZIR_ASSERT(normalized_sources.is_ok());
    auto sources = std::move(normalized_sources).unwrap();
    auto const source = sources.source;
    auto format = normalize_format(args_.format);
    TENZIR_ASSERT(format.is_ok());
    config_ = scan_config(args_, std::move(format).unwrap());
    auto outcome = co_await spawn_blocking([sources = std::move(sources)]() {
      return compile_sources(sources);
    });
    auto const has_compiler_error = std::ranges::any_of(
      outcome.messages, [](CompilerMessage const& message) {
        return not message.warning;
      });
    emit_compiler_messages(outcome.messages, source, ctx.dh());
    if (not outcome.rules) {
      if (not has_compiler_error) {
        diagnostic::error("failed to compile YARA rules")
          .primary(source.subloc(0, 1))
          .note("{}", outcome.error)
          .emit(ctx);
      }
      failed_ = true;
      co_return;
    }
    rules_ = std::move(*outcome.rules);
  }

  auto state() -> OperatorState override {
    return failed_ ? OperatorState::done : OperatorState::normal;
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    if (failed_ or not input or input->size() == 0) {
      co_return;
    }
    if (input->size() > config_.max_input_size - input_size_) {
      diagnostic::error("YARA input exceeds `max_input_size={}`",
                        config_.max_input_size)
        .primary(args_.operator_location.subloc(0, 4))
        .note("the operator buffers one finite byte stream before scanning")
        .emit(ctx);
      failed_ = true;
      first_chunk_ = {};
      buffer_.clear();
      input_size_ = 0;
      co_return;
    }
    input_size_ += input->size();
    if (not buffer_.empty()) {
      buffer_.insert(buffer_.end(), input->begin(), input->end());
    } else if (not first_chunk_) {
      first_chunk_ = std::move(input);
    } else {
      buffer_.reserve(first_chunk_->size() + input->size());
      buffer_.insert(buffer_.end(), first_chunk_->begin(), first_chunk_->end());
      buffer_.insert(buffer_.end(), input->begin(), input->end());
      first_chunk_ = {};
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (failed_ or not rules_) {
      co_return FinalizeBehavior::done;
    }
    auto rules = *rules_;
    auto first_chunk = std::exchange(first_chunk_, {});
    auto buffer = std::exchange(buffer_, std::vector<std::byte>{});
    input_size_ = 0;
    auto outcome = co_await spawn_blocking(
      [rules, first_chunk = std::move(first_chunk), buffer = std::move(buffer),
       config = config_]() mutable {
        auto bytes = std::span<const std::byte>{};
        if (not buffer.empty()) {
          bytes = as_bytes(buffer);
        } else if (first_chunk) {
          bytes = as_bytes(first_chunk);
        }
        return scan(*rules, bytes, config);
      });
    if (not outcome.error.empty()) {
      diagnostic::error("failed to scan input with YARA-X")
        .primary(args_.operator_location.subloc(0, 4))
        .note("{}", outcome.error)
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    for (auto const& warning : outcome.warnings) {
      diagnostic::warning("YARA-X match evidence is incomplete")
        .primary(args_.operator_location.subloc(0, 4))
        .note("{}", warning)
        .emit(ctx);
    }
    for (auto& slice : outcome.slices) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& serde) -> void override {
    // Buffered input is intentionally part of the snapshot. Its maximum size
    // is bounded by `max_input_size`.
    serde("failed", failed_);
    serde("first_chunk", first_chunk_);
    serde("buffer", buffer_);
    serde("input_size", input_size_);
  }

private:
  YaraArgs args_;
  ScanConfig config_;
  bool failed_ = false;
  chunk_ptr first_chunk_;
  std::vector<std::byte> buffer_;
  uint64_t input_size_ = 0;
  Option<Arc<Rules>> rules_;
};

// The plugin stays loaded for the process lifetime. Calling `yrx_finalize`
// during ordinary shutdown is unsafe because it tears down process-wide
// Wasmtime signal state.
class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "yara";
  }

  auto describe() const -> Description override {
    auto d = Describer<YaraArgs, Yara>{};
    auto path = d.named("path", &YaraArgs::path);
    auto rules = d.named("rules", &YaraArgs::rules);
    d.named("fast_scan", &YaraArgs::fast_scan);
    auto timeout = d.named("timeout", &YaraArgs::timeout);
    auto max_input_size = d.named("max_input_size", &YaraArgs::max_input_size);
    auto max_matches_per_pattern
      = d.named("max_matches_per_pattern", &YaraArgs::max_matches_per_pattern);
    auto include_dirs = d.named("include_dirs", &YaraArgs::include_dirs);
    auto format = d.named("format", &YaraArgs::format, "ocsf|plain");
    d.operator_location(&YaraArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto to_option = [](auto const& value) {
        using Value = std::remove_cvref_t<decltype(*value)>;
        return value ? Option<Value>{*value} : Option<Value>{};
      };
      auto sources
        = normalize_sources(to_option(ctx.get(path)), to_option(ctx.get(rules)),
                            to_option(ctx.get(include_dirs)),
                            ctx.operator_location());
      if (sources.is_err()) {
        static_cast<diagnostic_handler&>(ctx).emit(
          std::move(sources).unwrap_err());
        return {};
      }
      if (auto normalized = normalize_format(to_option(ctx.get(format)));
          normalized.is_err()) {
        static_cast<diagnostic_handler&>(ctx).emit(
          std::move(normalized).unwrap_err());
      }
      if (auto value = ctx.get(timeout)) {
        if (value->inner <= duration::zero()) {
          diagnostic::error("`timeout` must be a positive duration")
            .primary(value->source)
            .emit(ctx);
        } else if (value->inner
                   != std::chrono::duration_cast<std::chrono::seconds>(
                     value->inner)) {
          diagnostic::error("`timeout` must use whole-second precision")
            .primary(value->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(max_input_size); value and value->inner == 0) {
        diagnostic::error("`max_input_size` must be greater than zero")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(max_matches_per_pattern); value) {
        if (value->inner == 0) {
          diagnostic::error(
            "`max_matches_per_pattern` must be greater than zero")
            .primary(value->source)
            .emit(ctx);
        } else if (value->inner >= yara_x_max_stored_matches_per_pattern) {
          diagnostic::error("`max_matches_per_pattern` must be less than {}",
                            yara_x_max_stored_matches_per_pattern)
            .primary(value->source)
            .note("YARA-X cannot report whether its internal match storage "
                  "limit was reached")
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::yara

TENZIR_REGISTER_PLUGIN(tenzir::plugins::yara::plugin)
