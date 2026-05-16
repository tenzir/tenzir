//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <ranges>
#include <string_view>

namespace tenzir::plugins::read_auto {

namespace {

using detection_state = read_detection_result::result_state;

struct ReadAutoArgs {
  std::string fallback = "none";
  uint64_t max_probe_bytes = 1024 * 1024;
  location operator_location;
};

enum class DetectorKind {
  json_suricata,
  json_zeek,
  json_gelf,
  json_ndjson,
  json_array,
  json_object,
  cef,
  leef,
  zeek_tsv,
  syslog,
  csv,
  tsv,
  ssv,
  kv,
  yaml,
  pcap,
  feather,
  bitz,
  parquet,
};

struct Candidate {
  std::string id;
  std::string pipeline;
  int64_t priority = 0;
  std::optional<DetectorKind> detector;
  std::optional<size_t> plugin_candidate;
  bool live = true;
  std::optional<read_detection_result> last;
};

auto reject(std::string reason = {}) -> read_detection_result {
  return {.state = detection_state::reject, .reason = std::move(reason)};
}

auto need_more(std::string reason = {}) -> read_detection_result {
  return {.state = detection_state::need_more, .reason = std::move(reason)};
}

auto match(uint64_t confidence, std::string reason = {})
  -> read_detection_result {
  return {
    .state = detection_state::match,
    .confidence = confidence,
    .reason = std::move(reason),
  };
}

struct json_scan_result {
  enum class kind {
    incomplete,
    invalid,
    complete,
  };

  kind state = kind::incomplete;
  char top_level = '\0';
  size_t end = 0;
  char first_array_element = '\0';
};

auto scan_json_value(std::string_view input) -> json_scan_result {
  auto bytes = detail::trim_front(input);
  if (bytes.empty()) {
    return {};
  }
  auto stack = std::vector<char>{};
  auto in_string = false;
  auto escaped = false;
  auto top = bytes.front();
  auto first_array_element = char{'\0'};
  if (top != '{' and top != '[') {
    return {.state = json_scan_result::kind::invalid};
  }
  for (auto i = size_t{0}; i < bytes.size(); ++i) {
    auto ch = bytes[i];
    if (in_string) {
      if (escaped) {
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else if (ch == '"') {
        in_string = false;
      }
      continue;
    }
    if (ch == '"') {
      in_string = true;
      continue;
    }
    if (top == '[' and stack.size() == 1 and first_array_element == '\0'
        and not detail::ascii_whitespace.contains(ch) and ch != '[') {
      if (ch != ',' and ch != ']') {
        first_array_element = ch;
      }
    }
    if (ch == '{' or ch == '[') {
      stack.push_back(ch == '{' ? '}' : ']');
      continue;
    }
    if (ch == '}' or ch == ']') {
      if (stack.empty() or stack.back() != ch) {
        return {.state = json_scan_result::kind::invalid};
      }
      stack.pop_back();
      if (stack.empty()) {
        return {
          .state = json_scan_result::kind::complete,
          .top_level = top,
          .end = input.size() - bytes.size() + i + 1,
          .first_array_element = first_array_element,
        };
      }
    }
  }
  return {};
}

auto detect_json_object(read_detection_input input) -> read_detection_result {
  auto scan = scan_json_value(input.bytes);
  if (scan.state == json_scan_result::kind::invalid) {
    return reject();
  }
  if (scan.state == json_scan_result::kind::incomplete) {
    return input.eof ? reject("incomplete JSON object") : need_more();
  }
  if (scan.top_level != '{') {
    return reject();
  }
  auto rest = detail::trim_front(input.bytes.substr(scan.end));
  if (not rest.empty()) {
    return reject("trailing bytes after object");
  }
  return match(70, "top-level JSON object");
}

auto detect_json_array(read_detection_input input) -> read_detection_result {
  auto scan = scan_json_value(input.bytes);
  if (scan.state == json_scan_result::kind::invalid) {
    return reject();
  }
  if (scan.state == json_scan_result::kind::incomplete) {
    return input.eof ? reject("incomplete JSON array") : need_more();
  }
  if (scan.top_level == '[' and scan.first_array_element == '{') {
    return match(75, "top-level array of objects");
  }
  return reject();
}

auto complete_json_object_line(std::string_view line) -> bool {
  auto scan = scan_json_value(line);
  return scan.state == json_scan_result::kind::complete
         and scan.top_level == '{'
         and detail::trim_front(line.substr(scan.end)).empty();
}

auto detect_ndjson(read_detection_input input) -> read_detection_result {
  auto lines = detail::split_lines(input.bytes, 3);
  std::erase_if(lines, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  if (lines.empty()) {
    return input.eof ? reject() : need_more();
  }
  if (lines.size() >= 2 and complete_json_object_line(lines[0])
      and complete_json_object_line(lines[1])) {
    return match(82, "multiple JSON object lines");
  }
  auto bytes = detail::trim_front(input.bytes);
  auto scan = scan_json_value(bytes);
  if (scan.state == json_scan_result::kind::complete
      and scan.top_level == '{') {
    auto rest = detail::trim_front(bytes.substr(scan.end));
    if (not rest.empty()) {
      return match(82, "JSON object followed by more input");
    }
  }
  if (input.eof) {
    return reject();
  }
  return need_more();
}

auto detect_json_field(read_detection_input input, std::string_view field,
                       uint64_t confidence) -> read_detection_result {
  auto object = detect_json_object(input);
  auto ndjson = detect_ndjson(input);
  if (object.state == detection_state::reject
      and ndjson.state == detection_state::reject) {
    return reject();
  }
  if (input.bytes.contains(field)) {
    return match(confidence, fmt::format("contains `{}`", field));
  }
  if (object.state == detection_state::need_more
      or ndjson.state == detection_state::need_more) {
    return need_more();
  }
  return reject();
}

auto detect_gelf(read_detection_input input) -> read_detection_result {
  if (input.bytes.contains("\"version\"") and input.bytes.contains("\"host\"")
      and input.bytes.contains("\"short_message\"")) {
    return match(90, "GELF fields");
  }
  auto object = detect_json_object(input);
  return object.state == detection_state::need_more ? need_more() : reject();
}

auto detect_zeek_tsv(read_detection_input input) -> read_detection_result {
  auto bytes = detail::trim_front(input.bytes);
  if (bytes.starts_with("#separator") or bytes.starts_with("#fields")
      or bytes.starts_with("#types")) {
    return match(95, "Zeek TSV header");
  }
  return bytes.size() < 10 and not input.eof ? need_more() : reject();
}

auto detect_syslog(read_detection_input input) -> read_detection_result {
  auto bytes = detail::trim_front(input.bytes);
  if (bytes.empty()) {
    return input.eof ? reject() : need_more();
  }
  if (bytes.front() == '<') {
    auto end = bytes.find('>');
    if (end != std::string_view::npos and end > 1 and end <= 4) {
      auto pri = bytes.substr(1, end - 1);
      if (std::ranges::all_of(pri, [](char c) {
            return c >= '0' and c <= '9';
          })) {
        return match(50, "syslog priority prefix");
      }
    }
  }
  return bytes.size() < 32 and not input.eof ? need_more() : reject();
}

auto detect_xsv(read_detection_input input, char sep, uint64_t confidence)
  -> read_detection_result {
  if (not detail::is_valid_utf8(input.bytes)) {
    if (not input.eof and detail::is_valid_utf8_prefix(input.bytes)) {
      return need_more("partial UTF-8 sequence");
    }
    return reject();
  }
  auto lines = detail::split_lines(input.bytes, 3);
  std::erase_if(lines, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  auto counts = std::vector<size_t>{};
  for (auto line : lines) {
    if (line.starts_with("#")) {
      continue;
    }
    counts.push_back(static_cast<size_t>(std::ranges::count(line, sep)));
  }
  if (counts.size() >= 2 and counts[0] > 0
      and std::ranges::all_of(counts, [&](size_t count) {
            return count == counts[0];
          })) {
    return match(confidence, "stable delimiter counts");
  }
  if (input.eof and counts.size() == 1 and counts[0] > 0) {
    return match(confidence - 5, "single delimited row");
  }
  return input.eof ? reject() : need_more();
}

auto detect_kv(read_detection_input input) -> read_detection_result {
  auto lines = detail::split_lines(input.bytes, 2);
  std::erase_if(lines, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  if (lines.empty()) {
    return input.eof ? reject() : need_more();
  }
  auto has_kv = [](std::string_view line) {
    auto eq = line.find('=');
    return eq != std::string_view::npos and eq > 0 and eq + 1 < line.size();
  };
  if (std::ranges::all_of(lines, has_kv)) {
    return match(65, "key-value assignments");
  }
  return input.eof ? reject() : need_more();
}

auto detect_yaml(read_detection_input input) -> read_detection_result {
  if (not detail::is_valid_utf8(input.bytes)) {
    return reject();
  }
  auto bytes = detail::trim_front(input.bytes);
  if (bytes.empty()) {
    return input.eof ? reject() : need_more();
  }
  if (bytes.front() == '{' or bytes.front() == '[') {
    return reject("JSON-compatible YAML is left to JSON detectors");
  }
  auto lines = detail::split_lines(bytes, 3);
  std::erase_if(lines, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  if (std::ranges::any_of(lines, [](std::string_view line) {
        auto colon = line.find(':');
        return colon != std::string_view::npos and colon > 0;
      })) {
    return match(45, "YAML mapping");
  }
  return input.eof ? reject() : need_more();
}

auto detect_pcap(read_detection_input input) -> read_detection_result;

auto detect(DetectorKind kind, read_detection_input input)
  -> read_detection_result {
  switch (kind) {
    case DetectorKind::json_suricata:
      return detect_json_field(input, "\"event_type\"", 90);
    case DetectorKind::json_zeek:
      return detect_json_field(input, "\"_path\"", 90);
    case DetectorKind::json_gelf:
      return detect_gelf(input);
    case DetectorKind::json_ndjson:
      return detect_ndjson(input);
    case DetectorKind::json_array:
      return detect_json_array(input);
    case DetectorKind::json_object:
      return detect_json_object(input);
    case DetectorKind::cef: {
      auto bytes = detail::trim_front(input.bytes);
      if (bytes.size() < std::string_view{"CEF:"}.size()) {
        return input.eof ? reject() : need_more();
      }
      return bytes.starts_with("CEF:") ? match(90) : reject();
    }
    case DetectorKind::leef: {
      auto bytes = detail::trim_front(input.bytes);
      if (bytes.size() < std::string_view{"LEEF:"}.size()) {
        return input.eof ? reject() : need_more();
      }
      return bytes.starts_with("LEEF:") ? match(90) : reject();
    }
    case DetectorKind::zeek_tsv:
      return detect_zeek_tsv(input);
    case DetectorKind::syslog:
      return detect_syslog(input);
    case DetectorKind::csv:
      return detect_xsv(input, ',', 60);
    case DetectorKind::tsv:
      return detect_xsv(input, '\t', 60);
    case DetectorKind::ssv:
      return detect_xsv(input, ' ', 55);
    case DetectorKind::kv:
      return detect_kv(input);
    case DetectorKind::yaml:
      return detect_yaml(input);
    case DetectorKind::pcap:
      return detect_pcap(input);
    case DetectorKind::feather:
      if (input.bytes.size() < std::string_view{"ARROW1"}.size()) {
        return input.eof ? reject() : need_more();
      }
      return input.bytes.starts_with("ARROW1") ? match(100) : reject();
    case DetectorKind::bitz:
      if (input.bytes.size() < std::string_view{"TENZIR-BITZ"}.size()) {
        return input.eof ? reject() : need_more();
      }
      return input.bytes.starts_with("TENZIR-BITZ") ? match(100) : reject();
    case DetectorKind::parquet:
      if (input.bytes.size() < std::string_view{"PAR1"}.size()) {
        return input.eof ? reject() : need_more();
      }
      return input.bytes.starts_with("PAR1") ? match(100) : reject();
  }
  TENZIR_UNREACHABLE();
}

auto detect_pcap(read_detection_input input) -> read_detection_result {
  constexpr auto magics = std::array{
    std::string_view{"\xd4\xc3\xb2\xa1", 4},
    std::string_view{"\xa1\xb2\xc3\xd4", 4},
    std::string_view{"\x4d\x3c\xb2\xa1", 4},
    std::string_view{"\xa1\xb2\x3c\x4d", 4},
    std::string_view{"\x0a\x0d\x0d\x0a", 4},
  };
  if (input.bytes.size() < 4) {
    return input.eof ? reject() : need_more();
  }
  return std::ranges::any_of(magics,
                             [&](std::string_view magic) {
                               return input.bytes.starts_with(magic);
                             })
           ? match(100, "pcap magic")
           : reject();
}

auto builtin_candidates() -> std::vector<Candidate> {
  auto result = std::vector<Candidate>{};
  auto add = [&](std::string id, std::string pipeline, int64_t priority,
                 DetectorKind detector) {
    result.push_back(Candidate{
      .id = std::move(id),
      .pipeline = std::move(pipeline),
      .priority = priority,
      .detector = detector,
    });
  };
  add("json.suricata", "read_suricata", 30, DetectorKind::json_suricata);
  add("json.zeek", "read_zeek_json", 30, DetectorKind::json_zeek);
  add("json.gelf", "read_gelf", 30, DetectorKind::json_gelf);
  add("json.ndjson", "read_ndjson", 20, DetectorKind::json_ndjson);
  add("json.array_of_objects", "read_json arrays_of_objects=true", 10,
      DetectorKind::json_array);
  add("json.object", "read_json", 0, DetectorKind::json_object);
  add("cef", "read_cef", 20, DetectorKind::cef);
  add("leef", "read_leef", 20, DetectorKind::leef);
  add("zeek.tsv", "read_zeek_tsv", 20, DetectorKind::zeek_tsv);
  add("syslog", "read_syslog", 0, DetectorKind::syslog);
  add("xsv.csv", "read_csv", 0, DetectorKind::csv);
  add("xsv.tsv", "read_tsv", 0, DetectorKind::tsv);
  add("xsv.ssv", "read_ssv", 0, DetectorKind::ssv);
  add("kv", "read_kv", 0, DetectorKind::kv);
  add("yaml", "read_yaml", -10, DetectorKind::yaml);
  add("pcap", "read_pcap", 50, DetectorKind::pcap);
  add("feather", "read_feather", 50, DetectorKind::feather);
  add("bitz", "read_bitz", 50, DetectorKind::bitz);
  add("parquet", "read_parquet", 50, DetectorKind::parquet);
  return result;
}

auto plugin_detection_candidates()
  -> const std::vector<read_detection_candidate>& {
  static const auto result = [] {
    auto result = std::vector<read_detection_candidate>{};
    for (auto const* plugin : plugins::get<operator_factory_plugin>()) {
      auto candidates = plugin->read_detection_candidates();
      std::move(candidates.begin(), candidates.end(),
                std::back_inserter(result));
    }
    return result;
  }();
  return result;
}

class ReadAuto final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadAuto(ReadAutoArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    candidates_ = builtin_candidates();
    auto names = ctx.reg().operator_names();
    auto strip_tql2_prefix = [](std::string_view name) {
      constexpr auto prefix = std::string_view{"tql2."};
      return name.starts_with(prefix) ? name.substr(prefix.size()) : name;
    };
    auto operator_available = [&](std::string_view name) {
      name = strip_tql2_prefix(name);
      return std::ranges::find(names, name) != names.end();
    };
    for (auto& candidate : candidates_) {
      auto name = detail::split_once(candidate.pipeline, " ").first;
      if (not operator_available(name)) {
        candidate.live = false;
      }
    }
    auto& plugin_candidates = plugin_detection_candidates();
    for (auto index = size_t{0}; index < plugin_candidates.size(); ++index) {
      auto& candidate = plugin_candidates[index];
      candidates_.push_back(Candidate{
        .id = candidate.id,
        .priority = candidate.priority,
        .plugin_candidate = index,
        .live = not candidate.id.empty() and not candidate.operator_name.empty()
                and candidate.detect
                and operator_available(candidate.operator_name),
      });
    }
    co_return;
  }

  auto process(chunk_ptr input, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (selected_) {
      co_await push_to_selected(std::move(input), ctx);
      co_return;
    }
    if (input->size() == 0) {
      co_return;
    }
    seen_bytes_ = true;
    buffered_.push_back(input);
    if (probe_.size() < args_.max_probe_bytes) {
      auto remaining = args_.max_probe_bytes - probe_.size();
      auto bytes = std::string_view{
        reinterpret_cast<const char*>(input->data()),
        std::min<size_t>(input->size(), detail::narrow<size_t>(remaining)),
      };
      probe_.append(bytes);
    }
    auto exhausted = probe_.size() >= args_.max_probe_bytes;
    if (auto selected = select(exhausted, ctx)) {
      co_await spawn_selected(*selected, ctx);
    }
  }

  auto finalize(Push<table_slice>&, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (selected_) {
      co_await close_selected(ctx);
      co_return FinalizeBehavior::continue_;
    }
    if (not seen_bytes_) {
      done_ = true;
      co_return FinalizeBehavior::done;
    }
    if (auto selected = select(true, ctx)) {
      co_await spawn_selected(*selected, ctx);
      co_await close_selected(ctx);
      co_return FinalizeBehavior::continue_;
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push,
                   OpCtx&) -> Task<void> override {
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    done_ = true;
    co_return;
  }

  auto finish_sub(SubKeyView key, failure error, Push<table_slice>& push,
                  OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(error);
    co_await finish_sub(key, push, ctx);
  }

private:
  auto select(bool final, OpCtx& ctx) -> std::optional<size_t> {
    auto any_need_more = false;
    auto matches = std::vector<size_t>{};
    for (auto i = size_t{0}; i < candidates_.size(); ++i) {
      auto& candidate = candidates_[i];
      if (not candidate.live) {
        continue;
      }
      auto result = std::invoke([&] {
        if (candidate.detector) {
          return detect(*candidate.detector, {probe_, final});
        }
        if (candidate.plugin_candidate) {
          auto& plugin_candidate
            = plugin_detection_candidates()[*candidate.plugin_candidate];
          return plugin_candidate.detect(read_detection_input{probe_, final});
        }
        return reject();
      });
      candidate.last = result;
      switch (result.state) {
        case detection_state::reject:
          candidate.live = false;
          break;
        case detection_state::need_more:
          any_need_more = true;
          break;
        case detection_state::match:
          matches.push_back(i);
          break;
      }
    }
    if (not final and any_need_more) {
      return std::nullopt;
    }
    if (matches.empty()) {
      return fallback(final, ctx);
    }
    auto score = [&](size_t index) {
      auto& candidate = candidates_[index];
      TENZIR_ASSERT(candidate.last);
      return std::pair{candidate.last->confidence, candidate.priority};
    };
    std::ranges::sort(matches, [&](size_t lhs, size_t rhs) {
      return score(lhs) > score(rhs);
    });
    if (matches.size() >= 2 and score(matches[0]) == score(matches[1])) {
      diagnostic::error("read_auto detection is ambiguous")
        .primary(args_.operator_location)
        .note("candidates `{}` and `{}` both matched",
              candidates_[matches[0]].id, candidates_[matches[1]].id)
        .emit(ctx);
      return std::nullopt;
    }
    return matches[0];
  }

  auto fallback(bool final, OpCtx& ctx) -> std::optional<size_t> {
    if (not final) {
      return std::nullopt;
    }
    if (args_.fallback == "none") {
      diagnostic::error("read_auto could not detect an input format")
        .primary(args_.operator_location)
        .emit(ctx);
      return std::nullopt;
    }
    auto pipeline = std::string{};
    if (args_.fallback == "lines") {
      if (not detail::is_valid_utf8(probe_)) {
        diagnostic::error("read_auto fallback `lines` requires UTF-8 input")
          .primary(args_.operator_location)
          .emit(ctx);
        return std::nullopt;
      }
      pipeline = "read_lines";
    } else {
      pipeline
        = detail::is_valid_utf8(probe_) ? "read_all" : "read_all binary=true";
    }
    candidates_.push_back(Candidate{
      .id = fmt::format("fallback.{}", args_.fallback),
      .pipeline = std::move(pipeline),
      .priority = std::numeric_limits<int64_t>::min(),
      .detector = DetectorKind::json_object,
    });
    return candidates_.size() - 1;
  }

  auto spawn_selected(size_t index, OpCtx& ctx) -> Task<void> {
    selected_ = index;
    auto& candidate = candidates_[index];
    auto root = compile_ctx::make_root(base_ctx{ctx});
    auto pipe = failure_or<ir::pipeline>{};
    if (candidate.plugin_candidate) {
      auto& plugin_candidate
        = plugin_detection_candidates()[*candidate.plugin_candidate];
      auto operator_name = std::string_view{plugin_candidate.operator_name};
      constexpr auto prefix = std::string_view{"tql2."};
      if (operator_name.starts_with(prefix)) {
        operator_name.remove_prefix(prefix.size());
      }
      auto identifiers = std::vector<ast::identifier>{};
      for (auto segment : detail::split(operator_name, "::")) {
        identifiers.emplace_back(segment, location::unknown);
      }
      auto ast = ast::pipeline{};
      ast.body.push_back(ast::invocation{
        ast::entity{std::move(identifiers)},
        plugin_candidate.args,
      });
      pipe = std::move(ast).compile(root);
    } else {
      auto provider = session_provider::make(ctx.dh());
      auto session = provider.as_session();
      auto ast
        = parse_pipeline_with_bad_diagnostics(candidate.pipeline, session);
      if (not ast) {
        diagnostic::error("failed to parse read-auto candidate `{}`",
                          candidate.id)
          .primary(args_.operator_location)
          .emit(ctx);
        co_return;
      }
      pipe = std::move(*ast).compile(root);
    }
    if (not pipe) {
      diagnostic::error("failed to compile read-auto candidate `{}`",
                        candidate.id)
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    co_await ctx.spawn_sub<chunk_ptr>(int64_t{0}, std::move(*pipe),
                                      DiagnosticBehavior::Unchanged);
    for (auto& chunk : buffered_) {
      co_await push_to_selected(std::move(chunk), ctx);
    }
    buffered_.clear();
  }

  auto push_to_selected(chunk_ptr chunk, OpCtx& ctx) -> Task<void> {
    auto sub = ctx.get_sub(int64_t{0});
    if (not sub) {
      co_return;
    }
    std::ignore
      = co_await as<SubHandle<chunk_ptr>>(*sub).push(std::move(chunk));
  }

  auto close_selected(OpCtx& ctx) -> Task<void> {
    auto sub = ctx.get_sub(int64_t{0});
    if (not sub) {
      co_return;
    }
    co_await as<SubHandle<chunk_ptr>>(*sub).close();
  }

  ReadAutoArgs args_;
  std::vector<Candidate> candidates_;
  std::vector<chunk_ptr> buffered_;
  std::string probe_;
  std::optional<size_t> selected_;
  bool seen_bytes_ = false;
  bool done_ = false;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_auto";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadAutoArgs, ReadAuto>{};
    auto fallback = d.named_optional("fallback", &ReadAutoArgs::fallback);
    d.named_optional("max_probe_bytes", &ReadAutoArgs::max_probe_bytes);
    d.operator_location(&ReadAutoArgs::operator_location);
    d.validate([fallback](DescribeCtx& ctx) -> Empty {
      auto value = ctx.get(fallback).value_or("none");
      if (value != "none" and value != "lines" and value != "all") {
        diagnostic::error("`fallback` must be one of `none`, `lines`, or `all`")
          .primary(ctx.get_location(fallback).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_auto

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_auto::plugin)
