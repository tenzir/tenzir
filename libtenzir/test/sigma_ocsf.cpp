//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "../builtins/operators/sigma/ocsf.hpp"
#include "../builtins/operators/sigma/plan_cache.hpp"
#include "tenzir/test/test.hpp"

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::plugins::sigma;

TEST("OCSF Sigma mapping catalog") {
  auto const catalog = ocsf::catalog();
  REQUIRE_EQUAL(catalog.size(), ocsf::catalog_documents().size());
  auto ids = std::vector<std::string_view>{};
  for (auto const& mapping : catalog) {
    CHECK(not mapping.id.empty());
    CHECK(not mapping.selector.product.empty());
    CHECK(not mapping.fields.empty());
    CHECK(std::ranges::find(ids, mapping.id) == ids.end());
    ids.push_back(mapping.id);
    auto fields = std::vector<std::string_view>{};
    for (auto const& field : mapping.fields) {
      CHECK(not field.sigma_field.empty());
      CHECK(std::ranges::find(fields, field.sigma_field) == fields.end());
      fields.push_back(field.sigma_field);
    }
  }
  CHECK(std::ranges::find(ids, "sigma/windows/process_creation") != ids.end());
  CHECK(std::ranges::find(ids, "sigma/windows/registry_set") != ids.end());
  CHECK(std::ranges::find(ids, "sigma/zeek/dns") != ids.end());
}

TEST("OCSF Sigma mapping selectors") {
  auto process = tenzir::sigma::LogSource{
    .category = "process_creation",
    .product = "windows",
    .service = None{},
  };
  auto registry = tenzir::sigma::LogSource{
    .category = "registry_set",
    .product = "windows",
    .service = "sysmon",
  };
  auto dns = tenzir::sigma::LogSource{
    .category = None{},
    .product = "zeek",
    .service = "dns",
  };
  auto registry_event = tenzir::sigma::LogSource{
    .category = "registry_event",
    .product = "windows",
    .service = None{},
  };
  auto unsupported = tenzir::sigma::LogSource{
    .category = "clipboard_capture",
    .product = "windows",
    .service = None{},
  };
  REQUIRE(ocsf::find_mapping(process));
  CHECK_EQUAL(ocsf::find_mapping(process)->id,
              "sigma/windows/process_creation");
  REQUIRE(ocsf::find_mapping(registry));
  CHECK_EQUAL(ocsf::find_mapping(registry)->id, "sigma/windows/registry_set");
  REQUIRE(ocsf::find_mapping(dns));
  CHECK_EQUAL(ocsf::find_mapping(dns)->id, "sigma/zeek/dns");
  CHECK(not ocsf::find_mapping(unsupported));
  CHECK_EQUAL(ocsf::find_mappings(process).size(), 1u);
  // The umbrella registry category spans key and value activity; both
  // alternatives plan independently.
  auto const alternatives = ocsf::find_mappings(registry_event);
  REQUIRE_EQUAL(alternatives.size(), 2u);
  CHECK(alternatives[0]->guard.class_uid != alternatives[1]->guard.class_uid);
  CHECK(ocsf::find_mappings(unsupported).empty());
}

TEST("OCSF Sigma provenance-scoped guards") {
  auto registry = tenzir::sigma::LogSource{
    .category = "registry_set",
    .product = "windows",
    .service = "sysmon",
  };
  auto dns = tenzir::sigma::LogSource{
    .category = None{},
    .product = "zeek",
    .service = "dns",
  };
  auto const& registry_mapping = *ocsf::find_mapping(registry);
  // A rule without provenance-scoped fields matches any conformant producer
  // of the event class: no producer constraints apply.
  auto lenient = ocsf::make_guard(registry_mapping, false);
  CHECK(not lenient.event.source);
  CHECK(not lenient.event.log_name);
  CHECK(lenient.event.conflicting_log_names.empty());
  // The OS guard derives from the logsource product; no catalog knob.
  REQUIRE(lenient.event.os);
  CHECK_EQUAL(*lenient.event.os, ocsf::OsKind::windows);
  // A rule using a provenance-scoped field such as `EventID` requires the
  // producer that the mapping declares, independent of the rule's service:
  // category-only rules get the same protection.
  auto strict = ocsf::make_guard(registry_mapping, true);
  REQUIRE(strict.event.source);
  CHECK_EQUAL(*strict.event.source, ocsf::SourceKind::sysmon);
  // The provenance-scoped marker parses from the catalog.
  auto const& event_id = *ocsf::find_field(registry_mapping, "EventID");
  CHECK(event_id.provenance_scoped);
  auto const& target = *ocsf::find_field(registry_mapping, "TargetObject");
  CHECK(not target.provenance_scoped);
  // The Zeek DNS family projects only producer-independent DNS Activity
  // attributes; no field constrains the producer.
  auto const& dns_mapping = *ocsf::find_mapping(dns);
  for (auto const& field : dns_mapping.fields) {
    CHECK(not field.projection.provenance_scoped);
  }
  auto dns_guard = ocsf::make_guard(dns_mapping, false);
  CHECK(not dns_guard.event.source);
  CHECK(not dns_guard.event.log_name);
  // A non-OS product implies no OS guard.
  CHECK(not dns_guard.event.os);
}

namespace {

auto parse_error(std::string_view yaml) -> std::string {
  auto result = ocsf::parse_mapping(yaml);
  REQUIRE(result.is_err());
  return std::move(result).unwrap_err();
}

constexpr auto minimal_mapping = std::string_view{R"__(
id: sigma/test/minimal
title: Minimal test mapping
logsource:
  product: test
guard:
  class-uid: 1001
  activities: [1]
fields:
  Field:
    primitive: path
    path: some.path
)__"};

} // namespace

TEST("OCSF Sigma catalog documents parse and lint") {
  auto const documents = ocsf::catalog_documents();
  auto const catalog = ocsf::catalog();
  REQUIRE_EQUAL(documents.size(), catalog.size());
  auto selectors
    = std::vector<std::tuple<std::string, std::string, std::string, uint64_t>>{};
  for (auto const& document : documents) {
    auto mapping = ocsf::parse_mapping(document);
    REQUIRE(mapping.is_ok());
  }
  for (auto const& mapping : catalog) {
    // Documents sharing a selector triple must target distinct classes so
    // that at most one alternative matches a given event.
    auto selector
      = std::tuple{mapping.selector.category, mapping.selector.product,
                   mapping.selector.service, mapping.guard.class_uid};
    CHECK(std::ranges::find(selectors, selector) == selectors.end());
    selectors.push_back(std::move(selector));
    CHECK(not mapping.title.empty());
    CHECK(mapping.guard.class_uid > 0);
    for (auto const& unmapped : mapping.unmapped) {
      CHECK(not unmapped.field.empty());
      CHECK(not unmapped.reason.empty());
      // An unmapped field must not also have a projection.
      CHECK(not ocsf::find_field(mapping, unmapped.field));
    }
    // The operator never reads source residue: every projected path must
    // name a real OCSF attribute. A member of a free-form `data` object is
    // a producer payload, so reading one requires provenance: without it a
    // firewall rule's `Action: 3` would compare against any producer's
    // `entity.data.action_id`.
    for (auto const& field : mapping.fields) {
      auto const paths = ocsf::projection_paths(field.projection.value);
      for (auto const path : paths) {
        CHECK(not path.starts_with("unmapped"));
        if (ocsf::reads_free_form_data(path)) {
          CHECK(field.projection.provenance_scoped);
        }
      }
    }
    CHECK(not mapping.guard.activity_ids.empty());
    for (auto const activity : mapping.guard.activity_ids) {
      CHECK(activity != 0);
      CHECK(activity != 99);
    }
    if (not mapping.guard.conflicting_log_names.empty()) {
      CHECK(mapping.guard.log_name.has_value());
    }
  }
  // The free-form detection sees through nesting but ignores real attributes
  // that merely carry the name.
  CHECK(ocsf::reads_free_form_data("entity.data.action_id"));
  CHECK(ocsf::reads_free_form_data("application.data.path"));
  CHECK(ocsf::reads_free_form_data("data.x"));
  CHECK(not ocsf::reads_free_form_data("reg_value.data"));
  CHECK(not ocsf::reads_free_form_data("data"));
  CHECK(not ocsf::reads_free_form_data("metadata.event_code"));
  // The firewall family reads raw rule attributes from `entity.data`; a rule
  // naming only `Action` must still require the Windows Firewall producer.
  auto const firewall = *ocsf::find_mapping(tenzir::sigma::LogSource{
    .category = None{},
    .product = "windows",
    .service = "firewall-as",
  });
  auto const& action = *ocsf::find_field(firewall, "Action");
  CHECK(action.provenance_scoped);
  auto const& rule_name = *ocsf::find_field(firewall, "RuleName");
  CHECK(not rule_name.provenance_scoped);
}

TEST("OCSF Sigma mapping parser accepts a minimal document") {
  auto mapping = ocsf::parse_mapping(minimal_mapping);
  REQUIRE(mapping.is_ok());
  auto const& parsed = mapping.unwrap();
  CHECK_EQUAL(parsed.id, "sigma/test/minimal");
  CHECK_EQUAL(parsed.selector.product, "test");
  CHECK(parsed.selector.category.empty());
  CHECK(not parsed.selector.optional_service);
  REQUIRE_EQUAL(parsed.fields.size(), 1u);
  CHECK_EQUAL(parsed.fields[0].sigma_field, "Field");
  auto const* path
    = try_as<ocsf::PathField>(&parsed.fields[0].projection.value);
  REQUIRE(path);
  CHECK_EQUAL(path->path, "some.path");
  CHECK(parsed.fields[0].projection.kind == ocsf::ProjectedKind::any);
}

TEST("OCSF Sigma mapping parser rejects malformed documents") {
  CHECK(parse_error("not: a mapping").find("unknown key `not`")
        != std::string::npos);
  CHECK(parse_error("logsource: {product: x}").find("missing `id`")
        != std::string::npos);
  CHECK(parse_error("[1, 2]").find("must be a record") != std::string::npos);
  // Unknown keys fail loudly instead of being ignored.
  auto with = [&](std::string_view needle, std::string_view replacement) {
    auto result = std::string{minimal_mapping};
    auto const at = result.find(needle);
    REQUIRE(at != std::string::npos);
    result.replace(at, needle.size(), replacement);
    return result;
  };
  CHECK(
    parse_error(with("fields:", "extra: 1\nfields:")).find("unknown key `extra`")
    != std::string::npos);
  CHECK(parse_error(with("primitive: path", "primitive: teleport"))
          .find("unknown primitive `teleport`")
        != std::string::npos);
  CHECK(parse_error(with("path: some.path", "path: some.path\n    member: x"))
          .find("unknown key `member`")
        != std::string::npos);
  CHECK(parse_error(with("activities: [1]", "activities: [1, 99]"))
          .find("must not list 0 (Unknown) or 99 (Other)")
        != std::string::npos);
  CHECK(parse_error(with("product: test", "product: test\n  optional-service: "
                                          "true"))
          .find("`optional-service` requires a `service`")
        != std::string::npos);
  CHECK(parse_error(with("class-uid: 1001", "class-uid: 1001\n  source: nsa"))
          .find("unknown source kind `nsa`")
        != std::string::npos);
  CHECK(
    parse_error(with("class-uid: 1001", "class-uid: 1001\n  "
                                        "conflicting-log-names: [other.log]"))
      .find("`conflicting-log-names` requires a `log-name`")
    != std::string::npos);
}

TEST("OCSF Sigma dictionary rule values") {
  auto dictionary = ocsf::DictionaryRuleValue{
    .name = "Windows integrity level",
    .entries = {{"system", 5}, {"high", 4}},
  };
  auto projection = ocsf::FieldProjection{
    .value = ocsf::PathField{"process.integrity_id"},
    .rule_value = dictionary,
    .kind = ocsf::ProjectedKind::integral,
  };
  auto mapped = ocsf::project_rule_value(projection, data{"System"});
  REQUIRE(mapped.is_ok());
  CHECK_EQUAL(mapped.unwrap(), data{int64_t{5}});
  auto passthrough = ocsf::project_rule_value(projection, data{int64_t{4}});
  REQUIRE(passthrough.is_ok());
  CHECK_EQUAL(passthrough.unwrap(), data{int64_t{4}});
  auto unknown = ocsf::project_rule_value(projection, data{"Cosmic"});
  REQUIRE(unknown.is_err());
  CHECK(std::move(unknown).unwrap_err().find("Windows integrity level")
        != std::string::npos);
}

TEST("OCSF Sigma dictionary rule values accept booleans") {
  auto projection = ocsf::FieldProjection{
    .value = ocsf::PathField{"connection_info.direction_id"},
    .rule_value = ocsf::DictionaryRuleValue{
      .name = "connection direction",
      .entries = {{"true", 2}, {"false", 1}},
    },
    .kind = ocsf::ProjectedKind::integral,
  };
  // Stock rules spell `Initiated` both as a YAML boolean and as a string.
  auto spelled = ocsf::project_rule_value(projection, data{"True"});
  REQUIRE(spelled.is_ok());
  CHECK_EQUAL(spelled.unwrap(), data{int64_t{2}});
  auto boolean = ocsf::project_rule_value(projection, data{false});
  REQUIRE(boolean.is_ok());
  CHECK_EQUAL(boolean.unwrap(), data{int64_t{1}});
}

TEST("OCSF Sigma integrity levels accept names and SIDs") {
  auto process = tenzir::sigma::LogSource{
    .category = "process_creation",
    .product = "windows",
    .service = None{},
  };
  auto const& mapping = *ocsf::find_mapping(process);
  auto const& integrity = *ocsf::find_field(mapping, "IntegrityLevel");
  // Stock rules list the Sysmon name next to the mandatory-label SID that
  // Windows Security emits; both spellings must project to the same enum.
  for (auto const& [spelling, expected] :
       std::vector<std::pair<std::string, int64_t>>{{"High", 4},
                                                    {"S-1-16-12288", 4},
                                                    {"System", 5},
                                                    {"S-1-16-16384", 5},
                                                    {"Medium", 3},
                                                    {"s-1-16-8192", 3},
                                                    {"Low", 2},
                                                    {"S-1-16-4096", 2}}) {
    auto mapped = ocsf::project_rule_value(integrity, data{spelling});
    REQUIRE(mapped.is_ok());
    CHECK_EQUAL(mapped.unwrap(), data{expected});
  }
  CHECK(ocsf::project_rule_value(integrity, data{"S-1-16-99"}).is_err());
}

TEST("OCSF Sigma dictionary parsing rejects invalid entries") {
  auto const document = std::string_view{R"__(
id: sigma/test/dictionary
title: Dictionary test mapping
logsource:
  product: test
guard:
  class-uid: 1001
  activities: [1]
fields:
  Level:
    primitive: path
    path: some.path
    rule-value:
      dictionary:
        name: test level
        entries:
          Mixed: 1
)__"};
  CHECK(parse_error(document).find("must be lowercase") != std::string::npos);
}

TEST("OCSF Sigma schema shape ignores fields no projection reads") {
  auto const paths
    = std::vector<std::string>{"process.cmd_line", "process.path"};
  auto const process = record_type{
    {"path", string_type{}},
    {"cmd_line", string_type{}},
  };
  auto const base = type{record_type{
    {"class_uid", uint64_type{}},
    {"process", process},
  }};
  // Additional and reordered fields that no projection reads leave the shape
  // unchanged, so schema-rich input shares one compiled plan.
  auto const richer = type{record_type{
    {"unmapped", record_type{{"x", string_type{}}}},
    {"process", process},
    {"class_uid", uint64_type{}},
    {"metadata", record_type{{"version", string_type{}}}},
  }};
  CHECK_EQUAL(ocsf::schema_shape(base, paths),
              ocsf::schema_shape(richer, paths));
  // A different type at a read path changes the validation outcome and
  // therefore the shape.
  auto const retyped = type{record_type{
    {"class_uid", uint64_type{}},
    {"process",
     record_type{{"path", int64_type{}}, {"cmd_line", string_type{}}}},
  }};
  CHECK_NOT_EQUAL(ocsf::schema_shape(base, paths),
                  ocsf::schema_shape(retyped, paths));
  // An absent path is part of the shape: validation accepts it, but the
  // projection materializes differently.
  auto const missing = type{record_type{
    {"class_uid", uint64_type{}},
    {"process", record_type{{"path", string_type{}}}},
  }};
  CHECK_NOT_EQUAL(ocsf::schema_shape(base, paths),
                  ocsf::schema_shape(missing, paths));
  // A root field literally named like a dotted path resolves with the same
  // exact-key precedence that validation applies.
  auto const flat = type{record_type{
    {"process.path", string_type{}},
    {"process.cmd_line", string_type{}},
  }};
  CHECK_EQUAL(ocsf::schema_shape(base, paths), ocsf::schema_shape(flat, paths));
  // Literal projections validate the Sigma field itself.
  auto const literal = ocsf::FieldProjection{.value = ocsf::LiteralField{}};
  CHECK_EQUAL(ocsf::validated_paths("custom.field", literal),
              std::vector<std::string>{"custom.field"});
  auto const projected
    = ocsf::FieldProjection{.value = ocsf::PathField{"process.path"}};
  CHECK_EQUAL(ocsf::validated_paths("Image", projected),
              std::vector<std::string>{"process.path"});
}

TEST("Sigma plan cache evicts by cost and keeps the newest plan") {
  auto cache = BudgetedLruCache<std::string, int>{10};
  cache.put("a", 1, 4);
  cache.put("b", 2, 4);
  CHECK_EQUAL(cache.size(), size_t{2});
  CHECK_EQUAL(cache.cost(), uint64_t{8});
  // Touching `a` makes `b` the eviction candidate.
  REQUIRE(cache.get("a"));
  cache.put("c", 3, 4);
  CHECK(not cache.get("b"));
  CHECK(cache.get("a"));
  CHECK(cache.get("c"));
  CHECK_EQUAL(cache.cost(), uint64_t{8});
  // An entry that alone exceeds the budget still stays resident, because
  // the plan for the slice in flight must exist.
  cache.put("d", 4, 100);
  CHECK_EQUAL(cache.size(), size_t{1});
  REQUIRE(cache.get("d"));
  CHECK_EQUAL(*cache.get("d"), 4);
  // Replacing an entry releases its previous cost.
  cache.put("d", 5, 2);
  CHECK_EQUAL(cache.cost(), uint64_t{2});
  CHECK_EQUAL(*cache.get("d"), 5);
  cache.put("e", 6, 8);
  CHECK_EQUAL(cache.size(), size_t{2});
  // Shrinking the budget evicts down to the most recently used entry.
  cache.budget(1);
  CHECK_EQUAL(cache.size(), size_t{1});
  CHECK(cache.get("e"));
  cache.clear();
  CHECK_EQUAL(cache.size(), size_t{0});
  CHECK_EQUAL(cache.cost(), uint64_t{0});
}
