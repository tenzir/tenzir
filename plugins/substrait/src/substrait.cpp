//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "substrait/substrait.hpp"

#include "substrait/plan.pb.h"

#include <vast/error.hpp>

namespace vast::plugins::substrait {

// SELECT dns.rrname from suricata.dns;
// ->
// suricata.dns.dns.rrname

class SubstraitParseState {
public:
  SubstraitParseState()
    : result_(caf::make_error(ec::unimplemented, "uninitialized")) {
  }

  void visit_plan(const ::substrait::Plan& plan) {
    if (plan.relations_size() == 0) {
      result_ = caf::make_error(ec::unimplemented, "no relations");
      return;
    }
    VAST_INFO("found {} relations", plan.relations_size());
    for (const auto& relation : plan.relations()) {
      switch (relation.rel_type_case()) {
        case ::substrait::PlanRel::REL_TYPE_NOT_SET: {
          result_ = caf::make_error(ec::format_error, "invalid rel");
          return;
        }
        case ::substrait::PlanRel::kRel: {
          const auto& rel = relation.rel();
          visit_rel(rel);
          break;
        }
        case ::substrait::PlanRel::kRoot: {
          const auto& root = relation.root();
          visit_rel_root(root);
          break;
        }
      }
    }
  }

  void visit_rel(const ::substrait::Rel& rel) {
    switch (rel.rel_type_case()) {
      case ::substrait::Rel::RelTypeCase::kRead: {
        VAST_INFO("read rel!");
        const auto& read_rel = rel.read();
        visit_read_rel(read_rel);
        break;
      }
      case ::substrait::Rel::RelTypeCase::kProject: {
        const auto& project_rel = rel.project();
        visit_project_rel(project_rel);
        break;
      }
      default:
        VAST_INFO("other rel");
        break;
    }
  }

  void visit_rel_root(const ::substrait::RelRoot& root) {
    field_names.resize(root.names_size());
    for (int i = 0; i < root.names_size(); ++i)
      field_names[i] = root.names(i);
    VAST_INFO("got field names from root node: {}", field_names);
    if (!root.has_input()) {
      VAST_WARN("no input");
      return;
    }
    visit_rel(root.input());
  }

  void visit_read_rel(const ::substrait::ReadRel& read_rel) {
    if (!read_rel.has_named_table()) {
      result_ = caf::make_error(ec::format_error, "only supporting named "
                                                  "tables");
      return;
    }
    const auto& names = read_rel.named_table().names();
    std::string name;
    if (!names.empty()) {
      name = names[0];
      for (int i = 1; i < names.size(); ++i)
        name += "." + names[i];
    }
    result_ = vast::predicate{meta_extractor{meta_extractor::type},
                              relational_operator::equal, data{name}};
  }

  void visit_project_rel(const ::substrait::ProjectRel& project_rel) {
    for (const auto& expression : project_rel.expressions()) {
      if (!expression.has_selection())
        continue; // only support select statements for now
      auto selection = expression.selection();
      // TODO: Use `selection` expressions to restrict field names
    }
    VAST_INFO("projection has {} expressions", project_rel.expressions_size());
    if (!project_rel.has_input()) {
      result_ = caf::make_error(ec::format_error, "no input to projection");
      return;
    }
    visit_rel(project_rel.input());
  }

  [[nodiscard]] caf::expected<vast::expression> result() const {
    return result_;
  }

private:
  caf::expected<vast::expression> result_;
  std::vector<std::string> field_names;
};

[[nodiscard]] caf::expected<vast::expression>
parse_substrait(const ::substrait::Plan& plan) {
  SubstraitParseState parser;
  parser.visit_plan(plan);
  return parser.result();
}

} // namespace vast::plugins::substrait
