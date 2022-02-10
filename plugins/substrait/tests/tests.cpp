//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE substrait

#include "substrait/plan.pb.h"
#include "substrait/substrait.hpp"

#include <vast/test/test.hpp>

#include <google/protobuf/util/json_util.h>

namespace {

std::string isthmus_json
  = R"_({"relations":[{"root":{"input":{"project":{"common":{"emit":{"outputMapping":[16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]}},"input":{"read":{"common":{"direct":{}},"baseSchema":{"names":["L_ORDERKEY","L_PARTKEY","L_SUPPKEY","L_LINENUMBER","L_QUANTITY","L_EXTENDEDPRICE","L_DISCOUNT","L_TAX","L_RETURNFLAG","L_LINESTATUS","L_SHIPDATE","L_COMMITDATE","L_RECEIPTDATE","L_SHIPINSTRUCT","L_SHIPMODE","L_COMMENT"],"struct":{"types":[{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"precision":19,"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"precision":19,"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"precision":19,"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"precision":19,"nullability":"NULLABILITY_NULLABLE"}},{"fixedChar":{"length":1,"nullability":"NULLABILITY_NULLABLE"}},{"fixedChar":{"length":1,"nullability":"NULLABILITY_NULLABLE"}},{"date":{"nullability":"NULLABILITY_NULLABLE"}},{"date":{"nullability":"NULLABILITY_NULLABLE"}},{"date":{"nullability":"NULLABILITY_NULLABLE"}},{"fixedChar":{"length":25,"nullability":"NULLABILITY_NULLABLE"}},{"fixedChar":{"length":10,"nullability":"NULLABILITY_NULLABLE"}},{"varchar":{"length":44,"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"namedTable":{"names":["LINEITEM"]}}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":2}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":3}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":4}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":5}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":6}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":7}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":8}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":9}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":10}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":11}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":12}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":13}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":14}},"rootReference":{}}},{"selection":{"directReference":{"structField":{"field":15}},"rootReference":{}}}]}},"names":["L_ORDERKEY","L_PARTKEY","L_SUPPKEY","L_LINENUMBER","L_QUANTITY","L_EXTENDEDPRICE","L_DISCOUNT","L_TAX","L_RETURNFLAG","L_LINESTATUS","L_SHIPDATE","L_COMMITDATE","L_RECEIPTDATE","L_SHIPINSTRUCT","L_SHIPMODE","L_COMMENT"]}}]})_";

// Create a plan for the query "SELECT * FROM suricata.http;"
substrait::Plan create_plan() {
  auto plan = substrait::Plan{};
  google::protobuf::util::JsonParseOptions options2;
  JsonStringToMessage(isthmus_json, &plan, options2);
  return plan;
}

} // namespace

TEST(parse plan) {
  auto plan = create_plan();
  auto expression = vast::plugins::substrait::parse_substrait(plan);
  CHECK_NOERROR(expression);
  VAST_INFO("{}", *expression);
}
