//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/pipeline.hpp>
#include <vast/test/test.hpp>
#include <vast/transformer2.hpp>

#include <caf/test/dsl.hpp>

namespace vast {
namespace {

class dummy_transformer_control final : public transformer_control {
public:
  void abort(caf::error error) override {
    die(fmt::to_string(error));
  }
};

class source final : public crtp_transformer<source> {
public:
  auto operator()() const -> generator<table_slice> {
    co_yield {};
    co_return;
  }
};

class sink final : public crtp_transformer<sink> {
public:
  auto operator()(generator<table_slice> input) const
    -> generator<std::monostate> {
    for (auto&& slice : input) {
      (void)slice;
      co_yield {};
    }
  }
};

TEST(abc) {
  dummy_transformer_control control;
  {
    auto p = unbox(pipeline::parse(""));
    REQUIRE(std::holds_alternative<generator<std::monostate>>(
      unbox(p.instantiate(std::monostate{}, control))));
    REQUIRE(std::holds_alternative<generator<chunk_ptr>>(
      unbox(p.instantiate(generator<chunk_ptr>{}, control))));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, control))));
  }
  {
    auto p = unbox(pipeline::parse("pass"));
    REQUIRE_ERROR(p.instantiate(std::monostate{}, control));
    REQUIRE(std::holds_alternative<generator<chunk_ptr>>(
      unbox(p.instantiate(generator<chunk_ptr>{}, control))));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, control))));
  }
  {
    auto p = unbox(pipeline::parse("taste 42"));
    REQUIRE_ERROR(p.instantiate(std::monostate{}, control));
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, control));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, control))));
  }
  {
    auto p = unbox(pipeline::parse("where :ip"));
    REQUIRE_ERROR(p.instantiate(std::monostate{}, control));
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, control));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, control))));
  }
  {
    auto p = unbox(pipeline::parse("taste 13 | pass | where abc == 123"));
    REQUIRE_ERROR(p.instantiate(std::monostate{}, control));
    REQUIRE_ERROR(p.instantiate(generator<chunk_ptr>{}, control));
    REQUIRE(std::holds_alternative<generator<table_slice>>(
      unbox(p.instantiate(generator<table_slice>{}, control))));
  }
  {
    auto v = unbox(pipeline::parse("taste 42")).unwrap();
    v.insert(v.begin(), std::make_unique<source>());
    v.push_back(std::make_unique<sink>());
    auto p = pipeline{std::move(v)};
    for (auto&& error : make_local_executor(std::move(p))) {
      REQUIRE_EQUAL(error, caf::error{});
    }
  }
}

} // namespace
} // namespace vast
