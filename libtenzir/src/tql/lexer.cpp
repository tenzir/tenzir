//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/core/end_of_input.hpp"
#include "tenzir/concept/parseable/string/literal.hpp"

#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/tql/lexer.hpp>

namespace tenzir::tql {

auto lex(std::string_view content) -> std::vector<token> {
  std::vector<token> result;
  const auto* current = content.begin();
  // clang-format off
  using namespace parsers;
  auto identifier = (alpha | chr{'_'}) >> *(alnum | chr{'_'});
  auto p
    = ignore(identifier)
      ->* [] { return token_kind::identifier; }
    | ignore(+digit >> chr{'.'} >> *digit)
      ->* [] { return token_kind::real; }
    | ignore(+digit)
      ->* [] { return token_kind::integer; }
    | ignore(chr{'"'} >> *(any - chr{'"'}) >> (chr{'"'} | eoi))
      ->* [] { return token_kind::string; }
    | ignore((lit{"//"} | lit{"# "}) >> *(any - '\n'))
      ->* [] { return token_kind::line_comment; }
    | ignore(lit{"/*"} >> *(any - lit{"*/"}) >> (lit{"*/"} | eoi))
      ->* [] { return token_kind::delim_comment; }
    | ignore(chr{'#'} >> identifier)
      ->* [] { return token_kind::meta; }
#define X(x, y) ignore(lit{x}) ->* [] { return token_kind::y; }
    | X("||", logical_or)
    | X("|", pipe)
    | X(">", greater)
    | X(".", dot)
    | X("==", equals)
    | X("=", assign)
    | X("(", lpar)
    | X(")", rpar)
    | X("-", minus)
    | X(",", comma)
    | X("\n", newline)
#undef X
    | ignore(+(space - '\n'))
      ->* [] { return token_kind::whitespace; }
  ;
  // clang-format on
  while (current != content.end()) {
    auto kind = token_kind{};
    if (p.parse(current, content.end(), kind)) {
      result.emplace_back(kind, current - content.begin());
    } else {
      ++current;
      result.emplace_back(token_kind::error, current - content.begin());
    }
  }
  return result;
}

#if 1
class my_parser {
public:
  using enum token_kind;

  static auto parse_file(std::span<token> tokens) -> parse_tree {
    auto self = my_parser{};
    self.tokens_ = tokens;
    self.next_ = self.tokens_.data();
    return self.parse_file();
  }

private:
  auto parse_file() -> parse_tree {
    auto end = tokens_.empty() ? 0 : tokens_.back().end;
    nodes_.emplace_back("file", 0, end);
    active_ = 0;
    parse_operator();
    return parse_tree{std::move(nodes_)};
  }

  auto accept(token_kind kind) -> bool {
    consume_trivia();
    return accept_direct(kind);
  }

  auto accept_direct(token_kind kind) -> bool {
    if (next_ != tokens_.data() + tokens_.size()) {
      if (kind == next_->kind) {
        ++next_;
        return true;
      }
    }
    return false;
  }

  void parse_operator() {
    consume_trivia();
    active_ = append_child("operator");
    if (accept(identifier)) {
      append_child("operator_name");
    }
  }

  void consume_trivia() {
    while (accept_direct(line_comment) || accept_direct(delim_comment)
           || accept_direct(whitespace)) {
      append_child(std::string{to_string(last().kind)});
    }
  }

  auto append_child(std::string kind) -> size_t {
    auto parent = active_;
    auto index = nodes_.size();
    nodes_.push_back(parse_tree::node{std::move(kind)});
    if (not nodes_[parent].first_child) {
      nodes_[parent].first_child = index;
    } else {
      auto rightmost_child = nodes_[parent].first_child;
      while (nodes_[rightmost_child].right_sibling) {
        rightmost_child = nodes_[rightmost_child].right_sibling;
      }
      nodes_[rightmost_child].right_sibling = index;
    }
    return index;
  }

  auto last() -> token& {
    TENZIR_ASSERT(next_ != tokens_.data());
    return *(next_ - 1);
  }

  my_parser() = default;

  size_t active_{};
  token* next_{};
  std::span<token> tokens_;
  std::vector<parse_tree::node> nodes_;
};

auto parse(std::span<token> tokens) -> parse_tree {
  return my_parser::parse_file(tokens);
}
#else
auto parse(std::span<token> tokens) -> parse_tree {
  return {};
}
#endif

} // namespace tenzir::tql
