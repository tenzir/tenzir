//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/string.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::string_view_literals;

constexpr static auto npos = std::string_view::npos;

TEST("quoting policy default construction") {
  const auto q = detail::quoting_escaping_policy{};
  CHECK(q.is_quote_character('\''));
  CHECK(q.is_quote_character('\"'));
  CHECK_EQUAL(q.backslashes_escape, true);
  CHECK_EQUAL(q.doubled_quotes_escape, false);
}

TEST("finding quotes no escaping") {
  const auto q = detail::quoting_escaping_policy{
    .backslashes_escape = false,
    .doubled_quotes_escape = false,
  };
  {
    constexpr auto text = R"(text)"sv;
    CHECK_EQUAL(q.find_opening_quote(text), npos);
  }
  {
    constexpr auto text = R"("text")"sv;
    CHECK_EQUAL(q.find_opening_quote(text), size_t{0});
    CHECK_EQUAL(q.find_closing_quote(text, 0), text.size() - 1);
    for (size_t i = 1; i < text.size() - 1; ++i) {
      CHECK(q.is_inside_of_quotes(text, i));
    }
  }
  {
    constexpr auto text = R"("\"text\"")"sv;
    CHECK_EQUAL(q.find_opening_quote(text), size_t{0});
    CHECK_EQUAL(q.find_closing_quote(text, 0), size_t{2});
    CHECK(q.is_inside_of_quotes(text, 1));
  }
}

TEST("finding quotes basic escaping") {
  const auto q = detail::quoting_escaping_policy{};
  {
    constexpr auto text = R"(text)"sv;
    CHECK_EQUAL(q.find_opening_quote(text), npos);
  }
  {
    constexpr auto text = R"("text")"sv;
    CHECK_EQUAL(q.find_opening_quote(text), size_t{0});
    CHECK_EQUAL(q.find_closing_quote(text, 0), text.size() - 1);
    for (size_t i = 1; i < text.size() - 1; ++i) {
      CHECK(q.is_inside_of_quotes(text, i));
    }
  }
  {
    constexpr auto text = R"("\"text\"")"sv;
    CHECK_EQUAL(q.find_opening_quote(text), size_t{0});
    CHECK_EQUAL(q.find_closing_quote(text, 0), text.size() - 1);
    for (size_t i = 1; i < text.size() - 1; ++i) {
      CHECK(q.is_inside_of_quotes(text, i));
    }
  }
}

TEST("finding quotes doubled escaping") {
  const auto q = detail::quoting_escaping_policy{
    .doubled_quotes_escape = true,
  };
  {
    constexpr auto text = R"(text)"sv;
    CHECK_EQUAL(q.find_opening_quote(text), npos);
  }
  {
    constexpr auto text = R"("text")"sv;
    CHECK_EQUAL(q.find_opening_quote(text), size_t{0});
    CHECK_EQUAL(q.find_closing_quote(text, 0), text.size() - 1);
    for (size_t i = 1; i < text.size() - 1; ++i) {
      CHECK(q.is_inside_of_quotes(text, i));
    }
  }
  {
    constexpr auto text = R"("""text""")"sv;
    CHECK_EQUAL(q.find_opening_quote(text), size_t{0});
    CHECK_EQUAL(q.find_closing_quote(text, 0), text.size() - 1);
    for (size_t i = 1; i < text.size() - 1; ++i) {
      CHECK(q.is_inside_of_quotes(text, i));
    }
  }
}

TEST("finding nonquoted characters basic escaping") {
  const auto q = detail::quoting_escaping_policy{};
  {
    constexpr auto text = R"(text)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, npos);
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"(te$xt)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, text.find('$'));
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"("te$xt")"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, npos);
    CHECK(not q.is_inside_of_quotes(text, pos));
    const auto pos2 = text.find('$');
    CHECK(q.is_inside_of_quotes(text, pos2));
  }
  {
    constexpr auto text = R"("te$xt"$)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, text.size() - 1);
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"("te$xt\"$)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, text.find('$'));
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"("text"$)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, text.size() - 1);
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"("text$"%)"sv;
    const auto pos = q.find_first_of_not_in_quotes(text, "$%", 0);
    CHECK_EQUAL(pos, text.find('%'));
    CHECK_EQUAL(text[pos], '%');
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
}

TEST("finding nonquoted characters doubled escaping") {
  const auto q = detail::quoting_escaping_policy{
    .doubled_quotes_escape = true,
  };
  {
    constexpr auto text = R"(text)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, npos);
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"(te$xt)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, text.find('$'));
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"("""te$xt")"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, npos);
    CHECK(not q.is_inside_of_quotes(text, pos));
    const auto pos2 = text.find('$');
    CHECK(q.is_inside_of_quotes(text, pos2));
  }
  {
    constexpr auto text = R"("""te$xt"""$)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, text.size() - 1);
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"("te$xt""$)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, text.find('$'));
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"("text"$)"sv;
    const auto pos = q.find_not_in_quotes(text, '$');
    CHECK_EQUAL(pos, text.size() - 1);
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
  {
    constexpr auto text = R"("text$"""%)"sv;
    const auto pos = q.find_first_of_not_in_quotes(text, "$%", 0);
    CHECK_EQUAL(pos, text.find('%'));
    CHECK_EQUAL(text[pos], '%');
    CHECK(not q.is_inside_of_quotes(text, pos));
  }
}

TEST("unquote basic quote escaping") {
  const auto q = detail::quoting_escaping_policy{};
  {
    constexpr auto text = R"(text)"sv;
    CHECK_EQUAL(q.unquote(text), text);
  }
  {
    constexpr auto text = R"("text")"sv;
    CHECK_EQUAL(q.unquote(text), text.substr(1, text.size() - 2));
  }
  {
    constexpr auto text = R"("text\")"sv;
    CHECK_EQUAL(q.unquote(text), text);
  }
}

TEST("unquote doubled quote escaping") {
  const auto q = detail::quoting_escaping_policy{.doubled_quotes_escape = true};
  {
    constexpr auto text = R"(text)"sv;
    CHECK_EQUAL(q.unquote(text), text);
  }
  {
    constexpr auto text = R"("text""")"sv;
    CHECK_EQUAL(q.unquote(text), text.substr(1, text.size() - 2));
  }
  {
    constexpr auto text = R"("text"")"sv;
    CHECK_EQUAL(q.unquote(text), text);
  }
}

TEST("unquote_unescape basic quote escaping") {
  const auto q = detail::quoting_escaping_policy{};
  {
    constexpr auto text = R"(text)"sv;
    CHECK_EQUAL(q.unquote_unescape(text), text);
  }
  {
    constexpr auto text = R"(""text)"sv;
    CHECK_EQUAL(q.unquote_unescape(text), text);
  }
  {
    constexpr auto text = R"("text")"sv;
    CHECK_EQUAL(q.unquote_unescape(text), "text");
  }
  {
    constexpr auto text = R"("text\"")"sv;
    CHECK_EQUAL(q.unquote_unescape(text), R"(text")");
  }
  {
    constexpr auto text = R"("text\")"sv;
    CHECK_EQUAL(q.unquote_unescape(text), R"("text")");
  }
}

TEST("unquote_unescape basic escape sequences") {
  const auto q = detail::quoting_escaping_policy{};
  {
    constexpr auto text = R"(\x)"sv;
    CHECK_EQUAL(q.unquote_unescape(text), R"(\x)");
  }
  {
    constexpr auto text = R"(\\)"sv;
    CHECK_EQUAL(q.unquote_unescape(text), R"(\)");
  }
  {
    constexpr auto text = R"(\n)"sv;
    CHECK_EQUAL(q.unquote_unescape(text), "\n");
  }
  {
    constexpr auto text = R"(\\n)"sv;
    CHECK_EQUAL(q.unquote_unescape(text), R"(\n)");
  }
  {
    constexpr auto text = R"(\\n\\\x)"sv;
    CHECK_EQUAL(q.unquote_unescape(text), R"(\n\\x)");
  }
}

TEST("unquote_unescape doubled quote escaping") {
  const auto q = detail::quoting_escaping_policy{
    .doubled_quotes_escape = true,
  };
  {
    constexpr auto text = R"(text)"sv;
    CHECK_EQUAL(q.unquote_unescape(text), text);
  }
  {
    constexpr auto text = R"("text")"sv;
    CHECK_EQUAL(q.unquote_unescape(text), "text");
  }
  {
    constexpr auto text = R"("text""")"sv;
    CHECK_EQUAL(q.unquote_unescape(text), R"(text")");
  }
}
