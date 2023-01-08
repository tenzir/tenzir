//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/elements.hpp"

#include "tui/theme.hpp"

#include <vast/detail/narrow.hpp>

#include <ftxui/dom/table.hpp>

namespace vast::plugins::tui {

using namespace ftxui;

Element Vee() {
  Elements elements;
  auto line = [&](auto... xs) {
    elements.emplace_back(hbox(xs...));
  };
  auto c1 = [](auto x) {
    return text(x) | color(Color::Blue);
  };
  auto c2 = [](auto x) {
    return text(x) | color(Color::Cyan);
  };
  line(c1("////////////    "), c2("*************************"));
  line(c1(" ////////////    "), c2("*********************** "));
  line(c1("  ////////////    "), c2("*********************  "));
  line(c1("   ////////////    "), c2("*******************   "));
  line(c1("    ////////////    "), c2("*****************    "));
  line(c1("     ////////////         "), c2("**********     "));
  line(c1("      ////////////       "), c2("**********      "));
  line(c1("       ////////////     "), c2("**********       "));
  line(c1("        ////////////    "), c2("*********        "));
  line(c1("         ////////////    "), c2("*******         "));
  line(c1("          ////////////    "), c2("*****          "));
  line(c1("           ////////////    "), c2("***           "));
  line(c1("            ////////////    "), c2("*            "));
  line(c1("             ////////////                 "));
  line(c1("              ////////////                "));
  return vbox(elements);
}

static Element MonoVee() {
  static constexpr auto vee = {
    R"(////////////    **************************)",
    R"( ////////////    ************************ )",
    R"(  ////////////    **********************  )",
    R"(   ////////////    ********************   )",
    R"(    ////////////    ******************    )",
    R"(     ////////////         ***********     )",
    R"(      ////////////       ***********      )",
    R"(       ////////////     ***********       )",
    R"(        ////////////    **********        )",
    R"(         ////////////    ********         )",
    R"(          ////////////    ******          )",
    R"(           ////////////    ****           )",
    R"(            ////////////    **            )",
    R"(             ////////////                 )",
    R"(              ////////////                )",
  };
  Elements elements;
  for (const auto* line : vee)
    elements.emplace_back(text(line));
  return vbox(elements);
}

Element VAST() {
  static constexpr auto letters = {
    "@@@@@@        @@@@@@    @@@@@            @@@@@@@@      @@@@@@@@@@@@@@@@",
    " @@@@@@      @@@@@@    @@@@@@@        @@@@@@@@@@@@@@   @@@@@@@@@@@@@@@@",
    "  @@@@@@    @@@@@@    @@@@@@@@@      @@@@@@                 @@@@@@     ",
    "   @@@@@   @@@@@@    @@@@@ @@@@@      @@@@@@@@@@@@          @@@@@@     ",
    "    @@@@@  @@@@@    @@@@@   @@@@@       @@@@@@@@@@@@@       @@@@@@     ",
    "     @@@@@@@@@@    @@@@@@@@@@@@@@@              @@@@@@      @@@@@@     ",
    "      @@@@@@@@    @@@@@@@@@@@@@@@@@   @@@@@@   @@@@@@       @@@@@@     ",
    "       @@@@@@     @@@@@       @@@@@@    @@@@@@@@@@@@        @@@@@@     ",
  };
  Elements elements;
  for (const auto* line : letters)
    elements.emplace_back(text(line));
  return vbox(elements);
}

Table make_table(std::string key, std::string value, const record& xs) {
  std::vector<std::vector<std::string>> contents;
  contents.reserve(xs.size() + 1);
  auto header = std::vector<std::string>(2);
  header[0] = std::move(key);
  header[1] = std::move(value);
  contents.push_back(std::move(header));
  for (const auto& [k, v] : xs) {
    auto row = std::vector<std::string>(2);
    row[0] = k;
    row[1] = fmt::to_string(v);
    contents.push_back(std::move(row));
  }
  auto table = Table{std::move(contents)};
  default_theme.style_table_header(table);
  return table;
}

Table make_schema_table(const data& status) {
  using row_tuple = std::tuple<std::string, uint64_t, float>;
  std::vector<row_tuple> rows;
  if (auto xs = caf::get_if<record>(&status)) {
    if (auto i = xs->find("index"); i != xs->end()) {
      if (auto ys = caf::get_if<record>(&i->second)) {
        if (auto j = ys->find("statistics"); j != ys->end()) {
          if (auto zs = caf::get_if<record>(&j->second)) {
            if (auto k = zs->find("layouts"); k != zs->end()) {
              if (auto layouts = caf::get_if<record>(&k->second)) {
                for (auto& [name, details] : *layouts) {
                  if (auto obj = caf::get_if<record>(&details)) {
                    row_tuple row;
                    std::get<0>(row) = name;
                    if (auto cnt = obj->find("count"); cnt != obj->end())
                      if (auto n = caf::get_if<integer>(&cnt->second))
                        std::get<1>(row)
                          = detail::narrow_cast<uint64_t>(n->value);
                    if (auto perc = obj->find("percentage"); perc != obj->end())
                      if (auto frac = caf::get_if<real>(&perc->second))
                        std::get<2>(row) = *frac / 100;
                    rows.push_back(std::move(row));
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  // Sort by event count.
  std::sort(rows.begin(), rows.end(), [](const auto& xs, const auto& ys) {
    return std::get<1>(xs) > std::get<1>(ys);
  });
  // Render the data.
  std::vector<std::vector<Element>> contents;
  contents.reserve(rows.size() + 1);
  std::vector<Element> header(4);
  header[0] = text("Schema");
  header[1] = text("Events");
  header[2] = text("Percentage");
  header[3] = text("Histogram");
  contents.push_back(std::move(header));
  for (auto& [name, count, percentage] : rows) {
    std::vector<Element> row(4);
    row[0] = text(std::move(name));
    row[1] = text(fmt::format(std::locale("en_US.UTF-8"), "{:L}", count));
    row[2] = text(fmt::format("{:.1f}", percentage));
    row[3] = gauge(percentage) | size(WIDTH, EQUAL, 10);
    contents.push_back(std::move(row));
  }
  auto table = Table{std::move(contents)};
  default_theme.style_table_header(table);
  table.SelectColumns(1, 2).DecorateCells(align_right);
  return table;
}

Table make_build_configuration_table(const data& status) {
  if (const auto* xs = caf::get_if<record>(&status)) {
    if (auto i = xs->find("version"); i != xs->end()) {
      if (const auto* ys = caf::get_if<record>(&i->second)) {
        if (auto j = ys->find("Build Configuration"); j != ys->end()) {
          if (const auto* zs = caf::get_if<record>(&j->second)) {
            auto t = make_table("Option", "Value", *zs);
            default_theme.style_table_header(t);
            return t;
          }
        }
      }
    }
  }
  return {};
}

/// Creates a table that shows the VAST version details.
/// @param status An instance of a status record.
/// @returns A table of the version details fo the various components.
/// @relates make_table
Table make_version_table(const data& status) {
  if (const auto* xs = caf::get_if<record>(&status)) {
    if (auto i = xs->find("version"); i != xs->end()) {
      if (const auto* ys = caf::get_if<record>(&i->second)) {
        auto copy = *ys;
        if (auto j = copy.find("Build Configuration"); j != copy.end())
          copy.erase(j);
        auto t = make_table("Component", "Version", copy);
        default_theme.style_table_header(t);
        return t;
      }
    }
  }
  return {};
}

} // namespace vast::plugins::tui
