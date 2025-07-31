//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/strip_leading_indentation.hpp"

#include "tenzir/test/test.hpp"

using namespace std::string_literals;
using namespace tenzir::detail;

TEST("strip indentation empty") {
  auto code = ""s;
  CHECK_EQUAL(strip_leading_indentation(std::string{code}), code);
}

TEST("no indentation - single line") {
  auto code = "pass"s;
  CHECK_EQUAL(strip_leading_indentation(std::string{code}), code);
}

TEST("no indentation - multiline") {
  auto code = R"_(
import math

def main():
    if True:
        pass
)_"s;
  CHECK_EQUAL(strip_leading_indentation(std::string{code}), code);
}

TEST("indentation - spaces") {
  auto code_indented = R"_(
        # :<
    import math

    def main():
        if True:
          pass
)_"s;

  auto code = R"_(
    # :<
import math

def main():
    if True:
      pass
)_"s;
  CHECK_EQUAL(strip_leading_indentation(std::move(code_indented)), code);
}

TEST("indentation - tabs") {
  auto code_indented = R"_(

	import math

	def main():
		if True:
			pass
		if False:
		  pass # <- Mixed tabs and spaces
)_"s;

  auto code = R"_(

import math

def main():
	if True:
		pass
	if False:
	  pass # <- Mixed tabs and spaces
)_"s;
  CHECK_EQUAL(strip_leading_indentation(std::move(code_indented)), code);
}
