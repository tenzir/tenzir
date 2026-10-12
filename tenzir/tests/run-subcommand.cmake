# SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

# `tenzir run` must behave exactly like `tenzir` with the same arguments.
set(pipeline "from {answer: 42} | write_ndjson")
file(MAKE_DIRECTORY "${TEST_ROOT}")
file(WRITE "${TEST_ROOT}/pipeline.tql" "${pipeline}\n")

function (tenzir result)
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env --unset=TENZIR_UNIFIED "${TENZIR_BINARY}"
            ${ARGN}
    RESULT_VARIABLE status
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  if (NOT status EQUAL 0)
    message(FATAL_ERROR "tenzir ${ARGN}: ${status}\n${output}${error}")
  endif ()
  set("${result}"
      "${output}"
      PARENT_SCOPE)
endfunction ()

function (compare)
  tenzir(expected ${ARGN})
  tenzir(actual run ${ARGN})
  if (NOT actual STREQUAL expected OR expected STREQUAL "")
    message(FATAL_ERROR "tenzir run ${ARGN}:\n${actual}\ntenzir ${ARGN}:\n"
                        "${expected}")
  endif ()
endfunction ()

compare("${pipeline}")
compare(--dump-ast "${pipeline}")
compare(-f "${TEST_ROOT}/pipeline.tql")
tenzir(help run --help)
if (NOT help MATCHES "usage: run \\[<parameters>\\]" OR NOT help MATCHES
                                                        "--dump-ast")
  message(FATAL_ERROR "Unexpected help text for tenzir run:\n${help}")
endif ()
