# SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

# Exercise routing and argument forwarding without starting either backend.
file(MAKE_DIRECTORY "${TEST_ROOT}/bin"
     "${TEST_ROOT}/bin/${LIBEXEC_FROM_BINDIR}")
file(COPY_FILE "${TENZIR_BINARY}" "${TEST_ROOT}/bin/tenzir")
foreach (backend engine platform-cli)
  set(path "${TEST_ROOT}/bin/${LIBEXEC_FROM_BINDIR}/${backend}")
  file(WRITE "${path}" "#!/bin/sh\nprintf '%s\\n' '${backend}' \"$@\"\n")
  file(CHMOD "${path}" PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)
endforeach ()
foreach (name tenzir-node tenzir-ctl tenzir-rebuild)
  file(CREATE_LINK tenzir "${TEST_ROOT}/bin/${name}" SYMBOLIC)
endforeach ()

function (check mode name expected)
  if (mode STREQUAL "unset")
    set(environment --unset=TENZIR_UNIFIED)
  else ()
    set(environment "TENZIR_UNIFIED=${mode}")
  endif ()
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env "${environment}"
            "${TEST_ROOT}/bin/${name}" ${ARGN}
    RESULT_VARIABLE status
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  if (NOT status EQUAL 0 OR NOT output STREQUAL expected)
    message(
      FATAL_ERROR "${mode}: ${name} ${ARGN}: ${status}\n${output}${error}")
  endif ()
endfunction ()

foreach (
  mode
  unset
  ""
  0
  false
  FALSE
  FaLsE
  no
  NO
  nO)
  check("${mode}" tenzir "platform-cli\n--help\n" platform --help)
  check("${mode}" tenzir "engine\nup\n--help\n" up --help)
  check("${mode}" tenzir "engine\nrun\nfrom {x: 1}\n" run "from {x: 1}")
  check("${mode}" tenzir "engine\nunknown\n--flag\na b\n" unknown --flag "a b")
endforeach ()
foreach (mode 1 true yes arbitrary)
  check("${mode}" tenzir "engine\n--help\n" up --help)
  check("${mode}" tenzir "engine\nfrom {x: 1}\n" run "from {x: 1}")
  check("${mode}" tenzir "platform-cli\nunknown\n--flag\na b\n" unknown --flag
        "a b")
  check("${mode}" tenzir "platform-cli\nplatform\n--help\n" platform --help)
  check("${mode}" tenzir "platform-cli\n--help\n" --help)
endforeach ()
foreach (mode unset 1)
  foreach (name tenzir-node tenzir-ctl tenzir-rebuild)
    check("${mode}" "${name}" "engine\nplatform\n--help\n" platform --help)
  endforeach ()
endforeach ()

# No arguments, empty arguments, and backend exit status must survive exec.
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env TENZIR_UNIFIED=1 "${TEST_ROOT}/bin/tenzir"
  OUTPUT_VARIABLE output
  RESULT_VARIABLE status)
if (NOT status EQUAL 0 OR NOT output STREQUAL "platform-cli\n")
  message(FATAL_ERROR "No-argument dispatch failed: ${status}: ${output}")
endif ()
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env TENZIR_UNIFIED=1 "${TEST_ROOT}/bin/tenzir"
          unknown "" "a b"
  OUTPUT_VARIABLE output
  RESULT_VARIABLE status)
if (NOT status EQUAL 0 OR NOT output STREQUAL "platform-cli\nunknown\n\na b\n")
  message(FATAL_ERROR "Empty-argument dispatch failed: ${status}: ${output}")
endif ()
file(APPEND "${TEST_ROOT}/bin/${LIBEXEC_FROM_BINDIR}/platform-cli" "exit 42\n")
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env TENZIR_UNIFIED=1 "${TEST_ROOT}/bin/tenzir"
          unknown
  OUTPUT_QUIET
  RESULT_VARIABLE status)
if (NOT status EQUAL 42)
  message(FATAL_ERROR "Backend exit status was lost: ${status}")
endif ()
