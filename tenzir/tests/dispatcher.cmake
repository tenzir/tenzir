# SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

# Exercise routing and argument forwarding without starting either backend.
# The stand-ins print their name, TENZIR_LIBEXEC, and their arguments. As
# scripts, they cannot see the argv[0] they were started with.
set(libexec "${TEST_ROOT}/bin/${LIBEXEC_FROM_BINDIR}")
file(MAKE_DIRECTORY "${TEST_ROOT}/bin" "${libexec}")
file(REAL_PATH "${libexec}" libexec)
file(COPY_FILE "${TENZIR_BINARY}" "${TEST_ROOT}/bin/tenzir")
foreach (backend tenzir-engine tenzir-platform)
  file(
    WRITE "${libexec}/${backend}"
    "#!/bin/sh\nprintf '%s\\n' '${backend}' \"\${TENZIR_LIBEXEC-unset}\" \"$@\"\n"
  )
  file(CHMOD "${libexec}/${backend}" PERMISSIONS OWNER_READ OWNER_WRITE
       OWNER_EXECUTE)
endforeach ()
foreach (name tenzir-node tenzir-ctl tenzir-rebuild tenzir-up)
  file(CREATE_LINK tenzir "${TEST_ROOT}/bin/${name}" SYMBOLIC)
endforeach ()

function (check mode name expected)
  if (mode STREQUAL "unset")
    set(environment --unset=TENZIR_UNIFIED)
  else ()
    set(environment "TENZIR_UNIFIED=${mode}")
  endif ()
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env --unset=TENZIR_LIBEXEC "${environment}"
            "${TEST_ROOT}/bin/${name}" ${ARGN}
    RESULT_VARIABLE status
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error)
  if (NOT status EQUAL 0 OR NOT output STREQUAL expected)
    message(
      FATAL_ERROR "${mode}: ${name} ${ARGN}: ${status}\n${output}${error}")
  endif ()
endfunction ()

set(engine "tenzir-engine\nunset\n")
set(platform "tenzir-platform\n${libexec}\n")
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
  nO
  1
  true
  yes
  arbitrary)
  check("${mode}" tenzir "${engine}run\nfrom {x: 1}\n" run "from {x: 1}")
  check("${mode}" tenzir "${engine}run\n-f\npipeline.tql\n" run -f pipeline.tql)
  check("${mode}" tenzir "${engine}run\n--\n-r\n" run -- -r)
  foreach (name tenzir-node tenzir-ctl tenzir-rebuild tenzir-up)
    check("${mode}" "${name}" "${engine}run\n-r\n" run -r)
    check("${mode}" "${name}" "${engine}platform\n--help\n" platform --help)
  endforeach ()
endforeach ()
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
  check("${mode}" tenzir "${platform}--help\n" platform --help)
  check("${mode}" tenzir "${platform}" platform)
  check("${mode}" tenzir "${engine}run\n-r\nfrom {x: 1}\n" run -r "from {x: 1}")
  check("${mode}" tenzir "${engine}up\nname\n" up name)
  check("${mode}" tenzir "${engine}--help\n" --help)
  check("${mode}" tenzir "${engine}unknown\n--flag\na b\n" unknown --flag "a b")
  check("${mode}" tenzir "${engine}")
endforeach ()
foreach (mode 1 true yes arbitrary)
  check("${mode}" tenzir "${platform}run\n-r\nfrom {x: 1}\n" run -r
        "from {x: 1}")
  check(
    "${mode}"
    tenzir
    "${platform}run\n-f\np.tql\n--remote\n"
    run
    -f
    p.tql
    --remote)
  check("${mode}" tenzir "${platform}up\nname\n-d\n" up name -d)
  check("${mode}" tenzir "${platform}down\nname\n" down name)
  check("${mode}" tenzir "${platform}--help\n" --help)
  check("${mode}" tenzir "${platform}unknown\n--flag\na b\n" unknown --flag
        "a b")
  check("${mode}" tenzir "${platform}platform\n--help\n" platform --help)
  check("${mode}" tenzir "${platform}")
endforeach ()

# A function's arguments drop empty strings, so this calls the binary directly.
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env TENZIR_UNIFIED=1 "${TEST_ROOT}/bin/tenzir"
          unknown "" "a b"
  OUTPUT_VARIABLE output
  RESULT_VARIABLE status)
if (NOT status EQUAL 0 OR NOT output STREQUAL "${platform}unknown\n\na b\n")
  message(FATAL_ERROR "Empty-argument dispatch failed: ${status}: ${output}")
endif ()

# The backend's exit status must survive exec.
file(APPEND "${libexec}/tenzir-platform" "exit 42\n")
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env TENZIR_UNIFIED=1 "${TEST_ROOT}/bin/tenzir"
          unknown
  OUTPUT_QUIET
  RESULT_VARIABLE status)
if (NOT status EQUAL 42)
  message(FATAL_ERROR "Backend exit status was lost: ${status}")
endif ()
