# runner: python
# timeout: 60

"""Check static DuckDB dependency lookup with config packages preferred."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess

root = Path(os.environ["TENZIR_TMP_DIR"])
prefix = root / "prefix"
include = prefix / "include"
library = prefix / "lib"
include.mkdir(parents=True)
library.mkdir()
(include / "duckdb.h").write_text("")
(include / "expat.h").write_text(
    "#define XML_MAJOR_VERSION 2\n"
    "#define XML_MINOR_VERSION 8\n"
    "#define XML_MICRO_VERSION 3\n"
)
(include / "zlib.h").write_text('#define ZLIB_VERSION "1.3.2"\n')
for name in ("duckdb_static", "excel_extension", "expat", "z"):
    (library / f"lib{name}.a").touch()

# Nix's static Expat config export references a removed shared library.
# Module-mode discovery must bypass that export, even with config preferred.
expat_config = library / "cmake" / "expat"
expat_config.mkdir(parents=True)
(expat_config / "expat-config.cmake").write_text(
    'message(FATAL_ERROR "must not load the broken Expat config export")\n'
)
minizip_config = library / "cmake" / "minizip-ng"
minizip_config.mkdir()
(minizip_config / "minizip-ng-config.cmake").write_text(
    "add_library(MINIZIP::minizip-ng INTERFACE IMPORTED)\n"
)

source = root / "source"
source.mkdir()
(source / "main.c").write_text("int main(void) { return 0; }\n")
(source / "CMakeLists.txt").write_text(
    """cmake_minimum_required(VERSION 3.30)
project(duckdb_static_dependencies LANGUAGES C)
list(PREPEND CMAKE_MODULE_PATH "${TENZIR_SOURCE_DIR}/plugins/duckdb/cmake")
set(TENZIR_ENABLE_STATIC_EXECUTABLE ON)
find_package(DuckDB MODULE REQUIRED)
get_target_property(dependencies DuckDB::duckdb INTERFACE_LINK_LIBRARIES)
foreach(dependency EXPAT::EXPAT MINIZIP::minizip-ng ZLIB::ZLIB)
  if(NOT TARGET "${dependency}" OR NOT dependency IN_LIST dependencies)
    message(FATAL_ERROR "Missing static DuckDB dependency: ${dependency}")
  endif()
endforeach()
get_target_property(expat_library EXPAT::EXPAT IMPORTED_LOCATION)
if(NOT expat_library STREQUAL EXPAT_LIBRARY)
  message(FATAL_ERROR "Did not select the supplied static Expat archive")
endif()
add_executable(consumer main.c)
target_link_libraries(consumer PRIVATE DuckDB::duckdb)
"""
)
engine = Path(__file__).resolve().parents[4]
# Generation validates the imported target graph; no compilation of the
# placeholder archives is needed to exercise this configuration failure.
result = subprocess.run(
    [
        "cmake",
        "-G",
        "Ninja",
        "-S",
        str(source),
        "-B",
        str(root / "build"),
        f"-DTENZIR_SOURCE_DIR={engine}",
        f"-DCMAKE_PREFIX_PATH={prefix}",
        "-DCMAKE_FIND_PACKAGE_PREFER_CONFIG=ON",
        f"-DDuckDB_INCLUDE_DIR={include}",
        f"-DDuckDB_LIBRARY={library / 'libduckdb_static.a'}",
        f"-DEXPAT_INCLUDE_DIR={include}",
        f"-DEXPAT_LIBRARY={library / 'libexpat.a'}",
        f"-DZLIB_INCLUDE_DIR={include}",
        f"-DZLIB_LIBRARY={library / 'libz.a'}",
    ],
    text=True,
    capture_output=True,
    timeout=45,
)
assert result.returncode == 0, result.stdout + result.stderr
print("static DuckDB dependency lookup: ok")
