include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(vast)

add_executable(vast main.cc)
set_target_properties(vast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(vast libvast)
install(TARGETS vast DESTINATION bin)

#
# VAST wrapper - for convenience during development.
#

get_target_property(VAST_BINARY_LOCATION vast LOCATION)

if (UNIX)
    if (APPLE)
        set(VAST_SYSTEM_ENV DYLD_LIBRARY_PATH)
    else ()
        set(VAST_SYSTEM_ENV LD_LIBRARY_PATH)
    endif ()
else ()
    message(FATAL_ERROR "Supporting currently only UNIX, sorry...")
endif ()

# Only inserts ${value} into the list ${var} if it is not already contained.
macro (list_unique_insert var value)
    list(FIND ${var} ${value} contained)
    if (${contained} EQUAL -1)
        LIST(APPEND ${var} ${value})
    endif ()
endmacro ()

# A list of paths that is used to build the export string.
set(path_list)
foreach(i ${Boost_LIBRARIES} ${LIBCPPA_LIBRARIES} ${Broccoli_LIBRARIES})
    get_filename_component(dir ${i} PATH)
    if (dir)
        list_unique_insert(path_list ${dir})
    endif ()
endforeach ()

# Build the final string in the shell script.
list(LENGTH path_list path_list_length)
if (path_list_length GREATER 0)
    list(GET path_list 0 first)
    list(REMOVE_AT path_list 0)
    set(VAST_LIBRARY_PATH "${first}")
    foreach(p ${path_list})
        set(VAST_LIBRARY_PATH "${VAST_LIBRARY_PATH}:${p}")
    endforeach ()
else ()
    set(VAST_LIBRARY_PATH "")
endif()

# Copy the wrapper over to the build directory.
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/vast.in
    ${CMAKE_BINARY_DIR}/vast.sh
    @ONLY)
