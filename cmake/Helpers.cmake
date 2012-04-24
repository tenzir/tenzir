# Miscellaneous CMake helper.

# Joins a list to a string.
function(join values separator output)
    string (REPLACE ";" "${separator}" _tmp_str "${values}")
    set (${output} "${_tmp_str}" PARENT_SCOPE)
endfunction()
