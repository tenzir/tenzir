add_library(example example.cpp)
target_link_libraries(example libvast)
install(TARGETS example DESTINATION lib) # wrong
