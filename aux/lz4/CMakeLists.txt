project(lz4)

add_library(lz4 SHARED lz4.c)
add_library(lz4hc SHARED lz4hc.c)

install(FILES lz4.h lz4hc.h DESTINATION include)
install(TARGETS lz4 lz4hc DESTINATION lib)
