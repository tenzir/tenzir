add_subdirectory(dscat)
if (BROKER_FOUND)
  add_subdirectory(zeek-to-vast)
endif ()
