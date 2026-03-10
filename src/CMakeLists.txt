ADD_SUBDIRECTORY(common)

# observer 需要 Libevent + jsoncpp；oblsm 需要 jsoncpp。Windows 上未安装时跳过，便于 CLion 仅做编辑/部分构建，完整构建请在 Docker 中执行。
FIND_PACKAGE(Libevent CONFIG QUIET)
FIND_PACKAGE(jsoncpp CONFIG QUIET)
IF(jsoncpp_FOUND)
  ADD_SUBDIRECTORY(oblsm)
ENDIF()
IF(Libevent_FOUND AND jsoncpp_FOUND)
  ADD_SUBDIRECTORY(observer)
ELSE()
  MESSAGE(STATUS "Skipping observer (Libevent or jsoncpp not found; use Docker for full build)")
ENDIF()

ADD_SUBDIRECTORY(obclient)
IF (WITH_CPPLINGS)
    ADD_SUBDIRECTORY(cpplings)
endif()

if (WITH_MEMTRACER)
    ADD_SUBDIRECTORY(memtracer)
endif()
