set(TOOLCHAIN_PREFIX x86_64-w64-mingw32)
set (CMAKE_SYSTEM_NAME Windows)
set (CMAKE_C_COMPILER ${TOOLCHAIN_PREFIX}-gcc)
set (CMAKE_CXX_COMPILER ${TOOLCHAIN_PREFIX}-g++)
set (CMAKE_RC_COMPILER ${TOOLCHAIN_PREFIX}-windres)
set (CMAKE_Fortran_COMPILER ${TOOLCHAIN_PREFIX}-gfortran)
set (CMAKE_FIND_ROOT_PATH /usr/${TOOLCHAIN_PREFIX} /usr/${TOOLCHAIN_PREFIX})
set (CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set (CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set (CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set (CMAKE_CROSSCOMPILING_EMULATOR wine64)

include_directories(/usr/${TOOLCHAIN_PREFIX}/include)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS On CACHE BOOL "Export windows symbols")
