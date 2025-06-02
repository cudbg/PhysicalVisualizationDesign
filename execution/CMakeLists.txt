# Use project versions cmake --help-policy CMP0048
if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif(POLICY CMP0048)

cmake_minimum_required(VERSION 3.20)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS -std=c++20)

project(PVDExec VERSION 0.1)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -g")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# ---------------------------------------------------------------------------
# Ccache

if(EMSCRIPTEN AND EXISTS "$ENV{EMSDK}/ccache/git-emscripten_64bit/bin")
  find_program(CCACHE_PROGRAM ccache
               HINTS $ENV{EMSDK}/ccache/git-emscripten_64bit/bin)
else()
  find_program(CCACHE_PROGRAM ccache)
endif()

if(CCACHE_PROGRAM)
  set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()
message(STATUS "EMSDK=$ENV{EMSDK}")
message(STATUS "CCACHE_PROGRAM=${CCACHE_PROGRAM}")

# ---------------------------------------------------------------------------
# Threads

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
set(THREAD_LIBS Threads::Threads)

# ---------------------------------------------------------------------------
# Emscripten settings

if(EMSCRIPTEN)
  # WITH_WASM_SIMD
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msimd128")

  # WITH_WASM_BULK_MEMORY
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mbulk-memory")

  # WITH_WASM_THREAD
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -sUSE_PTHREADS=1")
  set(WASM_LINK_FLAGS "${WASM_LINK_FLAGS} -sUSE_PTHREADS=1 -sPTHREAD_POOL_SIZE=128")
endif()

# ---------------------------------------------------------------------------
# Parallelism

include(ProcessorCount)
ProcessorCount(NPROCS)
set(CMAKE_BUILD_PARALLEL_LEVEL ${NPROCS})

# ---------------------------------------------------------------------------
# Source code

include_directories(
	"${CMAKE_SOURCE_DIR}/share/include"
	"${CMAKE_SOURCE_DIR}/server/include"
	"${CMAKE_SOURCE_DIR}/client/wasm/include"
	)

file(GLOB PVD_SHARE_SOURCE
        "${CMAKE_SOURCE_DIR}/share/include/*.h"
        "${CMAKE_SOURCE_DIR}/share/src/*.c"
        "${CMAKE_SOURCE_DIR}/share/src/*.cpp"
        "${CMAKE_SOURCE_DIR}/share/src/*.cc"
        "${CMAKE_SOURCE_DIR}/share/src/*/*.c"
        "${CMAKE_SOURCE_DIR}/share/src/*/*.cpp"
        "${CMAKE_SOURCE_DIR}/share/src/*/*.cc"
        )

file(GLOB PVD_SERVER_SOURCE
        "${CMAKE_SOURCE_DIR}/server/include/*.h"
        "${CMAKE_SOURCE_DIR}/server/include/*.hpp"
        "${CMAKE_SOURCE_DIR}/server/include/*/*.hpp"
        "${CMAKE_SOURCE_DIR}/server/include/*/*/*.hpp"
        "${CMAKE_SOURCE_DIR}/server/include/*/*/*/*.hpp"
        "${CMAKE_SOURCE_DIR}/server/src/*.c"
        "${CMAKE_SOURCE_DIR}/server/src/*.cpp"
        "${CMAKE_SOURCE_DIR}/server/src/*.cc"
        "${CMAKE_SOURCE_DIR}/server/src/*/*.c"
        "${CMAKE_SOURCE_DIR}/server/src/*/*.cpp"
        "${CMAKE_SOURCE_DIR}/server/src/*/*.cc"
        )

file(GLOB PVD_CLIENT_SOURCE
        "${CMAKE_SOURCE_DIR}/client/wasm/include/*.h"
        "${CMAKE_SOURCE_DIR}/client/wasm/src/*.c"
        "${CMAKE_SOURCE_DIR}/client/wasm/src/*.cpp"
        "${CMAKE_SOURCE_DIR}/client/wasm/src/*.cc"
        "${CMAKE_SOURCE_DIR}/client/wasm/src/*/*.c"
        "${CMAKE_SOURCE_DIR}/client/wasm/src/*/*.cpp"
        "${CMAKE_SOURCE_DIR}/client/wasm/src/*/*.cc"
        )

include("${CMAKE_SOURCE_DIR}/third_party/arrow.cmake")

# ---------------------------------------------------------------------------
# Server 

if (NOT EMSCRIPTEN)
  include_directories("$ENV{EMSDK}/upstream/emscripten/system/include/")
  link_directories("${CMAKE_SOURCE_DIR}/server/lib/")
  add_executable(pvd_server ${PVD_SHARE_SOURCE} ${PVD_SERVER_SOURCE})
  target_link_libraries(pvd_server arrow_acero arrow duckdb ${THREAD_LIBS})
endif()

# ---------------------------------------------------------------------------
# Emscripten

if (EMSCRIPTEN)
  set(CMAKE_EXECUTABLE_SUFFIX ".js")
  add_executable(pvd_wasm ${PVD_SHARE_SOURCE} ${PVD_CLIENT_SOURCE})
  target_link_libraries(pvd_wasm arrow_acero arrow embind websocket.js ${THREAD_LIBS})
  message(STATUS "WASM_LINK_FLAGS=${WASM_LINK_FLAGS}")
  set_target_properties(
    pvd_wasm
    PROPERTIES
      LINK_FLAGS
      "${WASM_LINK_FLAGS} -sALLOW_MEMORY_GROWTH -sMAXIMUM_MEMORY=4GB")
     #"${WASM_LINK_FLAGS} -sASSERTIONS -sALLOW_MEMORY_GROWTH -sMAXIMUM_MEMORY=4GB -sSTACK_SIZE=5MB -sSTACK_OVERFLOW_CHECK -sNO_DISABLE_EXCEPTION_CATCHING")
endif()
