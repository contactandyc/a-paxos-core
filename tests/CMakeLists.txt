# SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

# CMakeLists.txt for tests
cmake_minimum_required(VERSION 3.20)

project(a_paxos_core_tests LANGUAGES C)

if(CMAKE_PREFIX_PATH)
  include_directories("${CMAKE_PREFIX_PATH}/include")
  link_directories("${CMAKE_PREFIX_PATH}/lib")
endif()

set(A_BUILD_VARIANT "debug" CACHE STRING
    "Variant to link via a_paxos_core::a_paxos_core (debug|memory|static|shared)")
set_property(CACHE A_BUILD_VARIANT PROPERTY STRINGS debug memory static shared)

option(A_ENABLE_COVERAGE "Enable code coverage instrumentation" OFF)

# ---- Determine compiler flags based on the variant ----
if(MSVC)
  set(_A_DEBUG_OPTS /Zi /Od)
  set(_A_RELEASE_OPTS /O2 /DNDEBUG)
else()
  set(_A_DEBUG_OPTS -O0 -g)
  set(_A_RELEASE_OPTS -O3 -DNDEBUG)
endif()

if(A_BUILD_VARIANT STREQUAL "debug" OR A_BUILD_VARIANT STREQUAL "memory")
  set(TEST_COMPILER_OPTS ${_A_DEBUG_OPTS})
else()
  set(TEST_COMPILER_OPTS ${_A_RELEASE_OPTS})
endif()

find_library(M_LIB m)

find_package(the_macro_library CONFIG REQUIRED)

# Fallback for standalone test builds (when not included via add_subdirectory)
if(NOT TARGET a_paxos_core::a_paxos_core)
  find_package(a_paxos_core CONFIG REQUIRED)
endif()

set(TEST_EXECUTABLES "")

# ---- Targets ----
# ==============================================================================
# test_paxos_catchup Target (Standard Test)
# ==============================================================================
add_executable(test_paxos_catchup
  src/test_paxos_catchup.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_acceptor.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_log.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_proposer.c
)

target_include_directories(test_paxos_catchup BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_paxos_catchup)

set_target_properties(test_paxos_catchup PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_paxos_catchup PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_paxos_catchup PRIVATE a_paxos_core::a_paxos_core)

if(M_LIB)
  target_link_libraries(test_paxos_catchup PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_paxos_catchup PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_paxos_catchup PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_paxos_catchup PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_paxos_catchup PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_paxos_catchup PRIVATE -O0 -g --coverage)
    target_link_options(test_paxos_catchup PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_paxos_catchup COMMAND $<TARGET_FILE:test_paxos_catchup>)
# ==============================================================================
# test_paxos_memory Target (Standard Test)
# ==============================================================================
add_executable(test_paxos_memory
  src/test_paxos_memory.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_acceptor.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_log.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_proposer.c
)

target_include_directories(test_paxos_memory BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_paxos_memory)

set_target_properties(test_paxos_memory PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_paxos_memory PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_paxos_memory PRIVATE a_paxos_core::a_paxos_core)

if(M_LIB)
  target_link_libraries(test_paxos_memory PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_paxos_memory PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_paxos_memory PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_paxos_memory PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_paxos_memory PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_paxos_memory PRIVATE -O0 -g --coverage)
    target_link_options(test_paxos_memory PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_paxos_memory COMMAND $<TARGET_FILE:test_paxos_memory>)
# ==============================================================================
# test_paxos_phase1 Target (Standard Test)
# ==============================================================================
add_executable(test_paxos_phase1
  src/test_paxos_phase1.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_acceptor.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_log.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_proposer.c
)

target_include_directories(test_paxos_phase1 BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_paxos_phase1)

set_target_properties(test_paxos_phase1 PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_paxos_phase1 PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_paxos_phase1 PRIVATE a_paxos_core::a_paxos_core)

if(M_LIB)
  target_link_libraries(test_paxos_phase1 PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_paxos_phase1 PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_paxos_phase1 PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_paxos_phase1 PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_paxos_phase1 PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_paxos_phase1 PRIVATE -O0 -g --coverage)
    target_link_options(test_paxos_phase1 PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_paxos_phase1 COMMAND $<TARGET_FILE:test_paxos_phase1>)
# ==============================================================================
# test_paxos_phase2 Target (Standard Test)
# ==============================================================================
add_executable(test_paxos_phase2
  src/test_paxos_phase2.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_acceptor.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_log.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_proposer.c
)

target_include_directories(test_paxos_phase2 BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_paxos_phase2)

set_target_properties(test_paxos_phase2 PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_paxos_phase2 PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_paxos_phase2 PRIVATE a_paxos_core::a_paxos_core)

if(M_LIB)
  target_link_libraries(test_paxos_phase2 PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_paxos_phase2 PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_paxos_phase2 PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_paxos_phase2 PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_paxos_phase2 PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_paxos_phase2 PRIVATE -O0 -g --coverage)
    target_link_options(test_paxos_phase2 PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_paxos_phase2 COMMAND $<TARGET_FILE:test_paxos_phase2>)
# ==============================================================================
# test_paxos_reads Target (Standard Test)
# ==============================================================================
add_executable(test_paxos_reads
  src/test_paxos_reads.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_acceptor.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_log.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_proposer.c
)

target_include_directories(test_paxos_reads BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_paxos_reads)

set_target_properties(test_paxos_reads PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_paxos_reads PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_paxos_reads PRIVATE a_paxos_core::a_paxos_core)

if(M_LIB)
  target_link_libraries(test_paxos_reads PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_paxos_reads PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_paxos_reads PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_paxos_reads PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_paxos_reads PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_paxos_reads PRIVATE -O0 -g --coverage)
    target_link_options(test_paxos_reads PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_paxos_reads COMMAND $<TARGET_FILE:test_paxos_reads>)
# ==============================================================================
# test_paxos_snapshot Target (Standard Test)
# ==============================================================================
add_executable(test_paxos_snapshot
  src/test_paxos_snapshot.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_acceptor.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_log.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/paxos_proposer.c
)

target_include_directories(test_paxos_snapshot BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_paxos_snapshot)

set_target_properties(test_paxos_snapshot PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_paxos_snapshot PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_paxos_snapshot PRIVATE a_paxos_core::a_paxos_core)

if(M_LIB)
  target_link_libraries(test_paxos_snapshot PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_paxos_snapshot PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_paxos_snapshot PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_paxos_snapshot PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_paxos_snapshot PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_paxos_snapshot PRIVATE -O0 -g --coverage)
    target_link_options(test_paxos_snapshot PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_paxos_snapshot COMMAND $<TARGET_FILE:test_paxos_snapshot>)

enable_testing()

# ---- Coverage aggregation ----
add_custom_target(coverage_report COMMENT "Generate coverage report")

if(A_ENABLE_COVERAGE AND CMAKE_C_COMPILER_ID MATCHES "Clang")
  include(ProcessorCount)
  ProcessorCount(NPROC)
  if(NPROC EQUAL 0)
    set(NPROC 4)
  endif()

  add_custom_command(TARGET coverage_report PRE_BUILD
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -j ${NPROC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running tests with coverage instrumentation"
  )

  find_program(LLVM_PROFDATA_EXECUTABLE llvm-profdata)
  find_program(LLVM_COV_EXECUTABLE llvm-cov)
  if(LLVM_PROFDATA_EXECUTABLE AND LLVM_COV_EXECUTABLE)
    add_custom_command(TARGET coverage_report POST_BUILD
      COMMAND ${LLVM_PROFDATA_EXECUTABLE} merge -sparse *.profraw -o default.profdata
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Merging coverage data"
    )

    # Normal compiled library path
    add_custom_command(TARGET coverage_report POST_BUILD
      COMMAND ${LLVM_COV_EXECUTABLE} show
              ../liba_paxos_core_${A_BUILD_VARIANT}.a
              -instr-profile=default.profdata
              -format=html -output-dir=coverage_html
              --ignore-filename-regex='/usr/.*'
              --show-regions --show-line-counts-or-regions
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Generated LLVM coverage report → open coverage_html/index.html"
    )
  endif()
endif()
