cmake_minimum_required(VERSION 3.20)
project(fleetright VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
option(FLEETRIGHT_ENABLE_SANITIZERS "Enable address and undefined behavior sanitizers" OFF)
option(FLEETRIGHT_BUILD_FUZZERS "Build libFuzzer smoke targets with Clang" OFF)

find_package(Protobuf REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(GTest REQUIRED)

set(FLEETRIGHT_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(PROTO_INPUTS
  "${PROJECT_SOURCE_DIR}/proto/fleetright/v1/model.proto"
  "${PROJECT_SOURCE_DIR}/proto/fleetright/v1/optimizer.proto"
  "${PROJECT_SOURCE_DIR}/proto/fleetright/v2/evacuation.proto"
)
set(PROTO_SOURCES
  "${FLEETRIGHT_GENERATED_DIR}/fleetright/v1/model.pb.cc"
  "${FLEETRIGHT_GENERATED_DIR}/fleetright/v1/optimizer.pb.cc"
  "${FLEETRIGHT_GENERATED_DIR}/fleetright/v2/evacuation.pb.cc"
)
set(PROTO_HEADERS
  "${FLEETRIGHT_GENERATED_DIR}/fleetright/v1/model.pb.h"
  "${FLEETRIGHT_GENERATED_DIR}/fleetright/v1/optimizer.pb.h"
  "${FLEETRIGHT_GENERATED_DIR}/fleetright/v2/evacuation.pb.h"
)
add_custom_command(
  OUTPUT ${PROTO_SOURCES} ${PROTO_HEADERS}
  COMMAND ${CMAKE_COMMAND} -E make_directory "${FLEETRIGHT_GENERATED_DIR}"
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
          --proto_path=${PROJECT_SOURCE_DIR}/proto
          --cpp_out=${FLEETRIGHT_GENERATED_DIR}
          "${PROJECT_SOURCE_DIR}/proto/fleetright/v1/model.proto"
          "${PROJECT_SOURCE_DIR}/proto/fleetright/v1/optimizer.proto"
          "${PROJECT_SOURCE_DIR}/proto/fleetright/v2/evacuation.proto"
  DEPENDS ${PROTO_INPUTS}
  VERBATIM
)
add_library(fleetright_proto ${PROTO_SOURCES} ${PROTO_HEADERS})
target_include_directories(fleetright_proto PUBLIC "${FLEETRIGHT_GENERATED_DIR}")
target_link_libraries(fleetright_proto PUBLIC protobuf::libprotobuf)

add_library(fleetright_core
  cpp/src/planner.cpp
  cpp/src/verifier.cpp
  cpp/src/planner_v2.cpp
  cpp/src/verifier_v2.cpp
)
target_include_directories(fleetright_core PUBLIC cpp/include)
target_link_libraries(fleetright_core PUBLIC fleetright_proto OpenSSL::Crypto)
target_compile_options(fleetright_core PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
target_compile_definitions(fleetright_core PUBLIC
  FLEETRIGHT_BUILD_ID="${PROJECT_VERSION}-${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}-${CMAKE_BUILD_TYPE}")

add_executable(fleetright-plan cpp/src/planner_main.cpp)
target_link_libraries(fleetright-plan PRIVATE fleetright_core)
add_executable(fleetright-verify cpp/src/verifier_main.cpp)
target_link_libraries(fleetright-verify PRIVATE fleetright_core)
add_executable(fleetright-core-benchmark cpp/benchmarks/core_benchmark.cpp)
target_link_libraries(fleetright-core-benchmark PRIVATE fleetright_core)

if(FLEETRIGHT_ENABLE_SANITIZERS)
  foreach(target fleetright_proto fleetright_core fleetright-plan fleetright-verify
                 fleetright-core-benchmark)
    target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(${target} PRIVATE -fsanitize=address,undefined)
  endforeach()
endif()

enable_testing()
add_executable(fleetright_cpp_tests cpp/tests/core_test.cpp)
target_link_libraries(fleetright_cpp_tests PRIVATE fleetright_core GTest::gtest_main)
target_compile_options(fleetright_cpp_tests PRIVATE -Wall -Wextra -Wpedantic)
include(GoogleTest)
gtest_discover_tests(fleetright_cpp_tests)

if(FLEETRIGHT_ENABLE_SANITIZERS)
  target_compile_options(fleetright_cpp_tests PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(fleetright_cpp_tests PRIVATE -fsanitize=address,undefined)
endif()

if(FLEETRIGHT_BUILD_FUZZERS)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "FLEETRIGHT_BUILD_FUZZERS requires Clang")
  endif()
  target_compile_options(fleetright_proto PRIVATE -fsanitize=fuzzer-no-link,address,undefined)
  target_compile_options(fleetright_core PRIVATE -fsanitize=fuzzer-no-link,address,undefined)
  foreach(target fleetright-plan fleetright-verify fleetright-core-benchmark fleetright_cpp_tests)
    target_link_options(${target} PRIVATE -fsanitize=fuzzer-no-link,address,undefined)
  endforeach()
  add_executable(resource_fuzz cpp/fuzz/resource_fuzz.cpp)
  target_link_libraries(resource_fuzz PRIVATE fleetright_core)
  target_compile_options(resource_fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
  target_link_options(resource_fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
endif()
