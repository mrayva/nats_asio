cmake_minimum_required(VERSION 3.25)

project(nats_asio 
    VERSION 0.1.0
    DESCRIPTION "Header-only C++ NATS client using standalone Asio"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(NATS_ASIO_BUILD_TESTS "Build tests" ON)
option(NATS_ASIO_BUILD_SAMPLES "Build sample programs" ON)
option(NATS_ASIO_INSTALL "Generate install target" ON)

find_program(CLANG_FORMAT_EXE NAMES "clang-format")

if(CLANG_FORMAT_EXE)
    # Globbing is generally discouraged for build sources, but acceptable for a dev-only format target.
    # We exclude the build directory explicitly.
    file(GLOB_RECURSE ALL_SOURCE_FILES 
        LIST_DIRECTORIES false
        CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp" 
        "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp"
    )

	list(FILTER ALL_SOURCE_FILES EXCLUDE REGEX ".*/build/.*")
 
    add_custom_target(format
        COMMAND ${CLANG_FORMAT_EXE} -i -style=file ${ALL_SOURCE_FILES}
        COMMENT "Formatting project files with clang-format..."
    )
else()
    message(WARNING "clang-format not found; 'format' target will not be available.")
endif()

find_package(fmt CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(simdjson CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(asio CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(libzip CONFIG REQUIRED)

# Fetch zerialize for binary format deserialization
include(FetchContent)
FetchContent_Declare(
    zerialize
    GIT_REPOSITORY https://github.com/colinator/zerialize.git
    GIT_TAG        main
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(zerialize)

add_library(nats_asio INTERFACE)
add_library(nats_asio::nats_asio ALIAS nats_asio)

set(NATS_ASIO_HEADERS
    include/nats_asio/interface.hpp
    include/nats_asio/nats_asio.hpp
    include/nats_asio/compression.hpp
    include/nats_asio/http_reader.hpp
    include/nats_asio/input_reader.hpp
)

target_sources(nats_asio INTERFACE
    FILE_SET HEADERS
    BASE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/include
    FILES ${NATS_ASIO_HEADERS}
)

target_include_directories(nats_asio 
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(nats_asio
    INTERFACE
        fmt::fmt
        nlohmann_json::nlohmann_json
        simdjson::simdjson
        spdlog::spdlog
        asio::asio
        OpenSSL::SSL
        ZLIB::ZLIB
        libzip::zip
)

target_compile_features(nats_asio INTERFACE cxx_std_20)

# Samples
if(NATS_ASIO_BUILD_SAMPLES)
    find_package(cxxopts CONFIG REQUIRED)
    find_package(mimalloc CONFIG REQUIRED)
    find_package(simdjson CONFIG REQUIRED)
    find_package(zstd CONFIG REQUIRED)

    add_executable(nats_tool samples/nats_tool.cpp)
    target_link_libraries(nats_tool
        PRIVATE
            nats_asio::nats_asio
            cxxopts::cxxopts
            mimalloc
            simdjson::simdjson
            zerialize
            $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>
    )

    set_target_properties(nats_tool PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
endif()

# Tests
if(NATS_ASIO_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    
    add_executable(parser_test tests/check1.cpp)
    target_link_libraries(parser_test 
        PRIVATE 
            nats_asio::nats_asio
            GTest::gtest_main
            GTest::gmock_main
    )
    
    set_target_properties(parser_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests
    )
    
    add_test(NAME parser_test COMMAND parser_test)
endif()

# Installation (same as before)
if(NATS_ASIO_INSTALL)
    include(GNUInstallDirs)
    include(CMakePackageConfigHelpers)
    
    install(TARGETS nats_asio
        EXPORT nats_asio-targets
        FILE_SET HEADERS
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
    
    install(EXPORT nats_asio-targets
        FILE nats_asio-targets.cmake
        NAMESPACE nats_asio::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nats_asio
    )
    
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/nats_asio-config-version.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    
    configure_package_config_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/nats_asio-config.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/nats_asio-config.cmake"
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nats_asio
    )
    
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/nats_asio-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/nats_asio-config-version.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/nats_asio
    )
endif()
