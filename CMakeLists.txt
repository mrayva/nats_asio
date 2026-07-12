cmake_minimum_required(VERSION 3.25)

set(VCPKG_OVERLAY_TRIPLETS "${CMAKE_CURRENT_SOURCE_DIR}/triplets" CACHE STRING "")

option(NATS_ASIO_BUILD_TESTS "Build tests" ON)
option(NATS_ASIO_BUILD_SAMPLES "Build sample programs" ON)
option(NATS_ASIO_INSTALL "Generate install target" ON)

# vcpkg resolves manifest features during project(), so select optional
# dependencies before the toolchain performs manifest installation.
if(NATS_ASIO_BUILD_SAMPLES)
    list(APPEND VCPKG_MANIFEST_FEATURES samples)
endif()
if(NATS_ASIO_BUILD_TESTS)
    list(APPEND VCPKG_MANIFEST_FEATURES tests)
endif()
list(REMOVE_DUPLICATES VCPKG_MANIFEST_FEATURES)

project(nats_asio
    VERSION 0.1.0
    DESCRIPTION "Header-only C++ NATS client using standalone Asio"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_program(CLANG_FORMAT_EXE NAMES "clang-format")

if(CLANG_FORMAT_EXE)
    # Restrict the dev-only format target to sources owned by this project.
    file(GLOB_RECURSE ALL_SOURCE_FILES
        LIST_DIRECTORIES false
        CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/samples/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/samples/*.hpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp"
    )

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
find_package(magic_enum CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(libzip CONFIG REQUIRED)
find_path(CONCURRENTQUEUE_INCLUDE_DIR NAMES concurrentqueue/moodycamel/concurrentqueue.h REQUIRED)
find_path(GTL_INCLUDE_DIR NAMES gtl/lru_cache.hpp REQUIRED)
find_path(STRINGZILLA_INCLUDE_DIR NAMES stringzilla/stringzilla.hpp REQUIRED)

add_library(nats_asio INTERFACE)
add_library(nats_asio::nats_asio ALIAS nats_asio)

set(NATS_ASIO_HEADERS
    include/nats_asio/interface.hpp
    include/nats_asio/nats_asio.hpp
    include/nats_asio/compression.hpp
    include/nats_asio/decompression_reader.hpp
    include/nats_asio/http_reader.hpp
    include/nats_asio/input_reader.hpp
    include/nats_asio/multi_file_reader.hpp
    include/nats_asio/zip_extractor.hpp
)

target_sources(nats_asio INTERFACE
    FILE_SET HEADERS
    BASE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/include
    FILES ${NATS_ASIO_HEADERS}
)

target_include_directories(nats_asio 
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CONCURRENTQUEUE_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${GTL_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${STRINGZILLA_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(nats_asio
    INTERFACE
        fmt::fmt
        nlohmann_json::nlohmann_json
        simdjson::simdjson
        spdlog::spdlog
        asio::asio
        magic_enum::magic_enum
        zstd::libzstd
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

    # zerialize is only used by nats_tool for binary format conversion.
    include(FetchContent)
    set(JSONCONS_BUILD_TESTS OFF CACHE BOOL "Disable jsoncons tests when vendored" FORCE)
    FetchContent_Declare(
        zerialize
        GIT_REPOSITORY https://github.com/colinator/zerialize.git
        GIT_TAG        aedaaf2e585dbc0a7f239bca33fd2a6595ed47df
    )
    FetchContent_MakeAvailable(zerialize)

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
    target_compile_definitions(nats_tool PRIVATE FLATBUFFERS_LOCALE_INDEPENDENT=0)

    set_target_properties(nats_tool PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
endif()

# Tests
if(NATS_ASIO_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    
    add_executable(parser_test
        tests/check1.cpp
        tests/check2.cpp
    )
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

    add_executable(public_headers_test tests/public_headers.cpp)
    target_link_libraries(public_headers_test PRIVATE nats_asio::nats_asio)
    set_target_properties(public_headers_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests
    )
    add_test(NAME public_headers_test COMMAND public_headers_test)

    if(TARGET nats_tool)
        add_test(
            NAME nats_tool_cli_validation
            COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/tests/nats_tool_cli_validation.sh
                    $<TARGET_FILE:nats_tool>
        )
    endif()
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

if(NATS_ASIO_BUILD_TESTS AND NATS_ASIO_INSTALL)
    set(NATS_ASIO_CONSUMER_DEPENDENCY_PREFIX "")
    if(DEFINED VCPKG_INSTALLED_DIR AND DEFINED VCPKG_TARGET_TRIPLET)
        set(NATS_ASIO_CONSUMER_DEPENDENCY_PREFIX
            "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}")
    endif()
    add_test(
        NAME installed_package_consumer
        COMMAND ${CMAKE_COMMAND}
            -DPROJECT_BUILD_DIR=${CMAKE_BINARY_DIR}
            -DCONSUMER_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/tests/install_consumer
            -DTEST_ROOT=${CMAKE_BINARY_DIR}/installed-package-test
            -DDEPENDENCY_PREFIX=${NATS_ASIO_CONSUMER_DEPENDENCY_PREFIX}
            -DINSTALL_CMAKE_DIR=${CMAKE_INSTALL_LIBDIR}/cmake/nats_asio
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/run_installed_consumer.cmake
    )
endif()
