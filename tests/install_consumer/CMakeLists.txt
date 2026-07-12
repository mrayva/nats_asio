cmake_minimum_required(VERSION 3.25)
project(nats_asio_install_consumer LANGUAGES CXX)

find_package(nats_asio CONFIG REQUIRED)

add_executable(install_consumer main.cpp)
target_link_libraries(install_consumer PRIVATE nats_asio::nats_asio)
