# nats-asio

## Overview
This is [nats-io](https://nats.io/) client writen in c++20 with use of [boost](https://www.boost.org/) [asio](https://www.boost.org/doc/libs/release/libs/asio/) and coroutines libraries.

## Requirements
For Library
```
fmt/6.2.0
spdlog/1.5.0
openssl/1.1.1d
nlohmann_json/3.9.1
```

For tests 
```
gtest/1.8.1
```
For nats tool 
```
cxxopts/2.2.1
```

## Usage of library
 - You can just copy `interface.hpp` and `impl.hpp` in you project (don't forget to include `impl.hpp` somewhere)

And then add `nats_asio/0.0.13@_/_` to dependencies. 


## Example
Please check source code of tool `samples/nats_tool.cpp`
