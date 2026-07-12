#include <nats_asio/nats_asio.hpp>

int main() {
    nats_asio::connect_config config;
    return config.address.empty() ? 1 : 0;
}
