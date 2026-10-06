#include <iostream>
#include <string>
#include <csignal>
#include <librdkafka/rdkafkacpp.h>

int main() {

    std::string brokers = "localhost:9092";
    std::string topicName = "orders";

    std::string errstr;

    // Create Kafka configuration
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    if (conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << "Configuration error: " << errstr << std::endl;
        return 1;
    }

    // Create producer
    RdKafka::Producer* producer = RdKafka::Producer::create(conf, errstr);

    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return 1;
    }

    delete conf;

    std::cout << "Kafka Producer started..." << std::endl;
    std::cout << "Type messages. Type 'exit' to stop." << std::endl;

    while (true) {

        std::string message;

        std::cout << "> ";
        std::getline(std::cin, message);

        if (message == "exit") {
            break;
        }

        RdKafka::ErrorCode err = producer->produce(
            topicName,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(message.c_str()),
            message.size(),
            nullptr,
            0,
            0,
            nullptr
        );

        if (err != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Failed to produce: "
                      << RdKafka::err2str(err)
                      << std::endl;
        } else {
            std::cout << "Sent: " << message << std::endl;
        }

        // Process delivery events
        producer->poll(0);
    }

    producer->flush(5000);

    delete producer;

    std::cout << "Producer stopped." << std::endl;

    return 0;
}