#include <iostream>
#include <string>
#include <librdkafka/rdkafkacpp.h>

int main()
{
    std::string brokers = "localhost:9092";
    std::string topicName = "orders";
    std::string groupId = "cpp-consumer-group";
    std::string errstr;

    // Create global configuration
    RdKafka::Conf* conf =
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    if (!conf)
    {
        std::cerr << "Failed to create Kafka configuration\n";
        return 1;
    }

    // Configure Kafka broker
    if (conf->set("bootstrap.servers", brokers, errstr)
        != RdKafka::Conf::CONF_OK)
    {
        std::cerr << "Failed to set broker: "
                  << errstr << std::endl;

        delete conf;
        return 1;
    }

    // Configure consumer group
    if (conf->set("group.id", groupId, errstr)
        != RdKafka::Conf::CONF_OK)
    {
        std::cerr << "Failed to set group ID: "
                  << errstr << std::endl;

        delete conf;
        return 1;
    }

    // Read messages from beginning if no previous offset exists
    if (conf->set("auto.offset.reset", "earliest", errstr)
        != RdKafka::Conf::CONF_OK)
    {
        std::cerr << "Failed to set offset: "
                  << errstr << std::endl;

        delete conf;
        return 1;
    }

    // Create Kafka consumer
    RdKafka::KafkaConsumer* consumer =
        RdKafka::KafkaConsumer::create(conf, errstr);

    if (!consumer)
    {
        std::cerr << "Failed to create consumer: "
                  << errstr << std::endl;

        delete conf;
        return 1;
    }

    delete conf;

    // Subscribe to the orders topic
    RdKafka::ErrorCode result =
        consumer->subscribe({topicName});

    if (result != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "Failed to subscribe: "
                  << RdKafka::err2str(result)
                  << std::endl;

        consumer->close();
        delete consumer;
        return 1;
    }

    std::cout << "Kafka consumer started." << std::endl;
    std::cout << "Waiting for messages..." << std::endl;

    while (true)
    {
        RdKafka::Message* message =
            consumer->consume(1000);

        if (message->err() == RdKafka::ERR_NO_ERROR)
        {
            std::string receivedMessage(
                static_cast<char*>(message->payload()),
                message->len()
            );

            std::cout << "Received: "
                      << receivedMessage
                      << std::endl;
        }
        else if (message->err() == RdKafka::ERR__TIMED_OUT)
        {
            // No message arrived within 1 second.
            continue;
        }
        else if (message->err() == RdKafka::ERR__PARTITION_EOF)
        {
            continue;
        }
        else
        {
            std::cerr << "Kafka error: "
                      << message->errstr()
                      << std::endl;
        }

        delete message;
    }

    consumer->close();
    delete consumer;

    return 0;
}