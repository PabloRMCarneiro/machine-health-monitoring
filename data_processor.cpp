#include <iostream>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <unistd.h>
#include <vector>
#include <sstream>
#include <string>
#include "json.hpp"
#include "mqtt/client.h"
#include <boost/asio.hpp>
#include <map>

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"
#define GRAPHITE_HOST "127.0.0.1"
#define GRAPHITE_PORT 2003

namespace asio = boost::asio;
using asio::ip::tcp;

std::string timestamp2UNIX(const std::string &timestamp)
{
    std::tm t = {};
    std::istringstream ss(timestamp);
    ss >> std::get_time(&t, "%Y-%m-%dT%H:%M:%S");
    std::time_t time_stamp = mktime(&t);
    return std::to_string(time_stamp);
}

std::string UNIX2timestamp(const std::time_t &timestamp)
{
    std::tm *ptm = std::localtime(&timestamp);
    char buffer[32];
    std::strftime(buffer, 32, "%Y-%m-%dT%H:%M:%S", ptm);
    return std::string(buffer);
}

void post_metric(const std::string &machine_id, const std::string &sensor_id, const std::string &timestamp_str, const float value)
{
    try
    {
        boost::asio::io_service io_service;

        // Resolve the host and port.
        tcp::resolver resolver(io_service);
        tcp::resolver::query query(GRAPHITE_HOST, std::to_string(GRAPHITE_PORT));
        tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

        // Create and connect the socket.
        tcp::socket socket(io_service);
        boost::asio::connect(socket, endpoint_iterator);

        // Format the metric.
        std::string metric_path = machine_id + "." + sensor_id;
        std::string message = metric_path + " " + std::to_string(value) + " " + timestamp2UNIX(timestamp_str) + "\n";

        // Send the metric to Graphite.
        boost::system::error_code ignored_error;
        boost::asio::write(socket, boost::asio::buffer(message), ignored_error);

        std::cout << "Metric sent: " << message << std::endl;
    }
    catch (std::exception &e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
}

std::map<std::pair<std::string, std::string>, std::time_t> last_sensor_activity;

void check_inactivity_and_post_alarm()
{
    std::time_t current_time = std::time(nullptr);
    for (auto &activity : last_sensor_activity)
    {
        std::time_t last_time = activity.second;
        std::string machine_id = activity.first.first;
        std::string sensor_id = activity.first.second;

        if (current_time - last_time > 10)
        {
            std::string alarm_path = machine_id + ".alarms.inactive";
            std::string message = alarm_path + " 1 " + UNIX2timestamp(current_time) + "\n";
            // Envie o alarme para o Graphite.
            post_metric(machine_id, "alarms.inactive", UNIX2timestamp(current_time), 1);
        }
    }
}

std::vector<std::string> split(const std::string &str, char delim)
{
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delim))
    {
        tokens.push_back(token);
    }
    return tokens;
}

int main(int argc, char *argv[])
{
    std::string clientId = "clientId";
    mqtt::async_client client(BROKER_ADDRESS, clientId);

    // Create an MQTT callback.
    class callback : public virtual mqtt::callback
    {
    public:
        void message_arrived(mqtt::const_message_ptr msg) override
        {
            auto j = nlohmann::json::parse(msg->get_payload());

            std::string topic = msg->get_topic();
            auto topic_parts = split(topic, '/');
            std::string machine_id = topic_parts[2];
            std::string sensor_id = topic_parts[3];

            std::string timestamp = j["timestamp"];
            float value = j["value"];
            post_metric(machine_id, sensor_id, timestamp, value);

            std::pair<std::string, std::string> machine_sensor_pair = {machine_id, sensor_id};
            last_sensor_activity[machine_sensor_pair] = std::time(nullptr);
        }
    };

    callback cb;
    client.set_callback(cb);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try
    {
        client.connect(connOpts)->wait();
        client.subscribe("/sensors/#", QOS);
    }
    catch (mqtt::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    while (true)
    {
        check_inactivity_and_post_alarm();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return EXIT_SUCCESS;
}
