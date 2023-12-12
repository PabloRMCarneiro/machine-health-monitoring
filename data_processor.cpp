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

struct SensorInfo
{
    std::string id;
    int interval;
    SensorInfo(std::string id, int interval)
        : id(id), interval(interval) {}
};

std::map<std::pair<std::string, std::string>, std::time_t> last_sensor_activity;
std::map<std::pair<std::string, std::string>, std::vector<float>> sensor_values_history;

std::vector<SensorInfo> firstMessages;
std::vector<std::string> machine_ids;

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

std::pair<float, float> calculate_mean_stddev(const std::vector<float> &data)
{
    float mean = std::accumulate(data.begin(), data.end(), 0.0) / data.size();
    float sq_sum = std::inner_product(data.begin(), data.end(), data.begin(), 0.0);
    float stddev = std::sqrt(sq_sum / data.size() - mean * mean);
    return {mean, stddev};
}

bool is_outlier(float value, const std::vector<float> &data)
{
    if (data.size() < 2)
        return false;

    auto [mean, stddev] = calculate_mean_stddev(data);
    float z_score = (value - mean) / stddev;
    return std::abs(z_score) > 3;
}

void processInitialMessage(const nlohmann::json &initialMessage)
{
    if (std::find(machine_ids.begin(), machine_ids.end(), initialMessage["machine_id"]) != machine_ids.end())
    {
        return;
    }; // se o machine_id já estiver na lista, não faz nada

    int minDataInterval = std::numeric_limits<int>::max(); // maior valor possível para int

    for (const auto &sensor : initialMessage["sensors"])
    {
        int dataInterval = sensor["data_interval"];
        if (dataInterval < minDataInterval)
        {
            minDataInterval = dataInterval;
        }
    }

    firstMessages.push_back(SensorInfo(initialMessage["machine_id"], minDataInterval));
    machine_ids.push_back(initialMessage["machine_id"]);
}

void post_metric(const std::string &machine_id, const std::string &sensor_id, const std::string &timestamp_str, const float value)
{
    if (firstMessages.empty())
    {
        return;
    }
    try
    {
        boost::asio::io_service io_service;

        tcp::resolver resolver(io_service);
        tcp::resolver::query query(GRAPHITE_HOST, std::to_string(GRAPHITE_PORT));
        tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

        tcp::socket socket(io_service);
        boost::asio::connect(socket, endpoint_iterator);

        std::string metric_path = machine_id + "." + sensor_id;
        std::string message = metric_path + " " + std::to_string(value) + " " + timestamp2UNIX(timestamp_str) + "\n";

        boost::system::error_code ignored_error;
        boost::asio::write(socket, boost::asio::buffer(message), ignored_error);

        std::cout << "Metric sent: " << message << std::endl;
    }
    catch (std::exception &e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
}

void processing_alarm_data(const int &interval)
{
    if (firstMessages.empty())
    {
        return;
    }

    std::time_t current_time = std::time(nullptr);
    for (auto &activity : last_sensor_activity)
    {
        std::pair<std::string, std::string> machine_sensor_pair = activity.first;
        std::time_t last_time = activity.second;
        std::string machine_id = activity.first.first;
        std::string sensor_id = activity.first.second;

        if (current_time - last_time >= (interval / 1000) * 10)
        {
            std::string alarm_path = machine_id + ".alarms.inactive." + sensor_id;
            std::string message = alarm_path + " 1 " + UNIX2timestamp(current_time) + "\n";
            post_metric(machine_id, "alarms.inactive." + sensor_id, UNIX2timestamp(current_time), 1);
        }
    }
}
void processing_outlier_data()
{
    if (firstMessages.empty())
    {
        return;
    }
    for (auto &sensor : firstMessages)
    {
        std::pair<std::string, std::string> machine_sensor_pair = {sensor.id, sensor.id};
        if (sensor_values_history.find(machine_sensor_pair) != sensor_values_history.end())
        {
            if (is_outlier(sensor_values_history[machine_sensor_pair].back(), sensor_values_history[machine_sensor_pair]))
            {
                std::string alarm_path = sensor.id + ".alarms.outlier." + sensor.id;
                std::string message = alarm_path + " 1 " + UNIX2timestamp(std::time(nullptr)) + "\n";
                post_metric(sensor.id, "alarms.outlier." + sensor.id, UNIX2timestamp(std::time(nullptr)), 1);
            }
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

    class callback : public virtual mqtt::callback
    {
    public:
        void message_arrived(mqtt::const_message_ptr msg) override
        {
            auto j = nlohmann::json::parse(msg->get_payload());

            std::string topic = msg->get_topic();
            if (topic == "/sensor_monitors")
            {
                processInitialMessage(j);
                return;
            }
            else
            {
                auto topic_parts = split(topic, '/');
                std::string machine_id = topic_parts[2];
                std::string sensor_id = topic_parts[3];

                std::string timestamp = j["timestamp"];
                float value = j["value"];
                post_metric(machine_id, sensor_id, timestamp, value);

                std::pair<std::string, std::string> machine_sensor_pair = {machine_id, sensor_id};
                last_sensor_activity[machine_sensor_pair] = std::time(nullptr);

                sensor_values_history[machine_sensor_pair].push_back(value);

                if (sensor_values_history.find(machine_sensor_pair) != sensor_values_history.end())
                {
                    if (is_outlier(value, sensor_values_history[machine_sensor_pair]))
                    {
                        std::string alarm_path = machine_id + ".alarms.outlier." + sensor_id;
                        std::string message = alarm_path + " 1 " + UNIX2timestamp(std::time(nullptr)) + "\n";
                        post_metric(machine_id, "alarms.outlier." + sensor_id, UNIX2timestamp(std::time(nullptr)), 1);
                    }
                }
            }
        }
    };

    callback cb;
    client.set_callback(cb);

    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try
    {
        client.connect(connOpts)->wait();
        client.subscribe("/sensors/#", QOS);
        client.subscribe("/sensor_monitors", QOS); 
    }
    catch (mqtt::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    while (true)
    {
        if (firstMessages.empty())
        {
            std::cout << "Waiting for initial messages..." << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            system("clear");
            continue;
        };

        for (auto &sensor : firstMessages)
        {
            processing_alarm_data(sensor.interval);
            std::this_thread::sleep_for(std::chrono::milliseconds(sensor.interval));
        }
    }

    return EXIT_SUCCESS;
}
