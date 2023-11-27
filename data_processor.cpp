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


std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();

double calculate_frequency()
{
    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_time - start_time;

    start_time = end_time;
    
    return elapsed_seconds.count();
}

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

std::map<std::pair<std::string, std::string>, std::vector<float>> sensor_values_history;

std::pair<float, float> calculate_mean_stddev(const std::vector<float> &data)
{
    float mean = std::accumulate(data.begin(), data.end(), 0.0) / data.size(); // soma os valores do vetor e divide pelo tamanho do vetor
    float sq_sum = std::inner_product(data.begin(), data.end(), data.begin(), 0.0); // multiplica os valores do vetor por ele mesmo e soma
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
    // https://www.analyticsvidhya.com/blog/2022/08/dealing-with-outliers-using-the-z-score-method/
}

void post_metric(const std::string &machine_id, const std::string &sensor_id, const std::string &timestamp_str, const float value)
{
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

std::map<std::pair<std::string, std::string>, std::time_t> last_sensor_activity;
int frequency_sensor = 0;

void processing_data_mosquitto()
{
    std::time_t current_time = std::time(nullptr);
    double frequency_sensor = calculate_frequency() * 10;
    for (auto &activity : last_sensor_activity)
    {
        std::pair<std::string, std::string> machine_sensor_pair = activity.first;
        std::time_t last_time = activity.second;
        std::string machine_id = activity.first.first;
        std::string sensor_id = activity.first.second;

        if (current_time - last_time >= frequency_sensor && last_sensor_activity.size() != 0) // Valor de exemplo para inatividade
        {
            std::string alarm_path = machine_id + ".alarms.inactive." + sensor_id;
            std::string message = alarm_path + " 1 " + UNIX2timestamp(current_time) + "\n";
            // Envie o alarme de inatividade individual para o Graphite.
            post_metric(machine_id, "alarms.inactive." + sensor_id, UNIX2timestamp(current_time), 1);
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

            // Verificar se o valor atual é um outlier.
            if (sensor_values_history.find(machine_sensor_pair) != sensor_values_history.end())
            {
                if (is_outlier(value, sensor_values_history[machine_sensor_pair]))
                {
                    std::string alarm_path = machine_id + ".alarms.outlier." + sensor_id;
                    std::string message = alarm_path + " 1 " + UNIX2timestamp(std::time(nullptr)) + "\n";
                    // Envie o alarme de outlier individual para o Graphite.
                    post_metric(machine_id, "alarms.outlier." + sensor_id, UNIX2timestamp(std::time(nullptr)), 1);
                    std::cout << "Outlier detected for " << sensor_id << ": " << value << std::endl;
                }
            }
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
        processing_data_mosquitto();
        std::this_thread::sleep_for(std::chrono::seconds(1)); // depois modificar o tempo para assim que chegar uma mensagem processar
    }

    return EXIT_SUCCESS;
}
