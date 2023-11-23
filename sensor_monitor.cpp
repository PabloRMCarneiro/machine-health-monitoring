#include <iostream>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <thread>
#include <unistd.h>
#include "json.hpp"      // json handling
#include "mqtt/client.h" // paho mqtt
#include <iomanip>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"

struct SensorInfo {
    std::string id;
    std::string type;
    int interval;
    // Constructor
    SensorInfo(std::string id, std::string type, int interval)
        : id(id), type(type), interval(interval) {}
};

std::string getMachineId()
{
    std::ifstream file("/proc/sys/kernel/random/boot_id");
    std::string machineId;
    if (file.is_open())
    {
        file >> machineId;
        file.close();
    }
    else
    {
        machineId = "unknown";
    }
    return machineId;
}

long long stringToLongLong(const std::string &str)
{
    std::stringstream ss(str);
    long long num;
    ss >> num;
    return num;
}

float getUsedMemoryInGB()
{
    std::ifstream file("/proc/meminfo");
    std::string line;
    long long totalMem = 0;
    long long freeMem = 0;
    long long availableMem = 0;
    long long buffers = 0;
    long long cached = 0;

    if (file.is_open())
    {
        while (std::getline(file, line))
        {
            if (line.find("MemTotal:") == 0)
            {
                totalMem = stringToLongLong(line.substr(10));
            }
            else if (line.find("MemFree:") == 0)
            {
                freeMem = stringToLongLong(line.substr(9));
            }
            else if (line.find("MemAvailable:") == 0)
            {
                availableMem = stringToLongLong(line.substr(14));
            }
            else if (line.find("Buffers:") == 0)
            {
                buffers = stringToLongLong(line.substr(9));
            }
            else if (line.find("Cached:") == 0)
            {
                cached = stringToLongLong(line.substr(8));
            }
        }
        file.close();

        // Calculate the used memory
        long long usedMem = totalMem - (freeMem + buffers + cached);
        // Convert from kB to GB
        float usedMemGB = usedMem / (1024.0 * 1024.0);

        return usedMemGB;
    }
    else
    {
        // Unable to read memory info, return error value
        return -1;
    }
}

float getCpuTemperature()
{
    std::ifstream file("/sys/class/thermal/thermal_zone0/temp");
    float temp;
    if (file.is_open())
    {
        file >> temp;
        // The temperature is usually reported as millidegrees Celsius, so convert to degrees
        temp /= 1000;
        file.close();
    }
    else
    {
        // Unable to read temperature, return error value
        temp = -1;
    }
    return temp;
}

void publishInitialMessage(mqtt::client& client, const std::string& machineId, const std::vector<SensorInfo>& sensors) {
    nlohmann::json j;
    j["machine_id"] = machineId;

    for (const auto& sensor : sensors) {
        nlohmann::json sensor_info;
        sensor_info["sensor_id"] = sensor.id;
        sensor_info["data_type"] = sensor.type;
        sensor_info["data_interval"] = sensor.interval;
        j["sensors"].push_back(sensor_info);
    }

    mqtt::message msg("/sensor_monitors", j.dump(), QOS, false);
    client.publish(msg);

    std::cout << "message published - topic: " << "/sensor_monitors" << " - message: " << j.dump() << std::endl;
}

void readAndPublishSensorData(mqtt::client& client, const std::string& machineId, const SensorInfo& sensor) {
    while (true) {
        // Simulate sensor data collection
        float sensorValue;
        if (sensor.id == "cpu_temperature") {
            sensorValue = getCpuTemperature();
        } else if (sensor.id == "used_memory") {
            sensorValue = getUsedMemoryInGB();
        } else {
            // Handle unknown sensor
            std::cerr << "Unknown sensor ID: " << sensor.id << std::endl;
            continue;
        }

        // Get current time as ISO 8601 formatted string
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_c);
        std::stringstream ss;
        ss << std::put_time(now_tm, "%FT%TZ");
        std::string timestamp = ss.str();

        // Construct JSON message
        nlohmann::json j;
        j["timestamp"] = timestamp;
        j["value"] = sensorValue;

        // Publish the JSON message to the appropriate topic
        std::string topic = "/sensors/" + machineId + "/" + sensor.id;
        mqtt::message msg(topic, j.dump(), QOS, false);
        client.publish(msg);

        // Sleep for the interval specified for the sensor
        std::cout << "message published - topic: " << topic << " - message: " << j.dump() << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(sensor.interval));
    }
}

int main(int argc, char *argv[])
{
    std::string clientId = "sensor-monitor";
    mqtt::client client(BROKER_ADDRESS, clientId);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try
    {
        client.connect(connOpts);
    }
    catch (mqtt::exception &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    std::clog << "connected to the broker" << std::endl;

    std::string machineId = getMachineId();
    // std::cout << machineId << std::endl;

    std::vector<SensorInfo> sensors;

    sensors.emplace_back("cpu_temperature", "float", 1000);
    sensors.emplace_back("used_memory", "float", 1000);

    // publish initial message

    publishInitialMessage(client, machineId, sensors);

    for (const auto& sensor : sensors) {
        std::thread(readAndPublishSensorData, std::ref(client), machineId, sensor).detach();
    }

    // The main thread can do other tasks or just wait
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return EXIT_SUCCESS;
}