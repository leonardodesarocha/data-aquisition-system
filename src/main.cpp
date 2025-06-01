#include <boost/asio.hpp>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <deque>
#include <mutex>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <memory>
#include <filesystem>

using boost::asio::ip::tcp;
namespace fs = std::filesystem;

#pragma pack(push, 1)
struct LogRecord {
    char sensor_id[32];
    std::time_t timestamp;
    double value;
};
#pragma pack(pop)

std::mutex file_mutex;

std::time_t string_to_time_t(const std::string& time_string) {
    std::tm tm = {};
    std::istringstream ss(time_string);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return std::mktime(&tm);
}

std::string time_t_to_string(std::time_t time) {
    std::tm* tm = std::localtime(&time);
    std::ostringstream ss;
    ss << std::put_time(tm, "%Y-%m-%dT%H:%M:%S");
    return ss.str();
}

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket) : socket_(std::move(socket)) {}

    void start() {
        do_read();
    }

private:
    void do_read() {
        auto self(shared_from_this());
        socket_.async_read_some(boost::asio::buffer(data_, max_length),
            [this, self](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    buffer_ += std::string(data_, length);
                    size_t pos;
                    while ((pos = buffer_.find("\r\n")) != std::string::npos) {
                        std::string message = buffer_.substr(0, pos);
                        buffer_.erase(0, pos + 2);
                        process_message(message);
                    }
                    do_read();
                }
            });
    }

    void process_message(const std::string& message) {
        if (message.rfind("LOG|", 0) == 0) {
            handle_log(message);
        } else if (message.rfind("GET|", 0) == 0) {
            handle_get(message);
        } else {
            send_response("ERROR|INVALID_COMMAND\r\n");
        }
    }

    void handle_log(const std::string& msg) {
        std::istringstream iss(msg);
        std::string token;
        std::string id, datetime, value_str;
        if (!std::getline(iss, token, '|') ||
            !std::getline(iss, id, '|') ||
            !std::getline(iss, datetime, '|') ||
            !std::getline(iss, value_str, '|')) {
            send_response("ERROR|INVALID_COMMAND\r\n");
            return;
        }

        LogRecord record;
        std::strncpy(record.sensor_id, id.c_str(), 31);
        record.sensor_id[31] = '\0';
        record.timestamp = string_to_time_t(datetime);
        record.value = std::stod(value_str);

        std::lock_guard<std::mutex> lock(file_mutex);
        fs::create_directory("logs");
        std::string filename = "logs/" + id + ".bin";
        std::ofstream file(filename, std::ios::binary | std::ios::app);
        if (!file) {
            std::cerr << "Erro ao abrir arquivo para escrita: " << filename << std::endl;
            return;
        }
        file.write(reinterpret_cast<const char*>(&record), sizeof(LogRecord));
    }

    void handle_get(const std::string& msg) {
        std::istringstream iss(msg);
        std::string token;
        std::string id, num_str;
        if (!std::getline(iss, token, '|') ||
            !std::getline(iss, id, '|') ||
            !std::getline(iss, num_str, '|')) {
            send_response("ERROR|INVALID_COMMAND\r\n");
            return;
        }

        int n;
        try {
            n = std::stoi(num_str);
        } catch (...) {
            send_response("ERROR|INVALID_COMMAND\r\n");
            return;
        }

        std::lock_guard<std::mutex> lock(file_mutex);
        std::string filename = "logs/" + id + ".bin";
        if (!fs::exists(filename)) {
            send_response("ERROR|INVALID_SENSOR_ID\r\n");
            return;
        }

        std::ifstream file(filename, std::ios::binary);
        std::deque<LogRecord> records;
        LogRecord record;
        while (file.read(reinterpret_cast<char*>(&record), sizeof(LogRecord))) {
            records.push_back(record);
            if (records.size() > n)
                records.pop_front();
        }

        std::ostringstream response;
        response << records.size();
        for (const auto& r : records) {
            response << ";" << time_t_to_string(r.timestamp) << "|" << r.value;
        }
        response << "\r\n";
        send_response(response.str());
    }

    void send_response(const std::string& message) {
        auto self(shared_from_this());
        boost::asio::async_write(socket_, boost::asio::buffer(message),
            [this, self](boost::system::error_code /*ec*/, std::size_t /*length*/) {});
    }

    tcp::socket socket_;
    enum { max_length = 1024 };
    char data_[max_length];
    std::string buffer_;
};

class Server {
public:
    Server(boost::asio::io_context& io_context, short port)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept(
            [this](boost::system::error_code ec, tcp::socket socket) {
                if (!ec) {
                    std::make_shared<Session>(std::move(socket))->start();
                }
                do_accept();
            });
    }

    tcp::acceptor acceptor_;
};

int main(int argc, char* argv[]) {
    try {
        boost::asio::io_context io_context;
        Server server(io_context, 9000);
        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}