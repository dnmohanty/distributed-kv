#include <iostream>
#include <string>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

int main(int argc, char* argv[]) {
    // Default to proxy port 7000 unless specified
    int port = 7000;
    if (argc > 1) {
        port = std::stoi(argv[1]);
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "Fatal: Could not create socket\n";
        return 1;
    }

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        std::cerr << "Fatal: Invalid address\n";
        return 1;
    }

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Fatal: Connection Failed. Is the Smart Proxy running on port " << port << "?\n";
        return 1;
    }

    std::cout << "Connected to Distributed KV Store (Port " << port << ")\n";
    std::cout << "Type 'exit' to quit.\n\n";

    std::string input;
    char buffer[1024] = {0};

    while (true) {
        std::cout << "kv-store> ";
        std::getline(std::cin, input);

        if (input == "exit" || input == "quit") {
            break;
        }
        if (input.empty()) {
            continue;
        }

        std::string req_with_nl = input + "\n";
        send(sock, req_with_nl.c_str(), req_with_nl.length(), 0);

        memset(buffer, 0, sizeof(buffer));
        int bytes_read = read(sock, buffer, sizeof(buffer) - 1);
        
        if (bytes_read > 0) {
            std::cout << buffer; // The response already has a newline from the server
        } else {
            std::cout << "Connection closed by server.\n";
            break;
        }
    }

    close(sock);
    return 0;
}