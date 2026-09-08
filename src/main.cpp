#include "Store.h"
#include "Server.h"
#include <iostream>

int main() {
    try {
        Store store(16);
        
        Server server(8080, store);
        
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << '\n';
        return 1;
    }
    return 0;
}