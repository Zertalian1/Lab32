#include <iostream>
#include <csignal>
#include "Proxy.h"

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: main.cpp [port number]" << std::endl;
        return -1;
    }

    std::cout << getpid() << std::endl;

    sigset(SIGPIPE, SIG_IGN);
    sigset(SIGSTOP, SIG_IGN);

    struct sigaction act{};
    act.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &act, nullptr);

    std::string mode = argv[1];

    std::cout << "STARTING PROXY ON PORT " << argv[1] << std::endl;

    auto proxy = new Proxy(atoi(argv[1]));
    if(!proxy->getProxyOn()){
        return 0;
    }
    proxy->run();
    delete proxy;
    close(atoi(argv[1]));
    std::cout << "PROXY IS МЁРТВ" << std::endl;

    return 0;
}
