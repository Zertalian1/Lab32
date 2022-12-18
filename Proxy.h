#ifndef LAB32_PROXY_H
#define LAB32_PROXY_H


#include <csignal>
#include <netinet/in.h>
#include <map>
#include <vector>
#include <cstring>
#include <stdexcept>
#include <netdb.h>
#include <sys/poll.h>
#include <iostream>
#include "Handlers/ServerHandler.h"
#include "Handlers/ClientHandler.h"
#include "Cache/Cache.h"

#define my_delete(x) {delete x; x = NULL;}

class Proxy {
private:
    sockaddr_in address{};
    int proxySocket;
    bool proxyOn = false;
    Cache *cache;
    pthread_mutex_t stoppingMutex{};
    static pthread_mutex_t mapMutex;
    static std::map<pthread_t, bool> handlerStatus;

    static void cleanHandlers();

    int createProxySocket(int port);

    bool initProxySocket();

    static void cleanDoneHandlers();


public:

    bool getProxyOn() const {return proxyOn;};

    explicit Proxy(int port);

    ~Proxy();

    void run();

    void stopProxy();

    bool shouldStop();

    int getSocket() const { return proxySocket; };

    static void addHandler(pthread_t tid);

    static void setHandlerDone(pthread_t tid);

    static void removeHandler(pthread_t tid);

    static void cancelHandlers();

    void returnRead();

    static bool isDone(pthread_t tid);
};


#endif //LAB32_PROXY_H
