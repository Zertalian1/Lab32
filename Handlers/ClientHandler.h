#ifndef LAB32_CLIENTHANDLER_H
#define LAB32_CLIENTHANDLER_H


#include <string>
#include <iostream>
#include <sys/poll.h>
#include "../Cache/CacheRecord.h"
#include "../Cache/Cache.h"

#define timeOut 1000
#define my_delete(x) {delete x; x = NULL;}


class ClientHandler {
private:
    std::string request;
    std::string prVersion;
    int clientSocket;
    std::string lastField;
    std::string url;
    std::string host;
    std::string port="80";
    std::string errorMsg;
    std::string headers;
    bool isConnected = false;
    pthread_t serverThread;
    Cache* cache;
    CacheRecord *record;
    size_t readPointer = 0;
    bool initialized = false;
    int serverSocket;

    static bool createServer(ClientHandler* client, CacheRecord *record, const std::string &host, const std::string &request);\

    static int connectToServer(ClientHandler* client);

public:

    void deleteEvent(pollfd* conn, short event);

    void setCache(Cache *cache);

    void setServerThread(pthread_t serverThread);

    explicit ClientHandler(int socket);

    bool readFromCache(pollfd* connection);

    const std::string &getErrorMsg() const{return errorMsg;};

    int getSocket() const { return clientSocket; };

    bool receive(ClientHandler* client);

    static void *clientHandlerRoutine(void *args);

    static std::string getPrVersion(std::string in);

    static std::string getUrl(std::string in);

    static std::string getMethod(std::string in);

    bool RequestParser();

    bool sendRequest(const std::string &request) const;

    short returnRead(pollfd* conn);
};


#endif //LAB32_CLIENTHANDLER_H
