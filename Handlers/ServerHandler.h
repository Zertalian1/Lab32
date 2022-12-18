#ifndef LAB32_SERVERHANDLER_H
#define LAB32_SERVERHANDLER_H

#include <iostream>
#include <sys/poll.h>
#include "../Cache/CacheRecord.h"

#define my_delete(x) {delete x; x = NULL;}
#define timeOut 1000

class ServerHandler {
private:
    int serverSocket;
    pollfd connection;
    CacheRecord *cacheRecord;

public:

    static void *serverHandlerRoutine(void *args);

    ServerHandler(int socket, CacheRecord *record);

    int getSocket() const { return serverSocket; };

    CacheRecord *getCacheRecord() { return cacheRecord; };

    void setRecordInvalid() { cacheRecord->setBroken(); };

    ~ServerHandler();

    bool receive();

    void addNewConnection(short event);
};


#endif //LAB32_SERVERHANDLER_H
