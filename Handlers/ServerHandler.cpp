//
// Created by danil on 06.11.22.
//

#include "ServerHandler.h"
#include "../Proxy.h"

ServerHandler::ServerHandler(int socket, CacheRecord *record){
    this->serverSocket = socket;
    this->cacheRecord = record;
}

bool ServerHandler::receive() {
    char buffer[BUFSIZ];
    ssize_t len;

    //std::cout << "start receive" << '\n';
    //pthread_testcancel();

    len = read(serverSocket, buffer, BUFSIZ);
    //std::cout << "READ " << "FROM SERVER " << '\n' << buffer << '\n';
    if (len > 0) {
        cacheRecord->write(std::string(buffer, len));
    }

   if (len < 0) {
       std::cerr << "Failed to readFromCache data" << '\n';
       cacheRecord->setBroken();
       return false;
   }

    if (len == 0) {
        std::cout << "Client #" + std::to_string(serverSocket) + " done writing. Closing connection" << '\n';
        cacheRecord->setFullyCached();
        return false;
    }

    if (cacheRecord->getObserversCount() == 0) {
        cacheRecord->setBroken();
        std::cout << "No one is currently reading url from server #" + std::to_string(serverSocket) + " closing connection." << '\n';
        return false;
    }
    return true;
}

ServerHandler::~ServerHandler() {
    close(serverSocket);
}



void cleanupServer(void *arg) { // server thread cancellation handler

    std::cout << "Server # " + std::to_string(pthread_self()) + " freeing" << '\n';
    /*auto *server = (ServerHandler *)arg;
    close(server->getSocket());
    server->setRecordInvalid();
    server->getCacheRecord()->wakeUpReaders();
    my_delete(server)*/
    //std::cout << "cleaned Server" << '\n';
    std::cout << "Server # " + std::to_string(pthread_self()) + " freed resources" << '\n';

}

void ServerHandler::addNewConnection(short event) {
    pollfd fd{};
    fd.fd = serverSocket;
    fd.events = event;
    fd.revents = 0;
    connection = fd;
}

void * ServerHandler::serverHandlerRoutine(void *args) {

    auto *arguments = (std::tuple<int, CacheRecord *, const std::string> *) args;
    ServerHandler *server = nullptr;
    int selectedDescNum;
    std::cout << "Server hi" << '\n';

    pthread_cleanup_push(cleanupServer, server);

        server = new ServerHandler(std::get<0>(*arguments), std::get<1>(*arguments));
        const auto &request = std::get<2>(*arguments);
        server->addNewConnection(POLLIN | POLLHUP);
        pollfd connection = server->connection;

        while(true) {
            pthread_testcancel();

            if ((selectedDescNum = poll(&connection, 1, timeOut)) == -1) {
                std::cerr << "Proxy: run poll internal error";
                break;
            }

            if(selectedDescNum == 0){
                continue;
            }

            if (connection.revents & POLLIN) {
                if (!server->receive()) {
                    break;
                }
            }
            if (connection.revents & POLLHUP) {
                break;
            }

            //server->receive();
        }

    pthread_cleanup_pop(0);

    close(server->getSocket());
    Proxy::setHandlerDone(pthread_self());
    my_delete(server)
    //std::cout << "Server #" + std::to_string(pthread_self()) + " done and freed resources" << '\n';

    return reinterpret_cast<void *>(0);
}
/* всё хорошо, но хорошо только для версии 1.0*/