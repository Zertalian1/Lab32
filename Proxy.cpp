
#include "Proxy.h"


pthread_mutex_t Proxy::mapMutex;
std::map<pthread_t, bool> Proxy::handlerStatus; // statuses of handlers to join them later

Proxy::Proxy(int port) {

    //std::cout <<"Proxy :: Starting proxy on port " + std::to_string(port) << "..." << '\n';

    this->proxySocket = createProxySocket(port);
    if (proxySocket == -1) {
        std::cerr << "Proxy :: Socket creation failed. Shutting down." << '\n';
        return;
    }

    if (!initProxySocket()) {
        std::cerr << "Proxy :: Shutting down due to an internal error." << '\n';
        std::cerr << "failed to bind socket on port " + std::to_string(port) << '\n';
        return;
    }

    this->proxyOn = true;
    this->cache = new Cache();

    //memset(&address, 0, sizeof(address));

    pthread_mutex_init(&stoppingMutex, nullptr);
    pthread_mutex_init(&mapMutex, nullptr);

}

int Proxy::createProxySocket(int port) {

    this->address.sin_port = htons(port);
    this->address.sin_family = AF_INET;
    this->address.sin_addr.s_addr = htonl(INADDR_ANY);
    return socket(AF_INET, SOCK_STREAM, 0);

}

bool Proxy::initProxySocket() {

    if (bind(proxySocket, (sockaddr *) &address, sizeof(address)) == -1) {
       std::cerr << "Proxy :: Socket bind failed. Shutting down." << '\n';
        return false;
    }

    if (listen(proxySocket, 510) == -1) {
        std::cerr << "Proxy :: listen(2) failed. Shutting down." << '\n';
        return false;
    }

    return true;
}

void *signalHandler(void *args) { // thread that sits and waits for incoming signals to notify proxy about

    auto *proxy = (Proxy *) args;
    sigset_t mask;
    int err, signo;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    //sigaddset(&mask, SIGSEGV);

    err = sigwait(&mask, &signo);
    //std::cout << "Proxy :: in signalHandler:: signo = " + std::to_string(signo) << '\n';

    if (err != 0) {
        std::cerr << "Proxy :: signalHandler:: error in sigwait()" << '\n';
        proxy->stopProxy();
        close(proxy->getSocket());
        pthread_exit(nullptr);
    }

    if (signo == SIGINT || signo == SIGSEGV || signo == SIGTERM) {
        std::cout << "Proxy :: stopping Proxy and closing socket" << '\n';
        proxy->stopProxy();
        close(proxy->getSocket());
    }

    return nullptr;
}


void Proxy::run() {

    /* SIGINT  для лохов (пусть ни прокси, ни его потомки не обращают внимание на SIGINT)*/
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT | SIGTERM);
    // обрабатывать SIGTERM
    if (pthread_sigmask(SIG_BLOCK, &mask, nullptr) != 0) {
        std::cerr << "Proxy :: signalHandler:: error in pthread_sigmask()" << '\n';
        pthread_exit(nullptr);
    }

    /* лох (на лекции говорили сделать пайпы и прочие приколы для обработки сигналов, сделаем отдельный поток для обработки SIGINT)*/
    pthread_t signalThread;
    if (pthread_create(&signalThread, nullptr, signalHandler, (void *) this) != 0) {
        std::cerr << "Proxy :: Failed to create server thread" << '\n';
        return;
    }

    /* мы вообще не хотим из основной нити трогать поток обработчик сигналов*/
    if (pthread_detach(signalThread) != 0) {
        std::cerr << "Proxy :: Failed to detach single handling thread" << '\n';
    }


    /* и снова poll ибо хз как иначе отслеживать происходящее */
    int selectedDescNum;

    std::vector<pollfd> pollingSet;
    pollfd fd{};
    fd.fd = proxySocket;
    fd.events = POLLIN;
    fd.revents = 0;
    pollingSet.push_back(fd);

    while (!shouldStop() && !cache->ranOutOfMemory()) {
        if ((selectedDescNum = poll(pollingSet.data(), pollingSet.size(), timeOut)) == -1) {
            std::cerr << "Proxy run poll error. Shutting down" << '\n';
            stopProxy();
        }

        /* poll писос долго может сидеть ждать, поэтому, проверим, мб нас закрыли */
        if (shouldStop()) {
            break;
        }

        /* смотрим, сколько событий случилось*/
        if (selectedDescNum > 0) {
            /* принимаем соединение*/
            if (pollingSet[0].revents == POLLIN) {
                pollingSet[0].revents = 0;

                int newClientFD;

                if ((newClientFD = accept(proxySocket, nullptr, nullptr)) == -1) {
                    std::cerr << "Proxy :: Failed to accept new connection" << '\n';
                    continue;
                }
                std::cout << "Proxy :: Accepted new connection from client #" + std::to_string(newClientFD) << '\n';

                pthread_t newThread;

                // для передачи разных параметров в поток
                auto args = new std::tuple<int, Cache *>(newClientFD, cache);

                // маски сигналов
                if (pthread_create(&newThread, nullptr, ClientHandler::clientHandlerRoutine, (void *) args) != 0) {
                    std::cerr << "Proxy :: Failed to create new thread" << '\n';
                    stopProxy();
                }

                addHandler(newThread);
            }
        }
        /* больше нет никакого цикла, где можно было бы понять, как там хэндлеры, поэтому есть мапа, позволяющая оценить это*/
        /* удаление мёртвых обработчиков*/
        cleanDoneHandlers();
        cache->deleteDeadRecords();
    }

    /* убиваем всех */
    cancelHandlers();

    /* wake up threads that are sleeping on a cond var*/
    /*if (!cache->empty()) {  //
        cache->wakeUpReaders();
    }*/

    /* and join them all */
    cleanHandlers();    //
}

template<typename T>
void clearVector(std::vector<T> &vec) {

    std::vector<T>().swap(vec);
    vec.clear();

}

void Proxy::cancelHandlers() {

    pthread_mutex_lock(&mapMutex);
    for (auto handler : handlerStatus) {
        pthread_cancel(handler.first);
        std::cout << "Proxy :: cancelled thread #" + std::to_string(handler.first) << '\n';
    }
    pthread_mutex_unlock(&mapMutex);

}

void Proxy::addHandler(pthread_t tid) {

    pthread_mutex_lock(&mapMutex);
    handlerStatus.insert(std::make_pair(tid, false));
    pthread_mutex_unlock(&mapMutex);

}

void Proxy::setHandlerDone(pthread_t tid) {

    pthread_mutex_lock(&mapMutex);
    auto itr = handlerStatus.find(tid);
    if (itr != handlerStatus.end()) {
        (*itr).second = true;
    }
    pthread_mutex_unlock(&mapMutex);

}

void Proxy::removeHandler(pthread_t tid) {

    pthread_mutex_lock(&mapMutex);
    auto itr = handlerStatus.find(tid);
    if (itr != handlerStatus.end()) {
        handlerStatus.erase(itr);
    }
    pthread_mutex_unlock(&mapMutex);

}

bool Proxy::isDone(pthread_t tid){
    pthread_mutex_lock(&mapMutex);
    auto itr = handlerStatus.find(tid);
    if (itr != handlerStatus.end()) {
        return itr->second;
    }
    pthread_mutex_unlock(&mapMutex);
}

void Proxy::cleanDoneHandlers() {

    pthread_mutex_lock(&mapMutex);
    for (auto it = handlerStatus.begin(); it != handlerStatus.end();) {
        if ((*it).second) {
            void *tret;
            pthread_t tid = (*it).first;
            pthread_join(tid, &tret);
            //std::cout << "Proxy :: joined thread #" + std::to_string(tid) + " status = " + std::to_string((long) tret) << '\n';
            it = handlerStatus.erase(it);
        } else {
            ++it;
        }
    }
    pthread_mutex_unlock(&mapMutex);

}


void Proxy::cleanHandlers() {

    while (!handlerStatus.empty()) {
        for (auto it = handlerStatus.cbegin(), next_it = it; it != handlerStatus.cend(); it = next_it) {
            ++next_it;
            void *tret;
            pthread_t tid = (*it).first;
            //std::cout << "Proxy :: joining thread #" + std::to_string(tid) << '\n';
            pthread_join(tid, &tret);
            //std::cout << "join";
            //std::cout << "Proxy :: joined thread #" + std::to_string(tid) + " status = " + std::to_string((long) tret) << '\n';
            removeHandler(tid);
        }
    }

}

void Proxy::stopProxy() {

    //std::cout << "Stopping proxy" << '\n';
    pthread_mutex_lock(&stoppingMutex);
    this->proxyOn = false;
    pthread_mutex_unlock(&stoppingMutex);

}

bool Proxy::shouldStop() {

    pthread_mutex_lock(&stoppingMutex);
    bool stop = this->proxyOn;
    pthread_mutex_unlock(&stoppingMutex);
    return !stop;

}

Proxy::~Proxy() {
    my_delete(cache)
    pthread_mutex_destroy(&stoppingMutex);
    pthread_mutex_destroy(&mapMutex);
    close(proxySocket);
}

/* если добавлять нормальную поддержку http 1.1 нкжно сделать:
 *  1) навесить цикл while на  server и client
 *  2) дать им пайпы, чтобы они общались между собой (сообщели о приёме/ отправке данных)
 *  3) повесить poll на пайпы (если не использоапть poll будут холостые циклы)
 *  4) мелкие доработки*/