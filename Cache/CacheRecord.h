#ifndef LAB32_CACHERECORD_H
#define LAB32_CACHERECORD_H


#include <csignal>
#include <string>
#include "iostream"
#include "Cache.h"

#define defaultIterationsAlive 100

class Cache;

class CacheRecord {
private:
    Cache *cache;
    bool is_fully_cached = false;
    std::string data;
    bool is_broken = false;
    pthread_mutex_t mutex;
    pthread_cond_t condVar;
    volatile int observersCount;
    bool isDelete = false;
    size_t iterationsAlive = defaultIterationsAlive;

public:

    size_t getIterationsAlive();

    void Iteration();

    void recoverIterationsAlive(){
        iterationsAlive = defaultIterationsAlive;
    }

    size_t getDataSize();

    explicit CacheRecord(Cache *cache);

    ~CacheRecord();

    void setBroken();

    bool isBroken();

    std::string read(size_t start, size_t length);

    void write(const std::string &);

    void setFullyCached();

    bool isFullyCached();

    void incrementObserversCount();

    void decrementObserversCount();

    int getObserversCount();

    void wakeUpReaders();

    void setDelete();

    bool getDelete();

    bool tryDeleteRecord();
};



#endif //LAB32_CACHERECORD_H
