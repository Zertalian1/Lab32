#ifndef LAB32_CACHE_H
#define LAB32_CACHE_H


#include <csignal>
#include <vector>
#include <map>
#include <string>
#include <iostream>
#include "CacheRecord.h"

class CacheRecord;

class Cache {
private:
    std::map<std::string, CacheRecord *> cache;
    bool ran_out_of_memory = false;
    pthread_mutex_t mutex;

public:
    Cache();

    ~Cache();

    bool isCached(const std::string &url);

    CacheRecord *addRecord(const std::string &url);

    void wakeUpReaders();

    bool empty() { return cache.empty(); };

    bool ranOutOfMemory();

    void setRanOutOfMemory();

    CacheRecord *getRecord(const std::string &url);

    void deleteDeadRecords();

};


#endif //LAB32_CACHE_H
