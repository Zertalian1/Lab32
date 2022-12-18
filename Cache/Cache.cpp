#include "Cache.h"
Cache::Cache() {
    pthread_mutex_init(&mutex, nullptr);
}

CacheRecord *Cache::getRecord(const std::string &url) {

    if (isCached(url)) {
        pthread_mutex_lock(&mutex);
        auto *rec = cache.at(url);
        pthread_mutex_unlock(&mutex);
        return rec;
    } else {
        return nullptr;
    }

}

bool Cache::isCached(const std::string &url) {

    bool cached, isBroken;
    pthread_mutex_lock(&mutex);
    cached = (cache.find(url) != cache.end()) && !cache.find(url)->second->getDelete();
    if (cached) {
        isBroken = cache.find(url)->second->isBroken();
    }
    pthread_mutex_unlock(&mutex);
    return cached && !isBroken;

}

CacheRecord *Cache::addRecord(const std::string &url) {

    pthread_mutex_lock(&mutex);
    if (cache.find(url) != cache.end()) {
        delete cache.find(url)->second;
    }
    //std::cout << "Adding new record for " + url << '\n';
    auto record = new CacheRecord(this);
    cache.insert(std::make_pair(url, record));
    pthread_mutex_unlock(&mutex);
    return record;

}

bool Cache::ranOutOfMemory() {

    pthread_mutex_lock(&mutex);
    bool ran = ran_out_of_memory;
    pthread_mutex_unlock(&mutex);
    return ran;

}

void Cache::setRanOutOfMemory() {

    pthread_mutex_lock(&mutex);
    ran_out_of_memory = true;
    pthread_mutex_unlock(&mutex);

}

void Cache::wakeUpReaders() {

    pthread_mutex_lock(&mutex);
    for (auto record : cache) {
        record.second->wakeUpReaders();
    }
    pthread_mutex_unlock(&mutex);

}

Cache::~Cache() {

    for (const auto &record : cache) {
        delete record.second;
    }
    pthread_mutex_destroy(&mutex);
    std::cout << "Cache deleted" << '\n';

}

void Cache::deleteDeadRecords() {
    //for (auto record : cache){
    //for (it1 = mp.begin(); it1!=mp.end(); ++it1)
    // навесить мьютекс на все
    pthread_mutex_lock(&mutex);
    for(auto record = cache.begin(); record!=cache.end();++record){
        if(!record->second->tryDeleteRecord()){
            //delete record->second;
            cache.erase(record->first);
            std::cout << "record deleted" << '\n';
        }

        /*if ((record->second->getObserversCount() == 0 && record->second->isFullyCached()) || record->second->isBroken()){
            if(record->second->getIterationsAlive() == 0 || record->second->isBroken()){
                record->second->setDelete();
                cache.erase(record->first);
                std::cout << "record deleted" << '\n';
                continue;
            }
            record->second->Iteration();
        }*/
    }
    pthread_mutex_unlock(&mutex);
}
