cmake_minimum_required(VERSION 3.23)
project(Lab32)

set(CMAKE_CXX_STANDARD 14)

add_executable(Lab32 main.cpp Proxy.cpp Proxy.h Cache/Cache.cpp Cache/Cache.h Cache/CacheRecord.cpp Cache/CacheRecord.h Handlers/ClientHandler.cpp Handlers/ClientHandler.h Handlers/ServerHandler.cpp Handlers/ServerHandler.h)
