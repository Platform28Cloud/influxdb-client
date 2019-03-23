/*
  ==============================================================================

    p28-influxdb-client.h
    Created: 22 Mar 2019 12:57:15pm
    Author:  Renny Koshy

  ==============================================================================
*/

#pragma once

#include <string>
#include <sstream>
#include <thread>
#include <future>
#include <mutex>
#include <vector>
#include <queue>

namespace Influx {
    class Connection;
    
    struct KeyValue {
        std::string name;
        std::string value;
        
        KeyValue(std::string name, std::string value);
        
        operator const std::string & () const;
    };

    class Measurement {
        friend class Connection;
        
    public:
        Measurement &tag(std::string name, std::string value);

        Measurement &field(std::string name, char value);
        Measurement &field(std::string name, bool value);
        Measurement &field(std::string name, std::string value);
        Measurement &field(std::string name, long long value);
        Measurement &field(std::string name, double value, int prec = 5);
        
        Measurement &timestamp(long long timestamp);

        Measurement();
        Measurement(std::string name);
        Measurement(std::string name, std::string precision);

        Measurement(const Measurement &other);

        Measurement& operator=(Measurement other);

    protected:
        std::string name;
        std::vector<KeyValue> tags;
        std::vector<KeyValue> fields;
        bool havePrecision;
        std::string precision;
        bool haveTimestamp;
        long long ts;
    };

    class Connection {
    public:
        Connection(std::string serverUrl, std::string database, std::string user, std::string password);

        Connection &setDefaultPrecision(std::string defaultPrecision);
        
        Connection &setBufferLength(int measurements);
        Connection &setBufferDuration(int millis);

        virtual ~Connection();
        
        void startWorker();
        void worker(std::future<void> futureObj);
        
        void skipCurlInitialization();
    
        void enqueue(Measurement &measurement);
    private:
        std::string serverUrl;
        std::string database;
        std::string user;
        std::string password;
        
        std::string defaultPrecision;
        
        int bufferLength;
        int bufferDuration;
        
        bool curlInitialized;
        bool skipCurlInit;

        std::unique_ptr<std::thread> workerThread;
        std::promise<void> exitSignal;

        std::mutex bufferMutex;
        std::queue<Measurement> *currentBuffer;
        std::queue<Measurement> bufferA;
        std::queue<Measurement> bufferB;
    };
    
};
