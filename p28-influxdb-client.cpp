/*
  ==============================================================================

    p28-influxdb-client.cpp
    Created: 22 Mar 2019 12:57:15pm
    Author:  Renny Koshy

  ==============================================================================
*/

#include "p28-influxdb-client.h"
#include <thread>
#include <future>
#include <curl/curl.h>
#include "TRADE_Logger.h"
#include <chrono>
#include <algorithm>

using namespace std;
using namespace Influx;

extern int cURL_debugFunction(CURL *curlHandle, curl_infotype infoType, char *message, size_t messageSize, void *userPointer);

static TRADE::Logger *logger = nullptr;
static TRADE::Logger *workerLogger = nullptr;

static std::mutex curlMutex;

KeyValue::KeyValue(std::string name, std::string value) : name(name), value(value)
{
}

KeyValue::operator const std::string &() const
{
    static string s;
    std::stringstream ss;
    ss << name << "=" << value << "";
    s = ss.str();
    return s;
}

Measurement::Measurement() : name(""), havePrecision(false), precision("ms")
{
    if (!logger) logger = TRADE::Logger::getLogger("InfluxDBClient");
}

Measurement::Measurement(std::string name) : name(name), havePrecision(false), precision("ms")
{
    if (!logger) logger = TRADE::Logger::getLogger("InfluxDBClient");
}

Measurement::Measurement(std::string name, std::string precision) : name(name), havePrecision(true), precision(precision)
{
    if (!logger) logger = TRADE::Logger::getLogger("InfluxDBClient");
}

Measurement::Measurement(const Measurement &other) :
    name(other.name), tags(other.tags), fields(other.fields),
    havePrecision(other.havePrecision), precision(other.precision), haveTimestamp(other.haveTimestamp), ts(other.ts)
{
    if (!logger) logger = TRADE::Logger::getLogger("InfluxDBClient");
}

Measurement& Measurement::operator=(Measurement other)
{
    name=other.name;
    tags = other.tags;
    fields = other.fields;
    if ((havePrecision=other.havePrecision))
        precision=other.precision;
    if ((haveTimestamp=other.haveTimestamp))
        ts=other.ts;

    return *this;
}


Measurement& Measurement::tag(std::string name, std::string value)
{
    tags.push_back(KeyValue(name,string("\"")+value+"\""));
    return *this;
}

Measurement& Measurement::field(std::string name, char value)
{
    fields.push_back(KeyValue(name,string("\"")+value+"\""));
    return *this;
}

Measurement& Measurement::field(std::string name, std::string value)
{
    fields.push_back(KeyValue(name,string("\"")+value+"\""));
    return *this;
}

Measurement& Measurement::field(std::string name, bool value)
{
    fields.push_back(KeyValue(name,value ? "t" : "f"));
    return *this;
}

Measurement& Measurement::field(std::string name, long long value)
{
    stringstream ss;
    ss << value << 'i';
    fields.push_back(KeyValue(name,ss.str()));
    return *this;
}

Measurement& Measurement::field(std::string name, double value, int prec)
{
    stringstream ss;
    ss.precision(prec);
    ss << value;
    fields.push_back(KeyValue(name,ss.str()));
    return *this;
}

Measurement& Measurement::timestamp(long long timestamp)
{
    ts = timestamp;
    haveTimestamp = true;

    return *this;
}

Connection::Connection(std::string serverUrl, std::string database, std::string user, std::string password) :
    serverUrl(serverUrl),
    database(database),
    user(user),
    password(password),
    defaultPrecision("ms"),
    bufferLength(5),
    bufferDuration(500),
    currentBuffer(&bufferA)
{
    if (!logger) logger = TRADE::Logger::getLogger("InfluxDBClient");
}

void Connection::skipCurlInitialization()
{
    if (!logger) logger = TRADE::Logger::getLogger("InfluxDBClient");

    skipCurlInit = true;
    curlInitialized = true;
    
    logger->info("cURL initialization will be skipped");
}

static bool tagsVectorComparator(const KeyValue &a,const KeyValue &b) { return (a.name < b.name); };
    
void Connection::worker(std::future<void> exitSignalFuture)
{
    CURL *curl;
    CURLcode ret;
    string currentError;

    workerLogger = TRADE::Logger::getLogger("InfluxDBClient.Worker");
    
    curlMutex.lock();
    if (!skipCurlInit && !curlInitialized) {
        curlInitialized = true;
        CURLcode ret = curl_global_init(CURL_GLOBAL_ALL);

        if (ret)
        {
            currentError = curl_easy_strerror(ret);
            workerLogger->error("Initialization failed!  Worker is shutting down: %s", currentError.c_str());
            return;
        }
    }
    curlMutex.unlock();

    while (exitSignalFuture.wait_for(std::chrono::milliseconds(1)) == std::future_status::timeout)
    {
        chrono::nanoseconds ns(bufferDuration*1000);
        std::this_thread::sleep_for(ns);

        std::queue<Measurement> *bufferToProcess = nullptr;
        bufferMutex.lock();
        if (!currentBuffer->empty())
        {
            if (currentBuffer == &bufferA)
            {
                if (workerLogger->isDebugEnabled())
                    workerLogger->debug("Switching to buffer B");
                currentBuffer = &bufferB;
            }
            else
            {
                if (workerLogger->isDebugEnabled())
                    workerLogger->debug("Switching to buffer A");
                currentBuffer = &bufferA;
            }
            bufferToProcess = currentBuffer;
        }
        bufferMutex.unlock();
        
        if (bufferToProcess)
        {
            curlMutex.lock();

            curl = curl_easy_init();
            if(curl)
            {
                curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, cURL_debugFunction);
                curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
                
                /* example.com is redirected, so we tell libcurl to follow redirection */
                if ((ret = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L)))
                {
                    currentError = curl_easy_strerror(ret);
                    workerLogger->error("Could not disable SSL peer validation!  Worker is shutting down: %s", currentError.c_str());
                    return;
                }

                /* We don't verify SSL certs */
                if ((ret = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0)))
                {
                    currentError = curl_easy_strerror(ret);
                    workerLogger->error("Could not disable SSL peer validation!  Worker is shutting down: %s", currentError.c_str());
                    return;
                }

                while (!bufferToProcess->empty())
                {
                    Measurement m = bufferToProcess->front();

                    std::stringstream url;
                    bool first = true;
                    url << serverUrl;
                    url << "/write"; // We are writing to db
                    if (!database.empty()) {
                        url << (first ? "?" : "&") << "db=" << database;
                        first = false;
                    }
                    if (!user.empty())
                    {
                        url << (first ? "?" : "&") << "u=" << user;
                        first = false;
                    }
                    if (!password.empty()) {
                        url << (first ? "?" : "&") << "p=" << password;
                        first = false;
                    }
                    if (m.havePrecision && m.precision.compare("ns"))
                    { // Prescision is not ns?
                        url << (first ? "?" : "&") << "precision=" << m.precision;
                        first = false;
                    }
                    else
                    {
                        url << (first ? "?" : "&") << "precision=" << defaultPrecision;
                        first = false;
                    }
                    
                    /* set the url */
                    curl_easy_setopt(curl, CURLOPT_URL, url.str().c_str());

                    std::stringstream dataLine;
                    string lastPrecision = m.precision;
                    do
                    {
                        /* We're adding this line, so remove from queue */
                        bufferToProcess->pop();

                        dataLine << m.name;
                        if (!m.tags.empty())
                        {
                            dataLine << ',';
                            sort(m.tags.begin(), m.tags.end(), tagsVectorComparator);
                            std::copy(m.tags.begin(), m.tags.end()-1, std::ostream_iterator<string>(dataLine, ","));
                            dataLine << (string)m.tags.back();
                            if (workerLogger->isDebugEnabled())
                                workerLogger->debug("Data is now: %s", dataLine.str().c_str());
                        }

                        dataLine << ' ';
                        
                        if (!m.fields.empty())
                        {
                            sort(m.fields.begin(), m.fields.end(), tagsVectorComparator);
                            std::copy(m.fields.begin(), m.fields.end()-1, std::ostream_iterator<string>(dataLine, ","));
                            dataLine << (string)m.fields.back();
                            if (workerLogger->isDebugEnabled())
                                workerLogger->debug("Data is now: %s", dataLine.str().c_str());
                        }
                        
                        if (m.haveTimestamp)
                            dataLine << " " << m.ts;
                        
                        dataLine << '\n';
                        
                        if (!bufferToProcess->empty())
                            m = bufferToProcess->front();
                    }
                    while (!bufferToProcess->empty() && m.precision == lastPrecision);

                    string data = dataLine.str();
                    
                    /* Perform the request, res will get the return code */
                    if (workerLogger->isDebugEnabled())
                    {
                        workerLogger->debug("--------------------------------------------------------------------------------");
                        workerLogger->debug("Sending data:\n%s", data.c_str());
                        workerLogger->debug("--------------------------------------------------------------------------------");
                    }
                    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data.length());
                    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
                    
                    if ((ret = curl_easy_perform(curl)))
                    {
                        workerLogger->error("curl_easy_perform() failed: %s", curl_easy_strerror(ret));
                    }
                }

                /* always cleanup */
                curl_easy_cleanup(curl);
            }
            
            curlMutex.unlock();
        }
    }
}

Connection::~Connection()
{
    logger->info("Signalling worker thread to stop");
    exitSignal.set_value();
}

void Connection::startWorker()
{
    logger->info("Starting worker thread");

    std::future<void> exitSignalFuture = exitSignal.get_future();
    workerThread.reset(new std::thread(&Connection::worker, this, std::move(exitSignalFuture)));
}

Connection &Connection::setDefaultPrecision(std::string defaultPrecision) {
    this->defaultPrecision = defaultPrecision;
    return *this;
}

Connection &Connection::setBufferLength(int measurements) {
    this->bufferLength = measurements;
    return *this;
}

Connection &Connection::setBufferDuration(int millis) {
    this->bufferDuration = millis;
    return *this;
}

void Connection::enqueue(Measurement &measurement) {
    bufferMutex.lock();
    currentBuffer->push(measurement);
    bufferMutex.unlock();
}
