#include "sqlconnpool.h"
using namespace std;

SqlConnPool::SqlConnPool() {
    useCount_ = 0;
    freeCount_ = 0;
}

SqlConnPool* SqlConnPool::Instance() {
    static SqlConnPool connPool;
    return &connPool;
}

std::unordered_map<std::string, std::string> SqlConnPool::readConfig(const std::string& filePath) {
    std::unordered_map<std::string, std::string> config;
    std::ifstream file(filePath);
    if (!file.is_open()) {
        LOG_ERROR("Cannot open configuration file: %s", filePath);
        // std::cerr << "Cannot open configuration file: " << filePath << std::endl;
        assert(0);
    }
    std::string line;
    while (std::getline(file, line)) {
        // 忽略空行和注释行（以 '#' 开头的行）
        if (line.empty() || line[0] == '#')
            continue;
        size_t pos = line.find('=');
        if (pos == std::string::npos)
            continue;
        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);
        LOG_DEBUG(key.c_str());
        // 可添加trim操作去除首尾空格，此处省略
        config[key] = value;
    }
    // 获取各项配置，如不存在则可使用默认值
    config["port"]      = config.count("port")    ? config["port"]      : "3306";
    config["user"]      = config.count("user")    ? config["user"]      : "root";
    config["pwd"]       = config.count("pwd")     ? config["pwd"]       : "123456";
    config["dbName"]    = config.count("dbName")  ? config["dbName"]    : "webserver";
    config["connSize"]  = config.count("connSize")? config["connSize"]  : "10";
    return config;
}

int SqlConnPool::Init(const char* host) {
    // 获取配置
    std::string configFile = "./code/pool/db_config.ini";
    auto config = readConfig(configFile);
    // std::cout << config["port"];
    // std::cout << config["connSize"];
    // int port = 3306;
    // int connSize = 10;
    // LOG_INFO("%s, %s", config["port"], config["connSize"]);
    int port           = std::stoi(config["port"]);
    std::string user   = config["user"];
    std::string pwd    = config["pwd"];
    std::string dbName = config["dbName"];
    int connSize       = std::stoi(config["connSize"]);
    assert(connSize > 0);

    for (int i = 0; i < connSize; i++) {
        MYSQL *sql = nullptr;
        sql = mysql_init(sql);
        if (!sql) {
            LOG_ERROR("MySql init error!");
            assert(sql);
        }
        // assert(sql);
        // printf("%s %s %s %s %d\n",host, user, pwd, dbName, port);
        sql = mysql_real_connect(sql, host,
                                 user.c_str(), pwd.c_str(),
                                 dbName.c_str(), port, nullptr, 0);
        if (!sql) {
            LOG_ERROR("MySql Connect error!");
            assert(sql);
        }
        // assert(sql);
        connQue_.push(sql);
    }
    MAX_CONN_ = connSize;
    sem_init(&semId_, 0, MAX_CONN_);
    return connSize;
}

MYSQL* SqlConnPool::GetConn() {
    MYSQL *sql = nullptr;
    if(connQue_.empty()){
        LOG_WARN("SqlConnPool busy!");
        return nullptr;
    }
    sem_wait(&semId_);
    {
        lock_guard<mutex> locker(mtx_);
        sql = connQue_.front();
        // cout << "test" << endl;
        connQue_.pop();
    }
    return sql;
}

void SqlConnPool::FreeConn(MYSQL* sql) {
    assert(sql);
    lock_guard<mutex> locker(mtx_);
    connQue_.push(sql);
    sem_post(&semId_);
}

void SqlConnPool::ClosePool() {
    lock_guard<mutex> locker(mtx_);
    while(!connQue_.empty()) {
        auto item = connQue_.front();
        connQue_.pop();
        mysql_close(item);
    }
    mysql_library_end();        
}

int SqlConnPool::GetFreeConnCount() {
    lock_guard<mutex> locker(mtx_);
    return connQue_.size();
}

SqlConnPool::~SqlConnPool() {
    ClosePool();
}
