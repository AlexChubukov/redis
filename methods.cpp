
#include <cpp_redis/cpp_redis>
#include <list>
#include <set>
#include <map>
#include <utility> // for make_pair
#include <future>  // for std::future<cpp_redis::reply>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include "json.hpp"

using json = nlohmann::json;

int main(){
    std::map<std::string, int> types_to_int;            // для switch
    types_to_int["string"] = 0;
    types_to_int["list"] = 1;
    types_to_int["set"] = 2;
    types_to_int["hash"] = 3;
    types_to_int["zset"] = 4;

    // путь к базе данных rocksDB
    std::string DBPath = "db";

    // инициализируем БД
    rocksdb::DB *db;

    // Для управления опциями
    rocksdb::Options options;

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();

    // create the DB if it's not already present
    options.create_if_missing = true;

    // open DB
    rocksdb::Status s = rocksdb::DB::Open(options, DBPath, &db);
    assert(s.ok());
    //if(!s.ok()){
    //    throw(std::runtime_error("can`t open the database"));
    //}

    // Put key-value
    //s = db->Put(rocksdb::WriteOptions(), "key1", "value");
    //assert(s.ok());
    //std::string value;
    // get value
    //s = db->Get(rocksdb::ReadOptions(), "key1", &value);
    //assert(s.ok());
    //assert(value == "value");

    // чтобы ловить reply от функций
    cpp_redis::reply answer, answer2;
    // ловлю reply от answer.as_array
    std::vector<cpp_redis::reply> keys, list_values;
    // лямбда-функция как аргумент callback
    auto reply = [&answer](cpp_redis::reply &reply) {
       answer = reply;
    };


    cpp_redis::redis_client client;
    // создаю клиент для дальнейших подключений
    //cpp_redis::client client;                       
    client.connect("127.0.0.1", 6379, [](const std::string& host, std::size_t port, cpp_redis::client::connect_state status) {  // соединяю с сервером
        if (status == cpp_redis::client::connect_state::dropped) {
            std::cout << "client disconnected from " << host << ":" << port << std::endl;
    }});
    // получаю список всех ключей
    std::future<cpp_redis::reply> my_reply = client.keys("*"); 
    // вношу изменения т.к. все в очереди
    client.sync_commit();
    keys = (my_reply.get()).as_array();

    for(auto &temp : keys){
        client.type(temp.as_string(), reply);
        client.sync_commit();                                  // segm fault
            switch(types_to_int[answer.as_string()]) {
            case 0:
            {
            client.get(temp.as_string(), reply);
            client.sync_commit();
            s = db->Put(rocksdb::WriteOptions(), temp.as_string(), answer.as_string());
            assert(s.ok());
            break;
            }
            case 1:
            {
                std::list<std::string> my_list;
                std::vector<cpp_redis::reply> list_fields;
                client.lrange(temp.as_string(),0 , -1, reply);
                client.sync_commit();
                list_fields = answer.as_array();
                for(auto &list_el : list_fields) {
                    my_list.push_back(list_el.as_string());
                }
                json j_list(my_list);
                s = db->Put(rocksdb::WriteOptions(), temp.as_string(), j_list.dump());
                assert(s.ok());
                break;
            }
            case 2:
            {
                std::set<std::string> my_set;
                std::size_t counter = 0;
                client.scard(temp.as_string(), reply);
                client.sync_commit();
                counter = answer.as_integer();
                std::future<cpp_redis::reply> temp_reply = client.smembers(temp.as_string());
                client.sync_commit();
                list_values = (temp_reply.get()).as_array();
                for(auto &list_el : list_values) {
                    my_set.insert(list_el.as_string());
                }
                json j_set(my_set);
                s = db->Put(rocksdb::WriteOptions(), temp.as_string(), j_set.dump());
                assert(s.ok());
                break;
            }
            case 3: 
            {
                std::map<std::string, std::string> my_map;
                std::vector<cpp_redis::reply> hash_fields;
                client.hgetall(temp.as_string(), reply);
                client.sync_commit();
                hash_fields = answer.as_array();
                for(std::size_t i = 0; i < hash_fields.size();){
                    my_map.insert(std::make_pair(hash_fields[i].as_string(), hash_fields[i + 1].as_string()));
                    i += 2;
                }
                json j_map(my_map);
                s = db->Put(rocksdb::WriteOptions(), temp.as_string(), j_map.dump());
                assert(s.ok());
                break;
            }
            case 4:
            {
                
                std::map<std::string, std::string> my_zset;
                std::vector<cpp_redis::reply> zset_fields;
                std::vector<std::string> zset_scores;
                client.zrange(temp.as_string(), 0, -1, reply);
                client.sync_commit();
                zset_fields = answer.as_array();
                for(auto &zsed_temp : zset_fields){
                    client.zscore(temp.as_string() ,zsed_temp.as_string(), reply);
                    client.sync_commit();
                    zset_scores.push_back(answer.as_string());
                }
                for(std::size_t i = 0; i < zset_scores.size(); ++i){
                    my_zset.insert(std::make_pair(zset_scores[i], zset_fields[i].as_string()));
                }
                json j_zset(my_zset);
                s = db->Put(rocksdb::WriteOptions(), temp.as_string(), j_zset.dump());
                assert(s.ok());
                break;
            }
            default: break;
        }
    std::string value;
    s = db->Get(rocksdb::ReadOptions(), temp.as_string(), &value);
    assert(s.ok());
    std::cout << std::endl << temp.as_string() << " : " << value << std::endl;
    }
    
    delete db;
    return 0;
}