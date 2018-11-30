
#include <cpp_redis/cpp_redis>
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
    //client.connect("127.0.0.1", 6379, [](const std::string& host, std::size_t port, cpp_redis::client::connect_state status) {  // соединяю с сервером
    //    if (status == cpp_redis::redis_client::connect_state::dropped) {
    //        std::cout << "client disconnected from " << host << ":" << port << std::endl;
    //}});
    client.connect("127.0.0.1", 6379);

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
                std::vector<cpp_redis::reply> list_fields;
                client.lrange(temp.as_string(),0 , -1, reply);
                client.sync_commit();
                list_fields = answer.as_array();
                // объект в виде массива
                json j_list;
                for(auto &list_el : list_fields) {
                    j_list.push_back(list_el.as_string());
                }
                s = db->Put(rocksdb::WriteOptions(), temp.as_string(), j_list.dump());
                assert(s.ok());
                // итерация по значениям в массиве json
                for (json::iterator it = j_list.begin(); it != j_list.end(); ++it) {
                    std::cout << *it << std::endl;
                }
                break;
            }
            case 2:
            {
                std::vector<cpp_redis::reply> set_fields;
                std::future<cpp_redis::reply> temp_reply = client.smembers(temp.as_string());
                client.sync_commit();
                set_fields = (temp_reply.get()).as_array();
                json j_set;
                for(auto &set_el : set_fields) {
                    j_set.push_back(set_el.as_string());
                }
                s = db->Put(rocksdb::WriteOptions(), temp.as_string(), j_set.dump());
                assert(s.ok());
                // итерация по значениям в массиве json
                for (json::iterator it = j_set.begin(); it != j_set.end(); ++it) {
                    std::cout << *it << std::endl;
                }
                break;
            }
            case 3: 
            {
                std::vector<cpp_redis::reply> hash_fields;
                client.hgetall(temp.as_string(), reply);
                client.sync_commit();
                hash_fields = answer.as_array();
                json j_hash;
                for(std::size_t i = 0; i < hash_fields.size();){
                    j_hash[hash_fields[i].as_string()] =  hash_fields[i + 1].as_string();
                    i += 2;
                }
                s = db->Put(rocksdb::WriteOptions(), temp.as_string(), j_hash.dump());
                assert(s.ok());
                // итерация по значениям в массиве json
                for (json::iterator it = j_hash.begin(); it != j_hash.end(); ++it) {
                    std::cout << it.key() << " : " << it.value() << "\n";
                }
                break;
            }
            case 4:
            {
                std::vector<cpp_redis::reply> zset_fields;
                std::vector<std::string> zset_scores;
                client.zrange(temp.as_string(), 0, -1, reply);
                client.sync_commit();
                zset_fields = answer.as_array();
                json j_zset;
                for(auto &zsed_temp : zset_fields){
                    client.zscore(temp.as_string() ,zsed_temp.as_string(), reply);
                    client.sync_commit();
                    j_zset[zsed_temp.as_string()] = answer.as_string();
                }
                s = db->Put(rocksdb::WriteOptions(), temp.as_string(), j_zset.dump());
                assert(s.ok());
                // итерация по значениям в массиве json
                for (json::iterator it = j_zset.begin(); it != j_zset.end(); ++it) {
                    std::cout << it.key() << " : " << it.value() << "\n";
                }
                break;
            }
            default: break;
        }
    }

    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << it->key().ToString() << ": " << it->value().ToString() << std::endl;
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;
    
    delete db;
    return 0;
}