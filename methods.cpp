
#include <cpp_redis/cpp_redis>
#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <rocksdb/db.h>

int main(){
    std::map<std::string, int> types_to_int;
    types_to_int["string"] = 0;
    types_to_int["list"] = 1;
    types_to_int["set"] = 2;
    types_to_int["hash"] = 3;
    types_to_int["zset"] = 4;
    //std::cout << types_to_string["string"] << std::endl;



    //cpp_redis::active_logger = std::unique_ptr<cpp_redis::logger>(new cpp_redis::logger);
    cpp_redis::reply answer, answer2;
    //std::vector<std::string> answer;
    std::vector<cpp_redis::reply> keys, types;
    auto reply = [&answer](cpp_redis::reply &reply) {
        //answer2.push_back(reply);
       answer = reply;
    };


    cpp_redis::client client;
    client.connect("127.0.0.1", 6379);
    std::cout << client.is_connected() << std::endl;
    client.keys("*", reply);
    client.sync_commit();
    keys = answer.as_array();
    for(auto &temp : keys){
        client.type(temp.as_string(), [&types](cpp_redis::reply &reply){
            types.push_back(reply);
            //answer2 = reply;
        });
        //client.get(temp.as_string(), [](cpp_redis::reply &reply){ std::cout << reply << std::endl;});
        //client.sync_commit();
        //std::cout << temp << std::endl;
    }
    client.sync_commit();
    //types = answer2.as_array();
    for(auto &temp : types){
        std::cout << temp << std::endl;
        switch(types_to_int[temp.as_string()]){
            case 0: break;
            case 1: break;
            case 2: break;
            case 3: break;
            case 4: break;
        }
    }
    
    //std::cout << answer2[0] << std::endl;
    //std::string temp_string="";
    //while()










    // client.send({"set","gmail","gmail.com"},reply);
    // client.send({"get", "gmail"}, reply);
    // client.sync_commit();
    // for(auto &temp : answer ){
    //     std::cout << temp << std::endl;
    // }
    // answer.clear();
    // //client.send({"scan", 0},reply);
    // client.send({"get","gmail"},reply);
    // //client.get("gmail",reply);
    // client.scan(0, [&answer2](cpp_redis::reply &reply){
    //     answer2.push_back(reply);
    // });

    // client.sync_commit();
    // std::cout << answer2[0] << std::endl;

    // for(auto &temp : answer2 ){
    //     std::cout << temp << std::endl;
    // }




    return 0;
}