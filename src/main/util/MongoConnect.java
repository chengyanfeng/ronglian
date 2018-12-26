package main.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据库连接的公共方法
 *
 * @author by caoxz
 * @date 2018-05-13
 */

public class MongoConnect {

    public static MongoConnect instance;

    private MongoConnect() {
        instance = this;
    }

    public static MongoConnect getInstance() {
        if (instance == null) {
            new MongoConnect();
        }
        return instance;
    }

    /**
     * 存放数据库连接
     */
    Map<String, MongoClient> map = new ConcurrentHashMap<>();

    /**
     * 真正连接数据库的方法
     * 如果连接已经存在了，返回已经建立的连接，不要重复创建连接
     *
     * @param ip
     * @param port
     * @param dbUsername
     * @param dbPassword
     * @return
     */
    public MongoClient connect(String ip, Integer port, String dbUsername, String dbPassword) {
        String key = ip + "_" + port;
        MongoClient mongo = map.get(key);
        if (mongo == null) {
            ServerAddress hosts = new ServerAddress(ip, port);
            List<MongoCredential> credentials = new ArrayList<>();
            //如果是需要密码认证的，加上下面这段代码
            MongoCredential credential = MongoCredential.createCredential(
                    dbUsername, "sj_data",
                    dbPassword.toCharArray());
            credentials.add(credential);
            MongoClientOptions options = MongoClientOptions.builder().cursorFinalizerEnabled(false).build();

            mongo = new MongoClient(hosts, credentials, options);
            map.put(key, mongo);
            System.out.println("connected to db");
        }

        return mongo;
    }

    public MongoClient connect(String ip, Integer port) {
        String key = ip + "_" + port;
        MongoClient mongo = map.get(key);
        if (mongo == null) {
            ServerAddress hosts = new ServerAddress(ip, port);
            List<MongoCredential> credentials = new ArrayList<>();
            MongoClientOptions options = MongoClientOptions.builder().cursorFinalizerEnabled(false).build();

            mongo = new MongoClient(hosts, credentials, options);
            map.put(key, mongo);
            System.out.println("connected to db");
        }

        return mongo;
    }

    /**
     * 关闭数据库连接
     */
    public void close() {
        for (MongoClient mongo : map.values()) {
            if (mongo != null) {
                mongo.close();
            }
        }
    }

}
