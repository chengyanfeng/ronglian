package main.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;


public class Mongodbjdbc {
   /* private static final Logger logger = LoggerFactory
            .getLogger(Mongodbjdbc.class);*/
    public static MongoDatabase mongoDatabase = null;


    //获取mongo链接
    private static MongoDatabase MongoConnet() {
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();
        optionsBuilder.socketKeepAlive(true);
        try {
            //连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址
            //ServerAddress()两个参数分别为 服务器地址 和 端口
            /*ServerAddress serverAddress = new ServerAddress("47.97.3.136", 9999);*/
            ServerAddress serverAddress = new ServerAddress("47.97.3.136", 9999);
            List<ServerAddress> addrs = new ArrayList<ServerAddress>();
            addrs.add(serverAddress);

            //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
            MongoCredential credential = MongoCredential.createScramSha1Credential("sj_user", "sj_data", "sj20181115".toCharArray());
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();
            credentials.add(credential);


            //通过连接认证获取MongoDB连接
            MongoClient mongoClient = new MongoClient(addrs, credentials,optionsBuilder.build());

            //连接到数据库
            mongoDatabase = mongoClient.getDatabase("sj_data");


        } catch (Exception e) {

           e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());


        }
        return mongoDatabase;


    }

    public static MongoDatabase MongGetDom() {
        if (mongoDatabase == null) {
            mongoDatabase = MongoConnet();

        }
        return mongoDatabase;
    }

    //关闭mongo链接
    public static boolean CloseMongoClient(MongoClient mongoDatabase) {
        if (mongoDatabase != null) {
            mongoDatabase.close();
          /* logger.debug("CloseMongoClient successfully");*/
            return true;
        } else {
           /* logger.debug("CloseMongoClient false");*/
            return false;
        }


    }

}
