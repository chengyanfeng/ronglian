package main.serverice;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import main.util.MongoConnect;
import main.util.Mongodbjdbc;
import main.util.ZipUtils;
import main.util.util;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class QueryData {
    /* private static String line = "0215";*/
    public static int i = 0;
    public static LinkedHashMap headMap = new LinkedHashMap();
    /* private static MongoClient mongo = MongoConnect.getInstance().connect("172.21.0.4", 7211, "7moor", "7moorcom");*/
    private static MongoClient mongo = MongoConnect.getInstance().connect("47.97.3.136", 9999, "sj_user", "sj20181115");
    public static List<DBObject> list = null;

    public List<Document> getShow(String beginTime, String endTime, String line) {
        List<Document> list = null;

        list = showDate(line, "cc", beginTime, endTime);

        return list;

    }

    public String getData(String beginTime, String endTime, String line, String user) {
        new Thread() {
            @Override
            public void run() {
                System.out.println("启动--线程");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
            /*String beginTime = "2018-10-01 00:00:00";
            String endTime = "2018-12-01 00:00:00";*/
                List<Date> dateList = util.findDates(beginTime, endTime, sdf);
                System.out.println("开始执行");
                util.creatFile("./CSVPATH/" + user);

                for (int i = 0; i < dateList.size(); i++) {
                    if (i < dateList.size() - 1) {
                        exportCdr(line, "cc", sdf.format(dateList.get(i)), sdf.format(dateList.get(i + 1)), user);
                        exportCdr(line, "400", sdf.format(dateList.get(i)), sdf.format(dateList.get(i + 1)), user);
                        exportCdr(line, "zj", sdf.format(dateList.get(i)), sdf.format(dateList.get(i + 1)), user);

                    }
                }
                util.delete("./CSVPATH/" + user + "/1.txt");

                List<File> srcFiles = new ArrayList<>();
                srcFiles.add(new File("./CSVPATH/" + user));
                //生成压缩包
                ZipUtils.zipFile("./ZIP/" + user + "/" + user + "_" + line + "_" + beginTime.substring(0, 10) + ".zip", srcFiles);
                //删除文件下的所有文件
                util.deleteDirectory("./CSVPATH/" + user);
            }
        }.start();

        return "ok";
    }

    public String getAccountData(String beginTime, String endTime, String account, String user) {

        new Thread() {
            @Override
            public void run() {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
            /*String beginTime = "2018-10-01 00:00:00";
            String endTime = "2018-12-01 00:00:00";*/
                List<Date> dateList = util.findDates(beginTime, endTime, sdf);

                util.creatFile("./CSVPATH/" + user);

                for (int i = 0; i < dateList.size(); i++) {
                    if (i < dateList.size() - 1) {
                        exportAccountCdr(account, "cc", sdf.format(dateList.get(i)), sdf.format(dateList.get(i + 1)), user);
                        exportAccountCdr(account, "400", sdf.format(dateList.get(i)), sdf.format(dateList.get(i + 1)), user);
                        exportAccountCdr(account, "zj", sdf.format(dateList.get(i)), sdf.format(dateList.get(i + 1)), user);

                    }
                }
                util.delete("./CSVPATH/" + user + "/1.txt");

                List<File> srcFiles = new ArrayList<>();
                srcFiles.add(new File("./CSVPATH/" + user));
                //生成压缩包
                ZipUtils.zipFile("./ZIP/" + user + "/" + user + "_" + account + "_" + beginTime.substring(0, 10) + ".zip", srcFiles);
                //删除文件下的所有文件
                util.deleteDirectory("./CSVPATH/" + user);

            }
        }.start();
        return "ok";
    }

    public List<Document> getAccouontShow(String beginTime, String endTime, String account) {
        List<Document> list = null;

        list = showAccountDate(account, "cc", beginTime, endTime);

        return list;

    }

    public List<Document> getSMSShow(String beginTime, String endTime, String account) {
        List<Document> list = null;

        list = showXiaoHaoDate(account, beginTime, endTime);

        return list;

    }

    public List<Document> getXiaoHaoShow(String beginTime, String endTime, String account) {
        List<Document> list = null;

        list = showXiaoHaoDate(account, beginTime, endTime);

        return list;

    }




    public String getXiaoHaoData(String beginTime, String endTime, String account, String user) {

        new Thread() {
            @Override
            public void run() {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
                List<Date> dateList = util.findDates(beginTime, endTime, sdf);

                util.creatFile("./CSVPATH/" + user);

                for (int i = 0; i < dateList.size(); i++) {
                    if (i < dateList.size() - 1) {
                        exportXiaoHaoCdr(account, sdf.format(dateList.get(i)), sdf.format(dateList.get(i + 1)), user);

                    }
                }
                util.delete("./CSVPATH/" + user + "/1.txt");

                List<File> srcFiles = new ArrayList<>();
                srcFiles.add(new File("./CSVPATH/" + user));
                //生成压缩包
                ZipUtils.zipFile("./ZIP/" + user + "/" + user + "_" + account + "_xiaohao_" + beginTime.substring(0, 10) + ".zip", srcFiles);
                //删除文件下的所有文件
                util.deleteDirectory("./CSVPATH/" + user);

            }
        }.start();
        return "ok";
    }


    private static void exportCdr(String line, String product, String beginTime, String endTime, String user) {
        Map<String, Object> query_agent = new HashMap<String, Object>();
        query_agent.put("product", product);
        DBObject where_agent = new BasicDBObject();
        DBObject userKeys = new BasicDBObject();
        userKeys.put("_id", 1);
        userKeys.put("displayName", 1);
        userKeys.put("exten", 1);
        where_agent.putAll(query_agent);

        if (list == null) {
            List<DBObject> list_agent = mongo.getDB("sj_data").getCollection("platform_user1210").find(where_agent, userKeys).toArray();
            list = list_agent;
        }
        FindIterable<Document> test = Mongodbjdbc.MongGetDom().getCollection(
                "bill_cdr_query_" + product).find(new Document().append("endTimeDisplay", new Document()
                .append("$gte", beginTime)
                .append("$lte", endTime)).append("line", line))
                .projection(new Document().append("uniqueid", 1).
                        append("account", 1).
                        append("callAddr", 1).
                        append("calledAddr", 1).
                        append("cid", 1).
                        append("did", 1).
                        append("beginTimeDisplay", 1).
                        append("endTimeDisplay", 1).
                        append("seconds", 1).
                        append("minutes", 1).
                        append("consume", 1).
                        append("isLocal", 1).
                        append("type", 1).
                        append("product", 1).
                        append("line", 1).
                        append("route", 1).
                        append("agentDisplayName", 1).
                        append("agentExten", 1).
                        append("consume", 1)
                )
                .noCursorTimeout(true);
        MongoCursor<Document> mongoCursor = test.iterator();
        List<Document> cdrCcList = new LinkedList<>();
        while (mongoCursor.hasNext()) {
            Document cdrCc = mongoCursor.next();
            i = i + 1;
            try {


                cdrCc.put("callAddr", "" + cdrCc.get("callerProvince") + "-" + cdrCc.get("callerCity"));
                cdrCc.put("calledAddr", "" + cdrCc.get("calleeProvince") + "-" + cdrCc.get("calleeCity"));
                String agentId = (String) cdrCc.get("agentId");

                DBObject dbAgent = (agentId == null || agentId.equals("") ? null : (checkAgentId(list, agentId == null ? "" : agentId.toString())));
                cdrCc.put("agentDisplayName", dbAgent == null ? "" : dbAgent.get("displayName"));
                String agentExtenId = (String) cdrCc.get("agentId");
                DBObject dbAgentExten = (agentExtenId == null || agentExtenId.equals("") ? null : (checkAgentId(list, agentExtenId == null ? "" : agentExtenId.toString())));
                cdrCc.put("agentExten", dbAgentExten == null ? "" : dbAgentExten.get("exten"));

                if (cdrCc.get("type") != null) {
                    String dialType = cdrCc.get("type").toString();
                    if (dialType.equals("normal")) {
                        dialType = "普通来电";
                    } else if (dialType.equals("dialout")) {
                        dialType = "外呼去电";
                    } else if (dialType.equals("transfer")) {
                        dialType = "转接来电";
                    } else if (dialType.equals("dialTransfer")) {
                        dialType = "外呼转接";
                    }
                    cdrCc.put("type", dialType);
                }
                if (cdrCc.get("isLocal") != null) {
                    String isLocal = cdrCc.get("isLocal").toString();
                    if (isLocal.equals(true)) {
                        isLocal = "本地";
                    } else {
                        isLocal = "长途";
                    }
                    cdrCc.put("isLocal", isLocal);
                }
            } catch (Exception e) {
                System.out.println(e);
            }
            if (i % 1000 == 0) {
                System.out.println(i);
            }

            cdrCcList.add(cdrCc);
        }
        util.createCSVFile(cdrCcList, headMap, "./CSVPATH/" + user, "./公司线路对应" + product + "话单" + beginTime.substring(0, 10) + "_");

    }

    private static void exportAccountCdr(String account, String product, String beginTime, String endTime, String user) {

        FindIterable<Document> test = Mongodbjdbc.MongGetDom().getCollection(
                "bill_cdr_query_" + product).find(new Document().append("endTimeDisplay", new Document()
                .append("$gte", beginTime)
                .append("$lte", endTime)).append("account", account))
                .projection(new Document().append("uniqueid", 1).
                        append("account", 1).
                        append("callAddr", 1).
                        append("calledAddr", 1).
                        append("cid", 1).
                        append("did", 1).
                        append("beginTimeDisplay", 1).
                        append("endTimeDisplay", 1).
                        append("seconds", 1).
                        append("minutes", 1).
                        append("consume", 1).
                        append("isLocal", 1).
                        append("type", 1).
                        append("product", 1).
                        append("line", 1).
                        append("route", 1).
                        append("agentDisplayName", 1).
                        append("agentExten", 1).
                        append("consume", 1)
                )
                .noCursorTimeout(true);
        MongoCursor<Document> mongoCursor = test.iterator();
        List<Document> cdrCcList = new LinkedList<>();
        while (mongoCursor.hasNext()) {
            Document cdrCc = mongoCursor.next();
            try {
                i = i + 1;

                cdrCc.put("callAddr", "" + cdrCc.get("callerProvince") + "-" + cdrCc.get("callerCity"));
                cdrCc.put("calledAddr", "" + cdrCc.get("calleeProvince") + "-" + cdrCc.get("calleeCity"));
                String agentId = (String) cdrCc.get("agentId");

                DBObject dbAgent = (agentId == null || agentId.equals("") ? null : (checkAgentId(list, agentId == null ? "" : agentId.toString())));
                cdrCc.put("agentDisplayName", dbAgent == null ? "" : dbAgent.get("displayName"));
                String agentExtenId = (String) cdrCc.get("agentId");
                DBObject dbAgentExten = (agentExtenId == null || agentExtenId.equals("") ? null : (checkAgentId(list, agentExtenId == null ? "" : agentExtenId.toString())));
                cdrCc.put("agentExten", dbAgentExten == null ? "" : dbAgentExten.get("exten"));

                if (cdrCc.get("type") != null) {
                    String dialType = cdrCc.get("type").toString();
                    if (dialType.equals("normal")) {
                        dialType = "普通来电";
                    } else if (dialType.equals("dialout")) {
                        dialType = "外呼去电";
                    } else if (dialType.equals("transfer")) {
                        dialType = "转接来电";
                    } else if (dialType.equals("dialTransfer")) {
                        dialType = "外呼转接";
                    }
                    cdrCc.put("type", dialType);
                }
                if (cdrCc.get("isLocal") != null) {
                    String isLocal = cdrCc.get("isLocal").toString();
                    if (isLocal.equals(true)) {
                        isLocal = "本地";
                    } else {
                        isLocal = "长途";
                    }
                    cdrCc.put("isLocal", isLocal);
                }
                if (i % 1000 == 0) {
                    System.out.println(i);
                }
            } catch (Exception e) {
                System.out.println(e);
            }

            cdrCcList.add(cdrCc);
        }
        util.createCSVFile(cdrCcList, headMap, "./CSVPATH/" + user, "./公司线路对应" + product + "话单" + beginTime.substring(0, 10) + "_");

    }

    private static void exportXiaoHaoCdr(String account, String beginTime, String endTime, String user) {

        FindIterable<Document> xiaohao = Mongodbjdbc.MongGetDom().getCollection(
                "bill_yiketong_xiaohao_cdr_query").find(new Document().append("begin_time", new Document()
                .append("$gte", beginTime)
                .append("$lte", endTime)).append("account", account))
                .projection(new Document().append("account", 1).
                        append("called_show", 1).
                        append("called", 1).
                        append("begin_time", 1).
                        append("connect_time", 1).
                        append("fee", 1).
                        append("income", 1).
                        append("call_cost", 1).
                        append("bill_duration", 1).
                        append("call_duration", 1).
                        append("call_cost", 1).
                        append("caller_area", 1).
                        append("called_area", 1).
                        append("minutes", 1)

                )
                .noCursorTimeout(true);
        MongoCursor<Document> mongoCursor = xiaohao.iterator();
        List<Document> cdrCcList = new LinkedList<>();
        while (mongoCursor.hasNext()) {
            Document cdrCc = mongoCursor.next();
            try {
                i = i + 1;
                cdrCcList.add(cdrCc);
                if (i % 1000 == 0) {
                    System.out.println(i);
                }
            } catch (Exception e) {
                System.out.println(e);
            }

            cdrCcList.add(cdrCc);
        }
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("account", "账户编号");
        map.put("called_show", "小号");
        map.put("called", "拨打电话");
        map.put("connect_time", "连接时常");
        map.put("fee", "消费");
        map.put("income", "呼入时长");
        map.put("begin_time", "开始时间");
        map.put("call_cost", "通话消费");
        map.put("caller_area", "呼叫地址");
        map.put("called_area", "被呼叫地址");
        map.put("minutes", "分钟数");
        map.put("call_duration", "通话时长");
        map.put("bill_duration", "计费时长");


        util.createCSVFile(cdrCcList, map, "./CSVPATH/" + user, "./公司线路对应" + "话单" + beginTime.substring(0, 10) + "_");

    }

    private List<Document> showDate(String line, String product, String beginTime, String endTime) {
        Map<String, Object> query_agent = new HashMap<String, Object>();
        query_agent.put("product", product);
        DBObject where_agent = new BasicDBObject();
        DBObject userKeys = new BasicDBObject();
        userKeys.put("_id", 1);
        userKeys.put("displayName", 1);
        userKeys.put("exten", 1);
        where_agent.putAll(query_agent);
        /*if(list==null){
            List<DBObject> list_agent = mongo.getDB("sj_data").getCollection("platform_user1210").find(where_agent, userKeys).toArray();
            list=list_agent;
        } */
        FindIterable<Document> test = Mongodbjdbc.MongGetDom().getCollection(
                "bill_cdr_query_cc").find(new Document().append("endTimeDisplay", new Document()
                .append("$gte", beginTime)
                .append("$lte", endTime)).append("line", line))
                .noCursorTimeout(true).limit(20);
        MongoCursor<Document> mongoCursor = test.iterator();
        List<Document> cdrCcList = new LinkedList<>();
        while (mongoCursor.hasNext()) {
            Document cdrCc = mongoCursor.next();
            cdrCc.put("callAddr", "" + cdrCc.get("callerProvince") + "-" + cdrCc.get("callerCity"));
            cdrCc.put("calledAddr", "" + cdrCc.get("calleeProvince") + "-" + cdrCc.get("calleeCity"));
            String agentId = (String) cdrCc.get("agentId");
            cdrCc.put("agentDisplayName", "");
            String agentExtenId = (String) cdrCc.get("agentId");
            cdrCc.put("agentExten", "");

            if (cdrCc.get("type") != null) {
                String dialType = cdrCc.get("type").toString();
                if (dialType.equals("normal")) {
                    dialType = "普通来电";
                } else if (dialType.equals("dialout")) {
                    dialType = "外呼去电";
                } else if (dialType.equals("transfer")) {
                    dialType = "转接来电";
                } else if (dialType.equals("dialTransfer")) {
                    dialType = "外呼转接";
                }
                cdrCc.put("type", dialType);
            }
            if (cdrCc.get("isLocal") != null) {
                String isLocal = cdrCc.get("isLocal").toString();
                if (isLocal.equals(true)) {
                    isLocal = "本地";
                } else {
                    isLocal = "长途";
                }
                cdrCc.put("isLocal", isLocal);
            }


            cdrCcList.add(cdrCc);
        }

        return cdrCcList;
    }


    private List<Document> showAccountDate(String account, String product, String beginTime, String endTime) {
        FindIterable<Document> test = Mongodbjdbc.MongGetDom().getCollection(
                "bill_cdr_query_cc").find(new Document().append("endTimeDisplay", new Document()
                .append("$gte", beginTime)
                .append("$lte", endTime)).append("account", account))
                .noCursorTimeout(true).limit(20);
        MongoCursor<Document> mongoCursor = test.iterator();
        List<Document> cdrCcList = new LinkedList<>();
        while (mongoCursor.hasNext()) {
            Document cdrCc = mongoCursor.next();
            cdrCc.put("callAddr", "" + cdrCc.get("callerProvince") + "-" + cdrCc.get("callerCity"));
            cdrCc.put("calledAddr", "" + cdrCc.get("calleeProvince") + "-" + cdrCc.get("calleeCity"));
            String agentId = (String) cdrCc.get("agentId");
            cdrCc.put("agentDisplayName", "");
            String agentExtenId = (String) cdrCc.get("agentId");
            cdrCc.put("agentExten", "");
            if (cdrCc.get("type") != null) {
                String dialType = cdrCc.get("type").toString();
                if (dialType.equals("normal")) {
                    dialType = "普通来电";
                } else if (dialType.equals("dialout")) {
                    dialType = "外呼去电";
                } else if (dialType.equals("transfer")) {
                    dialType = "转接来电";
                } else if (dialType.equals("dialTransfer")) {
                    dialType = "外呼转接";
                }
                cdrCc.put("type", dialType);
            }
            if (cdrCc.get("isLocal") != null) {
                String isLocal = cdrCc.get("isLocal").toString();
                if (isLocal.equals(true)) {
                    isLocal = "本地";
                } else {
                    isLocal = "长途";
                }
                cdrCc.put("isLocal", isLocal);
            }


            cdrCcList.add(cdrCc);
        }
        return cdrCcList;
    }
    private List<Document> showSMSDate(String account, String product, String beginTime, String endTime) {
        FindIterable<Document> test = Mongodbjdbc.MongGetDom().getCollection(
                "bill_cdr_query_cc").find(new Document().append("endTimeDisplay", new Document()
                .append("$gte", beginTime)
                .append("$lte", endTime)).append("account", account))
                .noCursorTimeout(true).limit(20);
        MongoCursor<Document> mongoCursor = test.iterator();
        List<Document> cdrCcList = new LinkedList<>();
        while (mongoCursor.hasNext()) {
            Document cdrCc = mongoCursor.next();
            cdrCcList.add(cdrCc);
        }
        return cdrCcList;
    }


    private List<Document> showXiaoHaoDate(String account, String beginTime, String endTime) {
        FindIterable<Document> xiaohao = Mongodbjdbc.MongGetDom().getCollection(
                "bill_yiketong_xiaohao_cdr_query").find(new Document().append("begin_time", new Document()
                .append("$gte", beginTime)
                .append("$lte", endTime)).append("account", account))
                .noCursorTimeout(true).limit(20);
        MongoCursor<Document> mongoCursor = xiaohao.iterator();
        List<Document> cdrCcList = new LinkedList<>();
        while (mongoCursor.hasNext()) {
            Document cdrCc = mongoCursor.next();
            cdrCcList.add(cdrCc);
        }
        return cdrCcList;
    }

    private List<Document> showSMSDate(String account, String beginTime, String endTime) {
        FindIterable<Document> xiaohao = Mongodbjdbc.MongGetDom().getCollection(
                "bill_sms_detail_cc_1").find(new Document().append("begin_time", new Document()
                .append("$gte", beginTime)
                .append("$lte", endTime)).append("account", account))
                .noCursorTimeout(true).limit(20);
        MongoCursor<Document> mongoCursor = xiaohao.iterator();
        List<Document> cdrCcList = new LinkedList<>();
        while (mongoCursor.hasNext()) {
            Document cdrCc = mongoCursor.next();
            cdrCcList.add(cdrCc);
        }
        return cdrCcList;
    }

    static {
        headMap.put("uniqueid", "话单编号");
        headMap.put("account", "账户编号");
        headMap.put("callAddr", "主叫地");
        headMap.put("calledAddr", "被叫地");
        headMap.put("cid", "主叫号码");
        headMap.put("did", "被叫号码");
        headMap.put("beginTimeDisplay", "开始时间");
        headMap.put("endTimeDisplay", "结束时间");
        headMap.put("seconds", "时长(秒)");
        headMap.put("minutes", "时长(分钟)");
        headMap.put("consume", "费用(元)");
        headMap.put("isLocal", "通信类型");
        headMap.put("type", "呼叫类型");
        headMap.put("product", "产品");
        headMap.put("line", "通道");
        headMap.put("route", "线路");
        headMap.put("agentDisplayName", "坐席名称");
        headMap.put("agentExten", "坐席工号");
        headMap.put("consume", "费用");
    }

    private static DBObject checkAgentId(List<DBObject> list_agent, String agentId) {
        for (int j = 0; j < list_agent.size(); j++) {

            if (list_agent.get(j) != null && list_agent.get(j).get("_id") != null && list_agent.get(j).get("_id").toString().equals(agentId)) {
                return list_agent.get(j);
            }
        }
        return null;
    }

}





