package main.controller;

import main.model.User;
import main.serverice.QueryData;
import main.util.util;
import org.bson.Document;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Controller
public class MainController {
    @RequestMapping(value = "/", method = {RequestMethod.GET}, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseBody
    public List<Document> queryLine(@RequestParam(value = "beginTime", required = false) String beginTime, @RequestParam(value = "endTime", required = false) String endTime, @RequestParam(value = "line", required = false) String line, @RequestParam(value = "user", required = true) String user) {
        if (user == null) {
            return null;
        }
        QueryData queryLineData = new QueryData();
        beginTime = beginTime + " 00:00:00";
        endTime = endTime + " 00:00:00";
        List<Document> list = queryLineData.getShow(beginTime, endTime, line);

        return list;
    }

    @RequestMapping(value = "/export", method = {RequestMethod.GET}, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseBody
    public User queryLineDate(@RequestParam(value = "beginTime", required = false) String beginTime, @RequestParam(value = "endTime", required = false) String endTime, @RequestParam(value = "line", required = false) String line, @RequestParam(value = "user", required = true) String user) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
        QueryData queryLineData = new QueryData();
        beginTime = beginTime + " 00:00:00";
        endTime = endTime + " 00:00:00";
        User userR=new User();
        List<Date> dateList = util.findDates(beginTime, endTime, sdf);
        if (dateList.size() > 31) {
            userR.setFlag("false");
            return userR;
        }
        boolean ex = util.ifEx("./CSVPATH/" + user + "/1.txt");
        if (ex) {
            userR.setFlag("ex");
            return userR;
        } else {
            String flag = queryLineData.getData(beginTime, endTime, line, user);
            userR.setFlag(flag);
            return userR;
        }

    }

    @RequestMapping(value = "/queryAccount", method = {RequestMethod.GET}, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseBody
    public List<Document> queryAccoun(@RequestParam(value = "beginTime", required = false) String beginTime, @RequestParam(value = "endTime", required = false) String endTime, @RequestParam(value = "account", required = false) String account, @RequestParam(value = "user", required = true) String user) {
        if (user == null) {
            return null;
        }
        QueryData queryLineData = new QueryData();
        beginTime = beginTime + " 00:00:00";
        endTime = endTime + " 00:00:00";
        List<Document> list = queryLineData.getAccouontShow(beginTime, endTime, account);

        return list;
    }

    @RequestMapping(value = "/exportAccount", method = {RequestMethod.GET}, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseBody
    public User queryAccountDate(@RequestParam(value = "beginTime", required = false) String beginTime, @RequestParam(value = "endTime", required = false) String endTime, @RequestParam(value = "account", required = false) String account, @RequestParam(value = "user", required = true) String user) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
        QueryData queryLineData = new QueryData();
        beginTime = beginTime + " 00:00:00";
        endTime = endTime + " 00:00:00";
        List<Date> dateList = util.findDates(beginTime, endTime, sdf);
        User userRe=new User();
        if (dateList.size() > 31) {
            userRe.setFlag("false");
            return userRe;
        }
        boolean ex = util.ifEx("./CSVPATH/" + user + "/1.txt");
        if (ex) {
            userRe.setFlag("ex");
            return userRe;
        } else {
            String flag = queryLineData.getAccountData(beginTime, endTime, account, user);
            userRe.setFlag("flag");
            return userRe;
        }

    }


    @RequestMapping(value = "/login", method = {RequestMethod.POST}, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseBody
    public User login(@RequestParam(value = "username", required = false) String username, @RequestParam(value = "password", required = false) String password) {
        User user = new User();
        switch (username) {
            case "one":
                if (password.equals("one123456")) {
                    user.setFlag("ok");
                    user.setName("one");
                } else {
                    user.setFlag("false");
                }
                break;
            case "two":
                if (password.equals("two123456")) {
                    user.setFlag("ok");
                    user.setName("two");
                } else {
                    user.setFlag("false");
                }
                break;
            case "three":
                if (password.equals("three123456")) {
                    user.setFlag("ok");
                    user.setName("three");
                } else {
                    user.setFlag("false");
                }
            default:
                user.setFlag("err");
                break;
        }

        return user;
    }

    @RequestMapping("/download")
    public String downloadFile(HttpServletRequest request, HttpServletResponse response, @RequestParam(value = "down", required = false) String down, @RequestParam(value = "user", required = true) String user) {
        String fileName = down;// 设置文件名，根据业务需要替换成要下载的文件名
        if (fileName != null) {
            //设置文件路径
            String realPath = "./ZIP/" + user + "/";
            File file = new File(realPath, fileName);
            if (file.exists()) {
                response.setContentType("application/force-download");// 设置强制下载不打开
                response.addHeader("Content-Disposition", "attachment;fileName=" + fileName);// 设置文件名
                byte[] buffer = new byte[1024];
                FileInputStream fis = null;
                BufferedInputStream bis = null;
                try {
                    fis = new FileInputStream(file);
                    bis = new BufferedInputStream(fis);
                    OutputStream os = response.getOutputStream();
                    int i = bis.read(buffer);
                    while (i != -1) {
                        os.write(buffer, 0, i);
                        i = bis.read(buffer);
                    }
                    System.out.println("success");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (bis != null) {
                        try {
                            bis.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    if (fis != null) {
                        try {
                            fis.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        return null;
    }

    @RequestMapping("/findExport")
    @ResponseBody
    public List<User> downloadFile(@RequestParam(value = "user", required = true) String user) {
        List<User> list = new ArrayList<>();
        boolean ex = util.ifEx("./CSVPATH/" + user + "/1.txt");
        if (ex) {
            User u = new User();
            u.setFileName("未知");
            u.setName(user);
            u.setFlag("正在导出");
            list.add(u);
        }
        List<String> nameList = util.getFileName("./ZIP/" + user, 1);
        for (String name : nameList) {
            User ulist = new User();
            ulist.setName(user);
            ulist.setFlag("导出成功");
            ulist.setFileName(name);
            list.add(ulist);
        }

        return list;


    }

}
