package main.util;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.beanutils.BeanUtils;
import org.springframework.context.annotation.Configuration;


public class util {
    //获取指定时间日期
    public static List<Date> findDates(String beginTime, String endTime, SimpleDateFormat sdf) {
        Date dStart = null;
        Date dEnd = null;
        try {
            dStart = sdf.parse(beginTime);
            dEnd = sdf.parse(endTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cStart = Calendar.getInstance();
        cStart.setTime(dStart);

        List dateList = new ArrayList();
        //别忘了，把起始日期加上
        dateList.add(dStart);
        // 此日期是否在指定日期之后
        while (dEnd.after(cStart.getTime())) {
            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量
            cStart.add(Calendar.DAY_OF_MONTH, 1);
            dateList.add(cStart.getTime());
        }
        return dateList;
    }
    public static File createCSVFile(List exportData, LinkedHashMap map, String outPutPath, String fileName) {
        File csvFile = null;
        BufferedWriter csvFileOutputStream = null;
        try {
            File file = new File(outPutPath);
            if (!file.exists()) {
                file.mkdir();
            }
            //定义文件名格式并创建
            csvFile = File.createTempFile(fileName, ".csv", new File(outPutPath));
            System.out.println("csvFile：" + csvFile);
            // UTF-8使正确读取分隔符","
            //如果生产文件乱码，windows下用gbk，linux用UTF-8
            csvFileOutputStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                    csvFile), "UTF-8"), 1024);
            System.out.println("csvFileOutputStream：" + csvFileOutputStream);
            // 写入文件头部
            for (Iterator propertyIterator = map.entrySet().iterator(); propertyIterator.hasNext(); ) {
                java.util.Map.Entry propertyEntry = (java.util.Map.Entry) propertyIterator.next();
                csvFileOutputStream.write((String) propertyEntry.getValue() != null ? (String) propertyEntry.getValue() : "");
                if (propertyIterator.hasNext()) {
                    csvFileOutputStream.write(",");
                }
            }
            csvFileOutputStream.newLine();
            // 写入文件内容
            for (Iterator iterator = exportData.iterator(); iterator.hasNext(); ) {
                Object row = (Object) iterator.next();
                for (Iterator propertyIterator = map.entrySet().iterator(); propertyIterator.hasNext(); ) {
                    java.util.Map.Entry propertyEntry = (java.util.Map.Entry) propertyIterator.next();
                   String insertRow= (String) BeanUtils.getProperty(row, (String) propertyEntry.getKey());
                   if(insertRow==null){
                       insertRow="";
                   }
                    csvFileOutputStream.write(insertRow);
                    if (propertyIterator.hasNext()) {
                        csvFileOutputStream.write(",");
                    }
                }
                if (iterator.hasNext()) {
                    csvFileOutputStream.newLine();
                }
            }
            csvFileOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                csvFileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return csvFile;
    }
public static boolean creatFile(String path){
    File file=new File(path);
    if(!file.exists()){//如果文件夹不存在
        file.mkdir();//创建文件夹
    }
    try{//异常处理
        //如果Qiju_Li文件夹下没有Qiju_Li.txt就会创建该文件
        BufferedWriter bw=new BufferedWriter(new FileWriter(path+"/1.txt"));
        System.out.println("创建1.txt 成功");
        bw.write("Hello I/O!");//在创建好的文件中写入"Hello I/O"
        bw.close();//一定要关闭文件
    }catch(IOException e){
        System.out.println("创建1.txt 失败");
        e.printStackTrace();
    }
    return true;
}


    /**
     * 删除文件，可以是文件或文件夹
     *
     * @param fileName
     *            要删除的文件名
     * @return 删除成功返回true，否则返回false
     */
    public static boolean delete(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("删除文件失败:" + fileName + "不存在！");
            return false;
        } else {
            if (file.isFile())
                return deleteFile(fileName);
            else
                return deleteDirectory(fileName);
        }
    }

    public static boolean ifEx(String fileName) {
        File file=new File(fileName);
        if(!file.exists()){//如果文件夹不存在
            return  false;//创建文件夹
        }else {
            return  true;
        }
    }

    /**
     * 删除单个文件
     *
     * @param fileName
     *            要删除的文件的文件名
     * @return 单个文件删除成功返回true，否则返回false
     */
    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        // 如果文件路径所对应的文件存在，并且是一个文件，则直接删除
        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                System.out.println("删除单个文件" + fileName + "成功！");
                return true;
            } else {
                System.out.println("删除单个文件" + fileName + "失败！");
                return false;
            }
        } else {
            System.out.println("删除单个文件失败：" + fileName + "不存在！");
            return false;
        }
    }
    /**
     * 删除目录及目录下的文件
     *
     * @param dir
     *            要删除的目录的文件路径
     * @return 目录删除成功返回true，否则返回false
     */
    public static boolean deleteDirectory(String dir) {
        // 如果dir不以文件分隔符结尾，自动添加文件分隔符
        if (!dir.endsWith(File.separator))
            dir = dir + File.separator;
        File dirFile = new File(dir);
        // 如果dir对应的文件不存在，或者不是一个目录，则退出
        if ((!dirFile.exists()) || (!dirFile.isDirectory())) {
            System.out.println("删除目录失败：" + dir + "不存在！");
            return false;
        }
        boolean flag = true;
        // 删除文件夹中的所有文件包括子目录
        File[] files = dirFile.listFiles();
        for (int i = 0; i < files.length; i++) {
            // 删除子文件
            if (files[i].isFile()) {
                flag = deleteFile(files[i].getAbsolutePath());
                if (!flag)
                    break;
            }

        }
       return true;

    }

    /*
     * 函数名：getFilegetFileName
     * 作用：使用递归，输出指定文件夹内的所有文件
     * 参数：path：文件夹路径   deep：表示文件的层次深度，控制前置空格的个数
     * 前置空格缩进，显示文件层次结构
     */
    public static List<String> getFileName(String path,int deep) {
        List<String> list=new ArrayList();
        // 获得指定文件对象
        File file = new File(path);
        // 获得该文件夹内的所有文件
        File[] array = file.listFiles();

        for (int i = 0; i < array.length; i++) {
            if (array[i].isFile())//如果是文件
            {
                for (int j = 0; j < deep; j++)//输出前置空格
                    System.out.print(" ");
                // 只输出文件名字
                list.add(array[i].getName());
                System.out.println(array[i].getName());
                // 输出当前文件的完整路径
                // System.out.println("#####" + array[i]);
                // 同样输出当前文件的完整路径   大家可以去掉注释 测试一下
                // System.out.println(array[i].getPath());
            } else if (array[i].isDirectory())//如果是文件夹
            {
                for (int j = 0; j < deep; j++)//输出前置空格
                    System.out.print(" ");

                System.out.println(array[i].getName());
                //System.out.println(array[i].getPath());
                //文件夹需要调用递归 ，深度+1
                /* getFile(array[i].getPath(),deep+1);*/
            }
        }
        return list;
    }

}
