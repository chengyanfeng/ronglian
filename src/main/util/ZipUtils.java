package main.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipFile;
import org.apache.tools.zip.ZipOutputStream;
import org.springframework.context.annotation.Configuration;

/**
 * 压缩或解压
 * 本测试例子使用的是ant-1.6.5.jar中的org.apache.tools.zip下的工具类
 * @author lisonglin
 * @date 2017年12月4日上午10:46:27
 */
@Configuration
public class ZipUtils {
    private static byte[] _byte = new byte[1024];

    private static final String ENCODE_UTF_8 = "UTF-8";

    /**
     * 压缩文件或路径
     *
     * @param zip      压缩的目的地址  例如：D://zipTest.zip
     * @param srcFiles 压缩的源文件
     */
    public static void zipFile(String zip, List<File> srcFiles) {
        try {
            if (zip.endsWith(".zip") || zip.endsWith(".ZIP")) {//判断是否为压缩后的文件后缀是否为.zip结尾
                ZipOutputStream _zipOut = new ZipOutputStream(new FileOutputStream(new File(zip)));
                _zipOut.setEncoding(ENCODE_UTF_8);//设置编码
                for (File _f : srcFiles) {
                    zipFile(zip, _zipOut, _f, "");
                }
                _zipOut.close();
            } else {
                System.out.println("target file[" + zip + "] is not .zip type file");
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        } catch (IOException e) {
            System.out.println(e);

        }
    }


    /**
     * @param zip     压缩的目的地址  例如：D://zipTest.zip
     * @param zipOut
     * @param srcFile 被压缩的文件
     * @param path    在zip中的相对路径
     * @throws IOException
     */
    private static void zipFile(String zip, ZipOutputStream zipOut, File srcFile, String path) throws IOException {
        System.out.println(" 开始压缩文件[" + srcFile.getName() + "]");
        if (!"".equals(path) && !path.endsWith(File.separator)) {
            path += File.separator;
        }
        if (!srcFile.exists()) {//测试此抽象路径名定义的文件或目录是否存在
            System.out.println("压缩失败，文件或目录 " + srcFile + " 不存在!");
        } else {
            if (!srcFile.getPath().equals(zip)) {
                if (srcFile.isDirectory()) {
                    File[] _files = srcFile.listFiles();//listFiles能够获取当前文件夹下的所有文件和文件夹，如果文件夹A下还有文件D，那么D也在childs里。
                    if (_files.length == 0) {
                        zipOut.putNextEntry(new ZipEntry(path + srcFile.getName() + File.separator));
                        zipOut.closeEntry();
                    } else {
                        for (File _f : _files) {
                            zipFile(zip, zipOut, _f, path + srcFile.getName());
                        }
                    }
                } else {
                    FileInputStream _in = new FileInputStream(srcFile);
                    zipOut.putNextEntry(new ZipEntry(path + srcFile.getName()));
                    int len = 0;
                    while ((len = _in.read(_byte)) > 0) {
                        zipOut.write(_byte, 0, len);
                    }
                    _in.close();
                    zipOut.closeEntry();
                }
            }
        }
    }

    /**
     * 解压缩ZIP文件，将ZIP文件里的内容解压到targetDIR目录下
     *
     * @param zipPath           待解压缩的ZIP文件名
     * @param descDir 目标目录
     */
    public static List<File> upzipFile(String zipPath, String descDir) {
        return upzipFile(new File(zipPath), descDir);
    }

    /**
     * 对.zip文件进行解压缩
     *
     * @param zipFile 解压缩文件
     * @param descDir 压缩的目标地址，如：D:\\测试 或 /mnt/d/测试
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static List<File> upzipFile(File zipFile, String descDir) {
        List<File> _list = new ArrayList<File>();
        try {
            if (!zipFile.exists()) {
                System.out.println("解压失败，文件 " + zipFile + " 不存在!");
                return _list;
            }
            ZipFile _zipFile = new ZipFile(zipFile, ENCODE_UTF_8);
            for (Enumeration entries = _zipFile.getEntries(); entries.hasMoreElements(); ) {
                ZipEntry entry = (ZipEntry) entries.nextElement();
                File _file = new File(descDir + File.separator + entry.getName());
                if (entry.isDirectory()) {
                    _file.mkdirs();
                } else {
                    File _parent = _file.getParentFile();
                    if (!_parent.exists()) {
                        _parent.mkdirs();
                    }
                    InputStream _in = _zipFile.getInputStream(entry);
                    OutputStream _out = new FileOutputStream(_file);
                    int len = 0;
                    while ((len = _in.read(_byte)) > 0) {
                        _out.write(_byte, 0, len);
                    }
                    _in.close();
                    _out.flush();
                    _out.close();
                    _list.add(_file);
                }
            }
        } catch (IOException e) {
        }
        return _list;
    }

    /**
     * 对临时生成的文件夹和文件夹下的文件进行删除
     */
    public static void deletefile(String delpath) {
        try {
            File file = new File(delpath);
            if (!file.isDirectory()) {//判断是不是一个目录
                file.delete();
            } else if (file.isDirectory()) {
                String[] filelist = file.list();
                for (int i = 0; i < filelist.length; i++) {
                    File delfile = new File(delpath + File.separator + filelist[i]);
                    if (!delfile.isDirectory()) {
                        delfile.delete();
                    } else if (delfile.isDirectory()) {
                        deletefile(delpath + File.separator + filelist[i]);//递归删除
                    }
                }
                file.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
