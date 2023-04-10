package yunnan;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.setting.Setting;
import com.jcraft.jsch.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class SFTPUtil {
    private ChannelSftp sftp;
    private Session session;
    private String username;
    private String password;
    private String privateKey;
    private String host;
    private int port;




    public SFTPUtil(String username, String password, String host, int port) {
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
    }


    public void login(){
        try {
            JSch jsch = new JSch();

            session = jsch.getSession(username, host, port);

            if (privateKey != null) {
                jsch.addIdentity(privateKey);// 设置私钥
            }

            if (password != null) {
                session.setPassword(password);
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");

            session.setConfig(config);
            session.connect();
            log.info("SFTP CONNECTION SUCCESSFUL");

            Channel channel = session.openChannel("sftp");
            channel.connect();

            sftp = (ChannelSftp) channel;
        } catch (JSchException e) {
            log.info("SFTP CONNECTION FAILED");
            e.printStackTrace();
        }
    }


    public void logout(){
        if (sftp != null) {
            if (sftp.isConnected()) {
                sftp.disconnect();
            }
        }
        if (session != null) {
            if (session.isConnected()) {
                session.disconnect();
            }
        }
    }

    public ArrayList<File> listFiles(String dir,String date) throws SftpException {
//        ArrayList<String> files = new ArrayList<String>();
//        sftp.cd(dir);
//        Vector<String> lss = sftp.ls("*");
//        for (int i = 0; i < lss.size(); i++) {
//            Object obj = lss.elementAt(i);
//            if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
//                ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) obj;
//                if (!entry.getAttrs().isDir()) {
//                    files.add(entry.getFilename());
//                }
//
//            }
//        }

        ArrayList<File> files = new ArrayList<>();

        sftp.cd(dir);
//        System.out.println("进入目录"+dir);
        log.info(" cd : "+dir);

        Vector<String> file_list = sftp.ls("*");

        String target_date;
        if (StrUtil.isBlank(date)){
            // yyyy-MM-dd
            target_date = LocalDate.now().toString();
        }else {
            target_date = date.trim();
        }

        log.info(" target Date ："+ target_date);

//        log.debug("获取 "+dir+" 下日期为 "+target_date+ "的数据");

        for (int i = 0; i < file_list.size(); i++) {

            Object obj = file_list.elementAt(i);
            String file_name = null;

            if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
                ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) obj;
                 file_name = entry.getFilename();
            }
//            System.out.println(file_name);
            File file = new File(new File(dir),file_name);
//            System.out.println(file.getAbsolutePath());
            SftpATTRS file_state = sftp.lstat(file.getAbsolutePath());
//            log.debug(file.getAbsolutePath());
//            System.out.println(file_state.getAtimeString());
            // Mon Feb 20 09:50:01 CST 2023    CST时间格式

            SimpleDateFormat sim1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
            SimpleDateFormat sim2 = new SimpleDateFormat("yyyy-MM-dd");

            String file_date = null;
            try {
                // getAtimeString() : 返回访问时间的字符串表示形式。
                // getMtimeString() : 返回修改时间的字符串表示形式。
                file_date = sim2.format(sim1.parse(file_state.getMtimeString()));
//                log.debug(file_date);

            } catch (ParseException e) {
                log.error("PARSE CST ERROR");
                e.printStackTrace();
            }

//            log.info( file_state.getMtimeString()+ " 日期  "+file_date +"  文件名 " + file_name);

            if (file_date.equals(target_date)){
                files.add(file);
                log.info("add file : "+ file.getName());
            }


        }

        return files;
    }

    public void download(File file, String savedir) throws SftpException, IOException {
        if (StrUtil.isNotBlank(savedir) && !file.isFile()){
            InputStream is = sftp.get(file.getAbsolutePath());
            File target_file = new File(savedir, file.getName());
//        log.debug("start download");
            FileOutputStream out = new FileOutputStream(target_file);
            IOUtils.copy(is,out);
            log.info("download successed :  " + file.getAbsolutePath() +"  to :"+ savedir );
            is.close();
            out.close();
        }else {
            log.error("target_dir is blank or saved_file is not file");
        }

    }

    public void delete(String fileName) throws SftpException {
        sftp.rm(fileName);
    }

    public long getFileSize(String srcSftpFilePath) {
        long fileSize;//文件大于等于0则存在
        try {
            SftpATTRS sftpATTRS = sftp.lstat(srcSftpFilePath);
            fileSize = sftpATTRS.getSize();
        } catch (Exception e) {
            fileSize = -1;//获取文件大小异常
            if (e.getMessage().toLowerCase().equals("no such file")) {
                fileSize = -2;//文件不存在
            }
        }
        return fileSize;
    }

    public String getLastModifiedTime(String srcSftpFilePath) {
        try {
        SftpATTRS sftpATTRS = sftp.lstat(srcSftpFilePath);
        int mTime = sftpATTRS.getMTime();
        Date lastModified = new Date(mTime * 1000L);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = format.format(lastModified);
        return time;
    } catch (Exception e) {
        e.printStackTrace();
    }
        return null;
}

}
