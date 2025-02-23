import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HDFSFileUploader {
    private static final String HDFS_URI = "hdfs://namenode:9000";
    private static final int THREAD_COUNT = 4;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java HDFSFileUploader <local_dir> <hdfs_dir>");
            return;
        }

        String localDir = args[0];
        String hdfsDir = args[1];

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        UserGroupInformation.setConfiguration(conf);

        FileSystem fs = FileSystem.get(conf);
        File folder = new File(localDir);
        File[] files = folder.listFiles();
        if (files == null) {
            System.out.println("No files found in directory");
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        for (File file : files) {
            executor.execute(() -> uploadFile(fs, file, hdfsDir));
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        for (File file : files) {
            verifyIntegrity(fs, file, hdfsDir);
        }
    }

    private static void uploadFile(FileSystem fs, File file, String hdfsDir) {
        try (InputStream in = new FileInputStream(file);
             OutputStream out = fs.create(new Path(hdfsDir + "/" + file.getName()))) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
            System.out.println("Uploaded: " + file.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void verifyIntegrity(FileSystem fs, File file, String hdfsDir) {
        try {
            String localMd5 = calculateMD5(file);
            Path hdfsPath = new Path(hdfsDir + "/" + file.getName());
            FileStatus fileStatus = fs.getFileStatus(hdfsPath);
            InputStream in = fs.open(hdfsPath);
            String hdfsMd5 = calculateMD5(in);
            in.close();
            
            if (localMd5.equals(hdfsMd5)) {
                System.out.println("Integrity check passed for: " + file.getName());
            } else {
                System.out.println("Integrity check failed for: " + file.getName());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String calculateMD5(File file) throws IOException, NoSuchAlgorithmException {
        try (InputStream in = new FileInputStream(file)) {
            return calculateMD5(in);
        }
    }

    private static String calculateMD5(InputStream in) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) > 0) {
            md.update(buffer, 0, bytesRead);
        }
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}