package pers.yzq.spark;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class LocalHandler {

    public static void main(String[] args) throws IOException {
        File f = new File("/opt/zequnyu/res_1.txt");
        Files.readLines(f,
                Charset.defaultCharset(),
                new JobHandler());
    }
}
