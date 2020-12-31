package pers.yzq.spark;

import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class JobHandler implements LineProcessor<String> {

    private String job = null;

    private Long total = 0L;

    private File nf = null;

    @Override
    public boolean processLine(String line) throws IOException {
        String [] splited = line.split("|", -1);

        if (job != splited[0]) {
            total += 1L;
            job = splited[0];
            nf = new File("/opt/zequnyu/res_2/" + splited[0]);

            if (total % 100000L == 0) System.out.println(total);
        }

        Files.append(splited[1] + "\n", nf, Charset.defaultCharset());
        return true;
    }

    @Override
    public String getResult() {
        return null;
    }
}
