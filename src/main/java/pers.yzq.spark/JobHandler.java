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
        String filtered = line.replaceAll("[(|)]", "");
        String [] splited = filtered.split(",", -1);

        if (job != splited[0]) {
            job = splited[0];
            nf = new File("/opt/zequnyu/res_1/job_" + job);

            if (total % 100000 == 0) System.out.println(total);
        }
        String record = new StringBuilder().
                append(splited[1]).append(",").
                append(splited[2]).append(",").
                append(splited[3]).append(",").
                append(splited[4]).append(",").
                append(splited[5]).append(",").
                append(splited[6]).append(",").
                append(splited[7]).append(",").
                append(splited[8]).append(",").
                append(splited[9]).append(",").
                append(splited[10]).append(",").
                append(splited[11]).append(",").
                append(splited[12]).append(",").toString();
        Files.append(record, nf, Charset.defaultCharset());
        return true;
    }

    @Override
    public String getResult() {
        return null;
    }
}
