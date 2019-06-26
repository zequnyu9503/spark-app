package pers.yzq.spark;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: YZQ
 * @date 2019/5/20
 */
public class Several {

    @Test
    public void rowKeyTrans(){
        String s = "111-18181811";
        String [] rkPrefix = {"e","a", "b","c","d"};
        String[] vt = s.split("-");
        Long timestamp = Long.valueOf(vt[1]);
        Long p = timestamp % rkPrefix.length;
        String rowKey = rkPrefix[p.intValue()] + String.format("%0" + (10) + "d", timestamp);
        // (rowKey, (data, timestamp))
        System.out.println(rowKey);
    }

    @Test
    public void getSplits(){
        List list = new ArrayList(){{
            add("b0000000000");
            add("c0000000000");
            add("d0000000000");
            add("e0000000000");
            add("f0000000000");
            add("g0000000000");
            add("h0000000000");
            add("i0000000000");
            add("j0000000000");
        }};
        HBaseBulkLoad load = new HBaseBulkLoad();
        load.getSplits(list);
    }
}
