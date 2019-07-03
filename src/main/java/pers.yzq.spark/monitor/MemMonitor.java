package pers.yzq.spark.monitor;

import pers.yzq.spark.YLogger;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class MemMonitor implements Runnable {

    private final double maga = 1024*1024;
    private final double giga = 1024*maga;

    private OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();
    @Override
    public void run() {
        while (!Thread.interrupted()) {
            long totalMem = Runtime.getRuntime().totalMemory();
            YLogger.ylogInfo(this.getClass().getSimpleName(), "totalMem " + totalMem / giga);
            long freeMem = Runtime.getRuntime().freeMemory();
            YLogger.ylogInfo(this.getClass().getSimpleName(), "freeMem " + freeMem / giga);
        }
    }


}
