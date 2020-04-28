package com.github.myetl.fiflow.io.mysqlbinlog;

import com.alibaba.otter.canal.common.alarm.LogAlarmHandler;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.parse.index.MemoryLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.sink.CanalEventSink;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * for test
 */
public class Main {

    public static void main(String[] args) {
        MysqlEventParser controller = new MysqlEventParser();

        controller.setSlaveId(11l);

        controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress("127.0.0.1", 3306),
                "root", "root"));
        controller.setConnectionCharset("UTF-8");

        controller.setDestination("haha");
        controller.setParallel(false);
        controller.setParallelBufferSize(1024);
        controller.setParallelThreadSize(1);
        controller.setIsGTIDMode(false);


        EntryPosition startPosition = new EntryPosition();

        startPosition.setJournalName("mysql-bin.000148");
        startPosition.setPosition(10436856l);
        startPosition.setTimestamp(System.currentTimeMillis());

        controller.setMasterPosition(startPosition);

        CanalLogPositionManager pm = new MemoryLogPositionManager();
        pm.start();


        controller.setLogPositionManager(pm);
        controller.setEventFilter(new AviaterRegexFilter("flink.student,flink.stuout"));

        controller.setAlarmHandler(new LogAlarmHandler());

        CanalEventSink<List<CanalEntry.Entry>> sink = new Sink();
        controller.setEventSink(sink);

        controller.setDefaultConnectionTimeoutInSeconds(60);
        controller.start();
    }
}
