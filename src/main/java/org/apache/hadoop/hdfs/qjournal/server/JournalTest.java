
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.TemlMkdirOP;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.DataOutputBuffer;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class JournalTest {

    private static final StorageErrorReporter mockErrorReporter = Mockito.mock(StorageErrorReporter.class);
    static NamespaceInfo namespaceInfo = new NamespaceInfo(12345, "cluster1", "mu-bh", 0L);
    static File TEST_LOG_DIR = new File("G:\\hadoopdata\\journaltest");
    static String journalId = "test-journal";
    static Configuration conf = new Configuration();

    public static void main(String[] args) throws IOException {
        JournalTest journalTest = new JournalTest();
        Journal journal = journalTest.createJournal();
        journalTest.scanEditLog(journal);
    }

    private void scanEditLog(Journal journal) throws IOException {
        journal.startLogSegment(makeRI(1), 1, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION - 1);
        int numTxns = 5;
        byte[] ops = createGabageTxns(1, numTxns);
        journal.journal(makeRI(2), 1, 1, numTxns, ops);
        QJournalProtocolProtos.SegmentStateProto segmentInfo = journal.getSegmentInfo(1);
        System.out.println(segmentInfo.getIsInProgress());
        System.out.println(segmentInfo.getEndTxId());

        journal.finalizeLogSegment(makeRI(3), 1, numTxns);
        QJournalProtocolProtos.SegmentStateProto segmentInfo1 = journal.getSegmentInfo(1);
        System.out.println(segmentInfo1.getIsInProgress());
        System.out.println(segmentInfo1.getStartTxId());
        System.out.println(segmentInfo1.getEndTxId());
    }


    /**
     * Generate byte array representing a set of GarbageMkdirOp
     */
    public static byte[] createGabageTxns(long startTxId, int numTxns)
            throws IOException {
        DataOutputBuffer buf = new DataOutputBuffer();
        FSEditLogOp.Writer writer = new FSEditLogOp.Writer(buf);

        for (long txid = startTxId; txid < startTxId + numTxns; txid++) {
            FSEditLogOp op = new TemlMkdirOP();
            op.setTransactionId(txid);
            writer.writeOp(op);
        }
        return Arrays.copyOf(buf.getData(), buf.getLength());
    }

    private static RequestInfo makeRI(int serial) {
        return new RequestInfo(journalId, 1, serial, 0);
    }

    private Journal createJournal() throws IOException {
        FileUtil.fullyDelete(TEST_LOG_DIR);
        Journal journal = new Journal(conf, TEST_LOG_DIR, journalId, HdfsServerConstants.StartupOption.REGULAR, mockErrorReporter);
        journal.format(namespaceInfo);
        return journal;
    }
}
