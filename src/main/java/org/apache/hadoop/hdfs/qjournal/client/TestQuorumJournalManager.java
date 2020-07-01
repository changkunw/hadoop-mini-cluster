
package org.apache.hadoop.hdfs.qjournal.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.MiniClusterExample;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TestQuorumJournalManager {
    private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
            12345, "mycluster", "my-bp", 0L);
    private final URI uri;
    public static final String kerberosPath = "G:\\workspace\\neworkspace\\platform\\hdfs-cp\\hadoop-test\\src\\main\\resources\\";

    private TestQuorumJournalManager() throws URISyntaxException {
        uri = new URI("qjournal://sp-dev-1:8485/cluster1");
    }

    public static void main(String[] args) throws URISyntaxException, IOException, TimeoutException, InterruptedException {
        TestQuorumJournalManager testQuorumJournalManager = new TestQuorumJournalManager();
        //        testQuorumJournalManager.test1();
        //        testQuorumJournalManager.test2();
        testQuorumJournalManager.testx();
    }

    private void test1() throws IOException {
        List<EditLogInputStream> journalStreams = createJournalStreams(FAKE_NSINFO, 1);
        System.out.println(journalStreams.size());
    }


    public List<EditLogInputStream> createJournalStreams(NamespaceInfo namespaceInfo, long txid) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication", "Kerberos");
        configuration.set("dfs.journalnode.kerberos.principal", "jn/_HOST@PANEL.COM");
        System.setProperty("java.security.krb5.conf", "G:\\workspace\\neworkspace\\platform\\hdfs-cp\\hadoop-test\\src\\main\\resources\\krb5.conf");
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("nn/10.202.239.38@PANEL.COM", "G:\\workspace\\neworkspace\\platform\\hdfs-cp\\hadoop-test\\src\\main\\resources\\local.keytab");
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        System.out.println(loginUser.getUserName());
        QuorumJournalManager quorumJournalManager = new QuorumJournalManager(configuration, uri, namespaceInfo);
        List<EditLogInputStream> streams = new ArrayList<>();
        quorumJournalManager.selectInputStreams(streams, txid, false);
        return streams;
    }

    public void test2() throws InterruptedException, IOException, TimeoutException {
        MiniDFSCluster miniDFSCluster = MiniClusterExample.createMiniDFSCluster();
        FSNamesystem namesystem = miniDFSCluster.getNamesystem();
        NamespaceInfo namespaceInfo = namesystem.getFSImage().getStorage().getNamespaceInfo();
        FSImage fsImage = namesystem.getFSImage();
        long lastWrittenTxId = fsImage.getEditLog().getLastWrittenTxId();
        System.out.println(lastWrittenTxId);
        List<EditLogInputStream> journalStreams = createJournalStreams(namespaceInfo, lastWrittenTxId);
        List<EditLogInputStream> streams = new ArrayList<>();
        streams.add(journalStreams.get(0));
        streams.add(journalStreams.get(1));
        streams.add(journalStreams.get(2));
        streams.add(journalStreams.get(3));
        streams.add(journalStreams.get(4));
        streams.add(journalStreams.get(5));
        streams.add(journalStreams.get(6));
        streams.add(journalStreams.get(7));
        fsImage.loadEditsSync(streams, namesystem);
        namesystem.rollEditLog();
    }

    public void testx() throws IOException {
        Configuration configuration = new Configuration();
        List<EditLogInputStream> streams = new ArrayList<>();
        QuorumJournalManager quorumJournalManager = new QuorumJournalManager(configuration, uri, FAKE_NSINFO);
        quorumJournalManager.selectInputStreams(streams, 1, false);
        System.out.println(streams.size());

    }
}
