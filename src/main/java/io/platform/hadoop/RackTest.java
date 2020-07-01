
package io.platform.hadoop;

import io.platform.hadoop.cluster.NodeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RackTest {

    public static final Map<String, Integer> workIngNodes = new HashMap<>();

    static {
        workIngNodes.put("10.200.64.125", 1);
    }

    public static void main(String[] args) throws IOException {
        Map<String, Set<NodeInfo>> racks = clusterArc();
        printArc(racks);
        List<NodeInfo> nodeInfos = sortRacksByRemainByRemain(racks);
        for (NodeInfo nodeInfo : nodeInfos) {
            System.out.println(nodeInfo.toString());
        }
        System.out.println(choseOne(racks));
    }

    public static String choseOne(Map<String, Set<NodeInfo>> racks) {
        List<NodeInfo> nodeInfos = sortRacksByRemainByRemain(racks);
        Long reduce = nodeInfos.stream().mapToLong(NodeInfo::getRemaining).sum();
        System.out.println(reduce);
        int sum = workIngNodes.values().stream().mapToInt(x -> x).sum() * 2;
        Map<String, Double> factor = workIngNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue() * 1.0 / sum));
        Map<String, Double> collect = nodeInfos.stream().collect(Collectors.toMap(NodeInfo::getIp, nodeInfo -> {
            if (factor.containsKey(nodeInfo.getIp())) {
                return nodeInfo.getRemaining() * 1.0 / reduce * factor.get(nodeInfo.getIp());
            } else {
                return nodeInfo.getRemaining() * 1.0 / reduce;
            }
        }));

        List<Map.Entry<String, Double>> list = new ArrayList<>(collect.entrySet());
        Collections.sort(list, (o1, o2) -> -Double.compare(o1.getValue(), o2.getValue()));

        return list.get(0).getKey();
    }

    public static void printArc(Map<String, Set<NodeInfo>> racks) {
        for (String s : racks.keySet()) {
            System.out.println("rack : " + s);
            Set<NodeInfo> nodeInfos = racks.get(s);
            for (NodeInfo nodeInfo : nodeInfos) {
                System.out.println("    Node  -> " + nodeInfo.toString());
            }
        }
    }

    public static List<NodeInfo> sortRacksByRemainByRemain(Map<String, Set<NodeInfo>> racks) {
        List<NodeInfo> nodeInfos = new ArrayList<>();
        for (Set<NodeInfo> value : racks.values()) {
            nodeInfos.addAll(value);
        }
        Collections.sort(nodeInfos, (o1, o2) -> -Long.compare(o1.getRemaining(), o2.getRemaining()));
        return nodeInfos;
    }


    public static Map<String, Set<NodeInfo>> clusterArc() throws IOException {
        System.setProperty("java.security.krb5.conf", "G:\\workspace\\neworkspace\\platform\\hdfs-cp\\hadoop-test\\src\\main\\resources\\krb5.conf");
        Configuration configuration = new Configuration();
        configuration.addResource("hdfs-site.xml");
        configuration.addResource("core-site.xml");
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("hdfs-panelsp@PANEL.COM", "G:\\workspace\\neworkspace\\platform\\hdfs-cp\\hadoop-test\\src\\main\\resources\\hdfs.headless.keytab");
        FileSystem fileSystem = FileSystem.get(configuration);
        DistributedFileSystem fs = (DistributedFileSystem) fileSystem;
        DatanodeInfo[] dataNodeStats = fs.getDataNodeStats();
        Map<String, Set<NodeInfo>> racks = new HashMap<>();
        for (DatanodeInfo dataNodeStat : dataNodeStats) {
            String networkLocation = dataNodeStat.getNetworkLocation();
            if (!racks.containsKey(networkLocation)) {
                Set<NodeInfo> nodeInfos = new HashSet<>();
                racks.put(networkLocation, nodeInfos);
            }
            NodeInfo nodeInfo = new NodeInfo();
            nodeInfo.setCapacity(dataNodeStat.getCapacity() / 1024 / 1024 / 1024);
            nodeInfo.setDfsUsed(dataNodeStat.getDfsUsed() / 1024 / 1024 / 1024);
            nodeInfo.setRemaining(dataNodeStat.getRemaining() / 1024 / 1024 / 1024);
            nodeInfo.setHostName(dataNodeStat.getHostName());
            nodeInfo.setIp(dataNodeStat.getIpAddr());
            nodeInfo.setRackInfo(networkLocation);
            racks.get(networkLocation).add(nodeInfo);
        }
        return racks;
    }
}
