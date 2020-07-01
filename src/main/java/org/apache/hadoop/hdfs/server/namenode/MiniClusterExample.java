
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MiniClusterExample {

    public static void main(String[] args) throws InterruptedException, TimeoutException, IOException {
        MiniDFSCluster miniDFSCluster = createMiniDFSCluster();
        //        DistributedFileSystem fileSystem = miniDFSCluster.getFileSystem();
        //        fileSystem.mkdir(new Path("/tmp/xxx"), FsPermission.getDefault());
        //        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        //        for (FileStatus fileStatus : fileStatuses) {
        //            System.out.println("file Name is :" + fileStatus.getPath().getName());
        //        }
        //        Path src = new Path("G:\\hadoopdata\\abcd\\zeta-hive-1.0.jar");
        //        Path dst = new Path("/tmp/hive-exec-2.2.1.jar");
        //        fileSystem.copyFromLocalFile(src, dst);
    }

    public static MiniDFSCluster createMiniDFSCluster() throws InterruptedException, TimeoutException, IOException {
        final Configuration conf = new HdfsConfiguration();
        final int numDatanodes = 1;
        final int defaultBlockSize = 2048576;
        final int blocks = 100;
        final int blocksSize = 1024;
        final int fileLen = blocks * blocksSize;
        final long capcacity = defaultBlockSize * 2 * fileLen;
        final long[] capacities = new long[]{capcacity, 2 * capcacity};
        MiniClusterExample miniClusterExample = new MiniClusterExample();
        MiniDFSCluster cluster = miniClusterExample.newCluster(
                conf,
                numDatanodes,
                capacities,
                defaultBlockSize,
                fileLen);
        return cluster;
    }

    public MiniDFSCluster newCluster(final Configuration conf,
                                     final int numDatanodes,
                                     final long[] storageCapacities,
                                     final int defaultBlockSize,
                                     final int fileLen)
            throws IOException, InterruptedException, TimeoutException {

        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, defaultBlockSize);
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, defaultBlockSize);
        conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);

        //        final String fileName = "/" + UUID.randomUUID().toString();
        //        final Path filePath = new Path(fileName);

        Preconditions.checkNotNull(storageCapacities);
        Preconditions.checkArgument(
                storageCapacities.length == 2,
                "need to specify capacities for two storages.");

        /* Write a file and restart the cluster */
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                .numDataNodes(numDatanodes)
                .storageCapacities(storageCapacities)
                .storageTypes(new StorageType[]{StorageType.DISK})
                .storagesPerDatanode(1)
                .nameNodeHttpPort(50070)
                .format(true)
                .build();
        cluster.waitActive();

        //        final short replicationFactor = (short) 1;
        //        final Random r = new Random();
        //        FileSystem fs = cluster.getFileSystem(0);
        //        DFSTestUtil.createFile(
        //                fs,
        //                filePath,
        //                fileLen,
        //                replicationFactor,
        //                r.nextLong());
        //        DFSTestUtil.waitReplication(fs, filePath, replicationFactor);

        return cluster;
    }
}
