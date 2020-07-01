
package io.platform.hadoop;

import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.util.DataChecksum;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class MetaDataTest {
    public static void main(String[] args) throws IOException {
        BlockMetadataHeader blockMetadataHeader = BlockMetadataHeader.readHeader(new FileInputStream(new File("G:\\hadoopdata\\dfs\\data\\data1\\current\\BP-171310559-169.254.250.200-1579228630883\\current\\finalized\\subdir0\\subdir0\\blk_1073741863_1039.meta")));
        short version = blockMetadataHeader.getVersion();
        System.out.println(version);
        DataChecksum checksum = blockMetadataHeader.getChecksum();
        int checksumSize = checksum.getChecksumSize();
        System.out.println(checksumSize);
        System.out.println(new String(checksum.getHeader()));
    }
}
