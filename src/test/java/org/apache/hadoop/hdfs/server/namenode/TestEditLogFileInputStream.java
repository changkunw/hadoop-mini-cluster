/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class TestEditLogFileInputStream {
  private static final Log LOG =
      LogFactory.getLog(TestEditLogFileInputStream.class);

  private final static File TEST_DIR = PathUtils
      .getTestDir(TestEditLogFileInputStream.class);


  /**
   * Regression test for HDFS-8965 which verifies that
   * FSEditLogFileInputStream#scanOp verifies Op checksums.
   */
  @Test(timeout=60000)
  public void testScanCorruptEditLog() throws Exception {
    Configuration conf = new Configuration();
    File editLog = new File("G:\\workspace\\neworkspace\\platform\\hdfs-cp\\hadoop-test\\src\\main\\resources\\xx.log");

    LOG.debug("Creating test edit log file: " + editLog);
    EditLogFileOutputStream elos = new EditLogFileOutputStream(conf,
                                                               editLog.getAbsoluteFile(), 8192);
    elos.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
    FSEditLogOp.OpInstanceCache cache = new FSEditLogOp.OpInstanceCache();
    FSEditLogOp.MkdirOp mkdirOp = FSEditLogOp.MkdirOp.getInstance(cache);
    mkdirOp.reset();
    mkdirOp.setRpcCallId(123);
    mkdirOp.setTransactionId(1);
    mkdirOp.setInodeId(789L);
    mkdirOp.setPath("/mydir");
    PermissionStatus perms = PermissionStatus.createImmutable(
            "myuser", "mygroup", FsPermission.createImmutable((short)0777));
    mkdirOp.setPermissionStatus(perms);
    elos.write(mkdirOp);
    mkdirOp.reset();
    mkdirOp.setRpcCallId(456);
    mkdirOp.setTransactionId(2);
    mkdirOp.setInodeId(123L);
    mkdirOp.setPath("/mydir2");
    perms = PermissionStatus.createImmutable(
            "myuser", "mygroup", FsPermission.createImmutable((short)0666));
    mkdirOp.setPermissionStatus(perms);
    elos.write(mkdirOp);
    elos.setReadyToFlush();
    elos.flushAndSync(false);
    elos.close();
    long fileLen = editLog.length();

    LOG.debug("Corrupting last 4 bytes of edit log file " + editLog +
        ", whose length is " + fileLen);
    RandomAccessFile rwf = new RandomAccessFile(editLog, "rw");
    rwf.seek(fileLen - 4);
    int b = rwf.readInt();
    rwf.seek(fileLen - 4);
    rwf.writeInt(b + 1);
    rwf.close();

    EditLogFileInputStream elis = new EditLogFileInputStream(editLog);
    Assert.assertEquals(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
                        elis.getVersion(true));
    Assert.assertEquals(1, elis.scanNextOp());
    LOG.debug("Read transaction 1 from " + editLog);
    try {
      elis.scanNextOp();
      Assert.fail("Expected scanNextOp to fail when op checksum was corrupt.");
    } catch (IOException e) {
      LOG.debug("Caught expected checksum error when reading corrupt " +
          "transaction 2", e);
      GenericTestUtils.assertExceptionContains("Transaction is corrupt.", e);
    }
    elis.close();
  }
}
