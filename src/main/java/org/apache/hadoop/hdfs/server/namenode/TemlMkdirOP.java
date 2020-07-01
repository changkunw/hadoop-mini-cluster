
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.util.XMLUtils;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

public class TemlMkdirOP extends FSEditLogOp {

    public TemlMkdirOP() {
        super(FSEditLogOpCodes.OP_MKDIR);
    }

    @Override
    void resetSubFields() {
        // nop
    }

    @Override
    void readFields(DataInputStream in, int logVersion) throws IOException {
        throw new IOException("cannot decode GarbageMkdirOp");
    }

    @Override
    public void writeFields(DataOutputStream out) throws IOException {
        // write in some garbage content
        Random random = new Random();
        byte[] content = new byte[random.nextInt(16) + 1];
        random.nextBytes(content);
        out.write(content);
    }

    @Override
    protected void toXml(ContentHandler contentHandler) throws SAXException {
        throw new UnsupportedOperationException(
                "Not supported for GarbageMkdirOp");
    }

    @Override
    void fromXml(XMLUtils.Stanza st) throws XMLUtils.InvalidXmlException {
        throw new UnsupportedOperationException(
                "Not supported for GarbageMkdirOp");
    }
}
