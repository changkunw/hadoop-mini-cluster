
package io.platform.hadoop.cluster;

import com.google.common.base.Objects;

public class NodeInfo {
    private String ip;
    private String hostName;
    private long dfsUsed;
    private long capacity;
    private String rackInfo;
    private long remaining;

    public long getRemaining() {
        return remaining;
    }

    public void setRemaining(long remaining) {
        this.remaining = remaining;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public long getDfsUsed() {
        return dfsUsed;
    }

    public void setDfsUsed(long dfsUsed) {
        this.dfsUsed = dfsUsed;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public String getRackInfo() {
        return rackInfo;
    }

    public void setRackInfo(String rackInfo) {
        this.rackInfo = rackInfo;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("ip", ip)
                .add("hostName", hostName)
                .add("dfsUsed", dfsUsed)
                .add("capacity", capacity)
                .add("remaining", remaining)
                .add("rackInfo", rackInfo)
                .toString();
    }
}
