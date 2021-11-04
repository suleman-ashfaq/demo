package com.example.demo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CuratorLeader implements Closeable, LeaderSelectorListener{
    private static final Logger LOG = LoggerFactory.getLogger(CuratorLeader.class);

    private String myId;
    private CuratorFramework client;
    private String serverPort;
    private final LeaderSelector leaderSelector;

    /*
     * We use one latch as barrier for the master selection
     * and another one to block the execution of master
     * operations when the ZooKeeper session transitions
     * to suspended.
     */
    private CountDownLatch closeLatch = new CountDownLatch(1);

    /**
     * Creates a new Curator client, setting the the retry policy
     * to ExponentialBackoffRetry.
     *
     * @param myId
     *          master identifier
     * @param hostPort
     *          list of zookeeper servers comma-separated
     * @param retryPolicy
     *          Curator retry policy
     */
    public CuratorLeader(String myId, String hostPort, RetryPolicy retryPolicy){
        LOG.info( myId + ": " + hostPort );

        this.myId = myId;
        this.client = CuratorFrameworkFactory.newClient("127.0.0.1:2181",
                retryPolicy);
        this.leaderSelector = new LeaderSelector(this.client, "/leader-election", this);
    }

    public void startZK(){
        client.start();
    }

    public void runForMaster() {
        leaderSelector.setId(myId);
        LOG.info( "Starting master selection: " + myId);
//        leaderSelector.autoRequeue();
        leaderSelector.start();
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    public void register() {
        try {
            Stat statPort = client.checkExists().forPath("/leader-port");
            if (statPort == null)
                client.create().forPath("/leader-port");

            Stat statWorker = client.checkExists().forPath("/workers");
            if (statWorker == null)
                client.create().forPath("/workers");

            //We want the ports to go away when connection is broken hence creating EPHEMERAL nodes
            client.create().withMode(CreateMode.EPHEMERAL).forPath("/workers/"+this.serverPort);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Could not register {}", this.serverPort);
        }
    }

    public List<String> getAllPorts() {
        try {
            List<String> ports = client.getChildren().forPath("/workers");
            return ports;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    public String getLeaderPort() throws Exception {
        byte[] arr = client.getData().forPath("/leader-port");
        String s = new String(arr, StandardCharsets.UTF_8);
        return s;

    }
    public boolean isLeader() {
        return leaderSelector.hasLeadership();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception
    {
        LOG.info( "Mastership participants: " + myId + ", " + leaderSelector.getParticipants() );

        try {
            System.out.println("Trying to access server port");
            System.out.println("Server PORT ++++++++++++++++++ " + serverPort);
            client.setData().forPath("/leader-port", serverPort.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Unable to register leaderPort {}", serverPort);
        }

        /*
         * This latch is to prevent this call from exiting. If we exit, then
         * we release mastership.
         */
        closeLatch.await();
        leaderSelector.autoRequeue();
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        LOG.info("State Changed: {}", newState);
    }

    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }

    @Override
    public void close()
            throws IOException
    {
        LOG.info( "Closing" );
        closeLatch.countDown();
        leaderSelector.close();
        client.close();
    }
}
