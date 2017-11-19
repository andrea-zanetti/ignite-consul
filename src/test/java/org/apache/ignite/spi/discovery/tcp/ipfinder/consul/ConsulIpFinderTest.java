package org.apache.ignite.spi.discovery.tcp.ipfinder.consul;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.ServiceHealth;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;

public class ConsulIpFinderTest extends GridCommonAbstractTest {

	 private ConsulProcess consul;


	/**
     * Before test.
     *
     * @throws Exception
     */
    @Override 
    public void beforeTest() throws Exception {
        super.beforeTest();

        // remove stale system properties
        System.getProperties().remove(TcpDiscoveryConsulIpFinder.PROP_CONSUL_ADDRESS_KEY);
        System.getProperties().remove(TcpDiscoveryConsulIpFinder.PROP_CONSUL_PORT_KEY);
        
        consul = ConsulStarterBuilder.consulStarter().build().start();
    }
    
    
    private String getConsulAddress() {
    	return "http://" + consul.getAddress();
    }
    
    
    /**
     * Enhances the default configuration with the {#TcpDiscoveryConsulIpFinder}.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Ignite configuration.
     * @throws Exception If failed.
     */
    @Override 
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi tcpDisco = (TcpDiscoverySpi)configuration.getDiscoverySpi();
        TcpDiscoveryConsulIpFinder consulIpFinder = new TcpDiscoveryConsulIpFinder();

        // first node => configure with address and port; second node => configure with Consul client; third and subsequent
        // shall be configured through system property
        if (igniteInstanceName.equals(getTestIgniteInstanceName(0))) {
        	consulIpFinder.setConsulAddres(getConsulAddress());
        	consulIpFinder.setConsulPort(consul.getHttpPort());
        }else if (igniteInstanceName.equals(getTestIgniteInstanceName(1))) {
        	consulIpFinder.setConsul(Consul.builder().withUrl(getConsulAddress() + ":" + consul.getHttpPort()).build());
        }

        tcpDisco.setIpFinder(consulIpFinder);

        return configuration;
    }
    
    
    /**
     * After test.
     *
     * @throws Exception
     */
    @Override 
    public void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
        consul.close();
    }
    

	/**
	 * @throws Exception If failed.
	 */
	public void testOneIgniteNodeIsAlone() throws Exception {
        startGrid(0);

        assertEquals(1, grid(0).cluster().metrics().getTotalNodes());

        stopAllGrids();
	}
	
    /**
     * @throws Exception If failed.
     */
    public void testTwoIgniteNodesFindEachOther() throws Exception {
        // start one node
        startGrid(0);

        // set up an event listener to expect one NODE_JOINED event
        CountDownLatch latch = expectJoinEvents(grid(0), 1);

        // start the other node
        startGrid(1);

        // assert the nodes see each other
        assertEquals(2, grid(0).cluster().metrics().getTotalNodes());
        assertEquals(2, grid(1).cluster().metrics().getTotalNodes());

        // assert the event listener got as many events as expected
        latch.await(1, TimeUnit.SECONDS);

        stopAllGrids();
    }
    
    /**
     * @throws Exception If failed.
     */
    public void testThreeNodesWithThreeDifferentConfigMethods() throws Exception {
        // start one node
        startGrid(0);

        // set up an event listener to expect one NODE_JOINED event
        CountDownLatch latch = expectJoinEvents(grid(0), 2);

        // start the 2nd node
        startGrid(1);

        // start the 3rd node, first setting the system property
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_ADDRESS_KEY, getConsulAddress());
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_PORT_KEY, consul.getHttpPort() + "");
        
        startGrid(2);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 2);

        // assert the nodes see each other
        assertEquals(3, grid(0).cluster().metrics().getTotalNodes());
        assertEquals(3, grid(1).cluster().metrics().getTotalNodes());
        assertEquals(3, grid(2).cluster().metrics().getTotalNodes());

        // assert the event listener got as many events as expected
        latch.await(1, TimeUnit.SECONDS);

        stopAllGrids();
    }
    
    
    /**
     * @throws Exception If failed.
     */
    public void testFourNodesStartingAndStopping() throws Exception {
        // start one node
        startGrid(0);

        // set up an event listener to expect one NODE_JOINED event
        CountDownLatch latch = expectJoinEvents(grid(0), 3);

        // start the 2nd node
        startGrid(1);

        // start the 3rd & 4th nodes, first setting the system property
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_ADDRESS_KEY, getConsulAddress());
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_PORT_KEY, consul.getHttpPort() + "");
        startGrid(2);
        startGrid(3);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // assert the nodes see each other
        assertEquals(4, grid(0).cluster().metrics().getTotalNodes());
        assertEquals(4, grid(1).cluster().metrics().getTotalNodes());
        assertEquals(4, grid(2).cluster().metrics().getTotalNodes());
        assertEquals(4, grid(3).cluster().metrics().getTotalNodes());

        // assert the event listener got as many events as expected
        latch.await(1, TimeUnit.SECONDS);

        // stop the first grid
        stopGrid(0);

        // make sure that nodes were synchronized; they should only see 3 now
        assertEquals(3, grid(1).cluster().metrics().getTotalNodes());
        assertEquals(3, grid(2).cluster().metrics().getTotalNodes());
        assertEquals(3, grid(3).cluster().metrics().getTotalNodes());

        // stop all remaining grids
        stopGrid(1);
        stopGrid(2);
        stopGrid(3);

        
        Consul consulClient = Consul.builder().withUrl(getConsulAddress() + ":" + consul.getHttpPort()).build();
        
        
        // check that the nodes are gone in Consul
        assertEquals(0, consulClient
        				.healthClient()
        				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
        				.getResponse().size());
    }
    
    /**
     * @throws Exception If failed.
     */
    public void testFourNodesWithNoDuplicateRegistrations() throws Exception {
    	
    	Consul consulClient = Consul.builder().withUrl(getConsulAddress() + ":" + consul.getHttpPort()).build();
    	
        // start 4 nodes
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_ADDRESS_KEY, getConsulAddress());
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_PORT_KEY, consul.getHttpPort() + "");
        startGrids(4);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // each node will only register itself
        assertEquals(4, consulClient
				.healthClient()
				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
				.getResponse().size());

        // stop all grids
        stopAllGrids();

        // check that all nodes are gone in Consul
        assertEquals(0, consulClient
				.healthClient()
				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
				.getResponse().size());
    }
    
    /**
     * @throws Exception If failed.
     */
    public void testFourNodesRestartLastSeveralTimes() throws Exception {
    	
    	Consul consulClient = Consul.builder().withUrl(getConsulAddress() + ":" + consul.getHttpPort()).build();

        // start 4 nodes
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_ADDRESS_KEY, getConsulAddress());
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_PORT_KEY, consul.getHttpPort() + "");
        startGrids(4);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // each node will only register itself
        assertEquals(4, consulClient
				.healthClient()
				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
				.getResponse().size());

        // repeat 5 times
        for (int i = 0; i < 5; i++) {
            // stop last grid
            stopGrid(2);

            // check that the node has unregistered itself and its party
            assertEquals(3, consulClient
    				.healthClient()
    				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
    				.getResponse().size());

            // start the node again
            startGrid(2);

            // check that the node back in Consul
            assertEquals(4, consulClient
    				.healthClient()
    				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
    				.getResponse().size());
        }

        stopAllGrids();

        assertEquals(0, consulClient
				.healthClient()
				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
				.getResponse().size());
    }
    
    
    /**
     * @throws Exception If failed.
     */
    public void testFourNodesKillRestartZookeeper() throws Exception {
    	
    	Consul consulClient = Consul.builder().withUrl(getConsulAddress() + ":" + consul.getHttpPort()).build();

        // start 4 nodes
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_ADDRESS_KEY, getConsulAddress());
        int port = consul.getHttpPort();
        System.setProperty(TcpDiscoveryConsulIpFinder.PROP_CONSUL_PORT_KEY, port + "");
        startGrids(4);

        // wait until all grids are started
        waitForRemoteNodes(grid(0), 3);

        // each node will only register itself
        assertEquals(4, consulClient
				.healthClient()
				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
				.getResponse().size());

        // remember Consul server configuration and stop the server
       List<ServiceHealth> state =  consulClient
    		   							.healthClient()
    		   							.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
    		   							.getResponse();
       
       consul.close();

        // start the cluster with the previous configuration
        consul = ConsulStarterBuilder.consulStarter().withHttpPort(port).build().start();
        
        for(ServiceHealth sh : state) {
        	Registration serviceRegistration = ImmutableRegistration.builder()
					.name(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
					.address(sh.getService().getAddress())
					.port(sh.getService().getPort())
					.id(sh.getService().getId())
					.build();
        	consulClient.agentClient().register(serviceRegistration);
        }
        

        // check that the nodes have registered again
        assertEquals(4, consulClient
				.healthClient()
				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
				.getResponse().size());

        // stop all grids
        stopAllGrids();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return 0 == consulClient
            				.healthClient()
            				.getHealthyServiceInstances(TcpDiscoveryConsulIpFinder.SERVICE_NAME)
            				.getResponse().size();
                }
                catch (Exception e) {
                    U.error(log, "Failed to wait for zk condition", e);

                    return false;
                }
            }
        }, 20000));
    }

    
    /**
     * @param ignite Node.
     * @param joinEvtCnt Expected events number.
     * @return Events latch.
     */
    @SuppressWarnings("serial")
	private CountDownLatch expectJoinEvents(Ignite ignite, int joinEvtCnt) {
        final CountDownLatch latch = new CountDownLatch(joinEvtCnt);

        ignite.events().remoteListen(new IgniteBiPredicate<UUID, Event>() {
            @Override public boolean apply(UUID uuid, Event evt) {
                latch.countDown();
                return true;
            }
        }, null, EventType.EVT_NODE_JOINED);

        return latch;
    }

}
