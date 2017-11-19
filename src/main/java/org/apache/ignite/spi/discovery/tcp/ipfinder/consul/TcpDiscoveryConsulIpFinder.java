package org.apache.ignite.spi.discovery.tcp.ipfinder.consul;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

import com.orbitz.consul.Consul;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.ServiceHealth;

/**
 * 
 * @author Andrea Zanetti
 * 
 * This is mainly a refactoring of {@link TcpDiscoveryZookeeperIpFinder} that uses Ashicorp Consul 
 * instead of ZooKeeper + Curator as service registry.
 * 
 * It is intended as a TCP cluster node IP discovery system
 * 
 * The main difference is that Consul does not allow for duplicated registration and hasn't the same notion of ephemeral 
 * node as ZooKeeper, so we have to take that into account and manually deregister nodes when closing
 * 
 *
 */
public class TcpDiscoveryConsulIpFinder extends TcpDiscoveryIpFinderAdapter {
	
	/** Default service name for service registrations. */
    public static final String SERVICE_NAME = "ignite";
    
    /** Key for retrieving Consul address from system properties */
    public static final String PROP_CONSUL_ADDRESS_KEY = "IGNITE_CONSUL_ADDRESS";
    
    /** Key for retrieving Consul port from system properties */
    public static final String PROP_CONSUL_PORT_KEY = "IGNITE_CONSUL_PORT";
    
    public static final int DEFAULT_CONSUL_PORT = 8500;
	
    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Close guard. */
    @GridToStringExclude
    private final AtomicBoolean closeGuard = new AtomicBoolean();
    
    /** Logger. */
    @LoggerResource
    private IgniteLogger log;
    
    private String serviceName = SERVICE_NAME;
    
    /** Consul client*/
	private Consul consul;
	
	/** Consul server address*/
	private String consulAddres;
	
	/** Consul server port*/
	private int consulPort = DEFAULT_CONSUL_PORT;
	
	/** All addresses registered by this node*/
	private HashSet<InetSocketAddress> ourInstances = new HashSet<>();
	
	/** Registered addresses for this node*/
	private HashSet<InetSocketAddress> myAddresses = new HashSet<>();
	
	/** Constructor. */
	public TcpDiscoveryConsulIpFinder() {
		setShared(true);
	}
	
	/** Initializes this IP Finder by creating the appropriate Curator objects. */
    private void init() {
        if (!initGuard.compareAndSet(false, true))
            return;
    
        String propsAddress = StringUtils.trim(System.getProperty(PROP_CONSUL_ADDRESS_KEY));
        String propsPort = StringUtils.trim(System.getProperty(PROP_CONSUL_PORT_KEY));
        
        if (log.isInfoEnabled())
            log.info("Initializing Consul IP Finder.");
        
        if(StringUtils.isNotBlank(propsAddress))
        	consulAddres = propsAddress;
        
        if(StringUtils.isNotBlank(propsPort))
        	consulPort = Integer.parseInt(propsPort);
        
        if (consul == null) {
            A.notNullOrEmpty(consulAddres, String.format("Consul URL (or system property %s) cannot be null " +
                "or empty if a Consul client object is not provided explicitly", PROP_CONSUL_ADDRESS_KEY));
            consul = Consul.builder().withUrl(consulAddres + ":" + consulPort).build();
        }
    }
	
	/** {@inheritDoc} */
    @Override
    public void onSpiContextDestroyed() {
    	 if (!closeGuard.compareAndSet(false, true)) {
             U.warn(log, "Consul IP Finder can't be closed more than once.");

             return;
         }

         if (log.isInfoEnabled())
             log.info("Destroying Consul IP Finder.");

         super.onSpiContextDestroyed();
         unregisterSelf();
         if(consul != null)
        	 consul.destroy();
    }
	
	/** {@inheritDoc} */
	@Override
	public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
		init();
		
		if (log.isDebugEnabled())
            log.debug("Getting registered addresses from Consul IP Finder.");
		
		List<ServiceHealth> nodes = consul.healthClient()
               							  .getHealthyServiceInstances(serviceName)
               							  .getResponse();
		
		Collection<InetSocketAddress> registeredAddresses = new HashSet<>();
		
		for(ServiceHealth node : nodes) {
			registeredAddresses.add(new InetSocketAddress(node.getService().getAddress(), node.getService().getPort()));
		}
		
        if (log.isInfoEnabled())
            log.info("Cosnul IP Finder resolved addresses: " + registeredAddresses);

		return registeredAddresses;
	}
	
	/** {@inheritDoc} */
	@Override
	public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
		init();
		
		if (log.isInfoEnabled())
            log.info("Registering addresses with Cosnul IP Finder: " + addrs + " Addresses that are already registered will be ignored");
		
		Collection<InetSocketAddress> alreadyRegistered = getRegisteredAddresses();
		
		addrs.removeAll(alreadyRegistered);
		
		for(InetSocketAddress addr : addrs) {
			String serviceUid = inetAddrToUid(addr);
			
			Registration serviceRegistration = ImmutableRegistration.builder()
																	.name(serviceName)
																	.address(addr.getAddress().getHostAddress())
																	.port(addr.getPort())
																	.id(serviceUid)
																	.build();
			consul.agentClient().register(serviceRegistration);
			ourInstances.add(addr);
			
			log.info("registered service " + serviceUid + ", registration status now is {}"
					+ consul.agentClient().isRegistered(serviceUid));
		}
		
	}

	/** {@inheritDoc} */
	@Override
	public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
		init();
		
		if (log.isInfoEnabled())
            log.info("Unregistering addresses with Consul IP Finder: " + addrs);
		
		for(InetSocketAddress addr : addrs) {
			String serviceUid = inetAddrToUid(addr);
			
			consul.agentClient().deregister(serviceUid);
		}
	}
	
    /** {@inheritDoc} */
    @Override 
    public void initializeLocalAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
    	registerSelf(addrs);
    }
    
    private String inetAddrToUid(InetSocketAddress addr) {
    	return addr.getAddress().getHostName() + ":" + addr.getAddress().getHostAddress() + ":" + addr.getPort();
    }
    
    private void registerSelf(Collection<InetSocketAddress> addrs) {
    	registerAddresses(addrs);
    	myAddresses.addAll(addrs);
    }
    
    private void unregisterSelf() {
    	unregisterAddresses(myAddresses);
    }

	public Consul getConsul() {
		return consul;
	}

	public void setConsul(Consul consul) {
		this.consul = consul;
	}

	public String getConsulAddres() {
		return consulAddres;
	}

	public void setConsulAddres(String consulAddres) {
		this.consulAddres = consulAddres;
	}

	public int getConsulPort() {
		return consulPort;
	}

	public void setConsulPort(int consulPort) {
		this.consulPort = consulPort;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
}
