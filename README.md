ignite-consul
===================

**A simple Consul based cluster discovery for Apache Ingite**

####Motivation:

Hashicorp Consul is becoming an increaingly used KV store and service discovery, being comparatevely simpler than Apache ZooKeeper. Unfortunately Consul support is missing from the standar Ignite distribution as a mean of cluster discovery. 

####How does it work:

This a very simple cluster discovery method that uses [OrbitzWorldwide consul-client](https://github.com/OrbitzWorldwide/consul-client) as a mean to register node on a Consul cluster and thus provide discovery

####How to use it:

Right now the simpler way to incorporate this code into your Apache Ignite artifact is to clone Ignite project, clone this repository inside the 
_modules_ subdirectory and modify Ignite _pom.xml_ adding this as a module. 

I'll provide some easier way of deployment ASAP.

A sample Ignite configuration for the discovery module is provided in [this project](https://github.com/andrea-zanetti/ignite-config) but basically all you need is to add (port is optional)

        <property name="discoverySpi">
             <bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">
				<property name="ipFinder">
					<bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.consul.TcpDiscoveryConsulIpFinder">
						<property name="consulAddres" value="http://consul"/>
						<property name="consulPort" value="8500"/>
					</bean>
				</property>
			</bean>
        </property>

