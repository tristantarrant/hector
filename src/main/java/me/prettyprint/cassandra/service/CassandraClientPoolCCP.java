package me.prettyprint.cassandra.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.exceptions.HectorTransportException;
import net.dataforte.cassandra.pool.DataSource;
import net.dataforte.cassandra.pool.PoolConfiguration;
import net.dataforte.cassandra.pool.PoolProperties;

public class CassandraClientPoolCCP implements CassandraClientPool {

	DataSource dataSource;
	private CassandraClientMonitor clientMonitor;
	private ThriftCluster cluster;
	CassandraHost host;
	private Set<CassandraHost> knownHosts;

	public CassandraClientPoolCCP(CassandraClientMonitor clientMonitor) {
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("cassandrapool.properties");
		Properties p = new Properties();
		try {
			p.load(is);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		PoolConfiguration poolProperties = new PoolProperties();
		for (String propertyName : p.stringPropertyNames()) {
			poolProperties.set(propertyName, p.getProperty(propertyName));
		}
		poolProperties.setName("HectorCCP");
		host = new CassandraHost(poolProperties.getHost(), poolProperties.getPort());
		knownHosts = new HashSet<CassandraHost>();
		knownHosts.add(host);
		dataSource = new DataSource(poolProperties);

		this.clientMonitor = clientMonitor;
		this.cluster = new ThriftCluster("Default Cluster", this);
	}

	public CassandraClientPoolCCP(CassandraClientMonitor clientMonitor, CassandraHostConfigurator cassandraHostConfigurator) {
		this(clientMonitor, cassandraHostConfigurator.buildCassandraHosts());
	}

	public CassandraClientPoolCCP(CassandraClientMonitor clientMonitor, CassandraHost[] buildCassandraHosts) {
		this(clientMonitor);
	}

	@Override
	public CassandraClient borrowClient() throws HectorException {
		try {
			return new CassandraClientImpl(dataSource.getConnection(), new KeyspaceServiceFactory(clientMonitor), host, this, cluster,
					CassandraHost.DEFAULT_TIMESTAMP_RESOLUTION);
		} catch (Exception e) {
			throw new HectorException(e);
		}
	}

	@Override
	public CassandraClient borrowClient(String url, int port) throws HectorException {
		return borrowClient();
	}

	@Override
	public CassandraClient borrowClient(CassandraHost cassandraHost) throws HectorException {
		return borrowClient();
	}

	@Override
	public CassandraClient borrowClient(String urlPort) throws HectorException {
		return borrowClient();
	}

	@Override
	public CassandraClient borrowClient(String[] clientUrls) throws HectorException {
		return borrowClient();
	}

	@Override
	public void releaseClient(CassandraClient client) throws HectorException {
		dataSource.releaseConnection(client.getCassandra());
	}

	@Override
	public void releaseKeyspace(KeyspaceService k) throws HectorException {
		releaseClient(k.getClient());
	}

	@Override
	public void updateKnownHosts() throws HectorTransportException {
		// Do nothing
	}

	@Override
	public Set<String> getExhaustedPoolNames() {
		return Collections.emptySet();
	}

	@Override
	public Set<String> getPoolNames() {
		return Collections.emptySet();
	}

	@Override
	public int getNumPools() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumIdle() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumExhaustedPools() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumBlockedThreads() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumActive() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Set<CassandraHost> getKnownHosts() {
		return knownHosts;
	}

	@Override
	public void addCassandraHost(CassandraHost cassandraHost) {
		// TODO Auto-generated method stub

	}

	@Override
	public void invalidateClient(CassandraClient client) {
		// TODO Auto-generated method stub

	}

	@Override
	public void invalidateAllConnectionsToHost(CassandraClient client) {
		// TODO Auto-generated method stub

	}

	@Override
	public CassandraClientMonitorMBean getMbean() {
		return clientMonitor;
	}

	@Override
	public Cluster getCluster() {
		return cluster;
	}

}
