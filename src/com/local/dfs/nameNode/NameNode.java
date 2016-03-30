package com.local.dfs.nameNode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;

import com.local.dfs.utilities.Pair;
import com.local.dfs.utilities.StringUtility;

public class NameNode extends UnicastRemoteObject implements INameNode {

	/**
	 * The serialization runtime associates with each serializable class a version number,
	 * called a serialVersionUID, which is used during deserialization to verify that
	 * the sender and receiver of a serialized object have loaded classes for that object 
	 * that are compatible with respect to serialization.
	 */
	private static final long serialVersionUID = 5848865380480200406L;

	// Variable names are self explanatory
	private int registryPort;
	private int blockSize;
	private int replicationFactor;
	private int heartBeatTimeOut;
	private int curBlockNumber;
	private String serviceName;						// name of the RMI object
	private Map<String, Pair<String, Integer>> activeDataNodes;

	/**
	 * Constructor
	 * @throws RemoteException
	 */
	public NameNode() throws RemoteException {
		super();
	}

	/**
	 * Driver function
	 * @param args
	 * @throws RemoteException
	 * @throws AlreadyBoundException 
	 */
	public static void main(String[] args) throws RemoteException, AlreadyBoundException {
		if (args.length < 1) {
			System.out.println("Too few arguments. Provide the configuration file!");
			System.exit(0);
		}

		NameNode server = new NameNode();
		server.loadNNConfig(args[0]).loadDNConfig(args[1]).launchServer();;
	}

	/**
	 * Load the name node configuration from the specified file
	 * @param configFile
	 * @return
	 * @throws RemoteException
	 */
	private NameNode loadNNConfig(String configFile) throws RemoteException {

		BufferedReader fr = null;
		String line = null;
		
		try {
			fr = new BufferedReader(new FileReader(configFile));
			
			// registry port
			line = fr.readLine();
			this.registryPort = Integer.parseInt(StringUtility.getSegment(0, line, ":"));
			
			// block size
			line = fr.readLine();
			this.blockSize = Integer.parseInt(StringUtility.getSegment(0, line, ":"));
			
			// replication factor
			line = fr.readLine();
			this.replicationFactor = Integer.parseInt(StringUtility.getSegment(0, line, ":"));

			// heartbeat timeout
			line = fr.readLine();
			this.heartBeatTimeOut = Integer.parseInt(StringUtility.getSegment(0, line, ":"));

			// current block number
			line = fr.readLine();
			this.curBlockNumber = Integer.parseInt(StringUtility.getSegment(0, line, ":"));

			// service name
			line = fr.readLine();
			this.serviceName = StringUtility.getSegment(0, line, ":");

		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return this;
	}

	private NameNode loadDNConfig(String configFile) {
		
		BufferedReader fr = null;
		String line = null;
		
		try {
			fr = new BufferedReader(new FileReader(configFile));
			while ((line = fr.readLine()) != null) {
				
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return this;
	}

	private void launchServer() throws RemoteException, AlreadyBoundException {
		Registry registry = LocateRegistry.createRegistry(this.registryPort);
		registry.bind(this.serviceName, this);
	}

	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

}
