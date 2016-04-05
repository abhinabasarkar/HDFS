package com.local.dfs.nameNode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import com.google.protobuf.InvalidProtocolBufferException;
import com.local.dfs.proto.Hdfs.AssignBlockRequest;
import com.local.dfs.proto.Hdfs.AssignBlockResponse;
import com.local.dfs.proto.Hdfs.BlockLocationRequest;
import com.local.dfs.proto.Hdfs.BlockLocationResponse;
import com.local.dfs.proto.Hdfs.BlockLocations;
import com.local.dfs.proto.Hdfs.BlockReportRequest;
import com.local.dfs.proto.Hdfs.BlockReportResponse;
import com.local.dfs.proto.Hdfs.CloseFileRequest;
import com.local.dfs.proto.Hdfs.CloseFileResponse;
import com.local.dfs.proto.Hdfs.DataNodeLocation;
import com.local.dfs.proto.Hdfs.HeartBeatRequest;
import com.local.dfs.proto.Hdfs.HeartBeatResponse;
import com.local.dfs.proto.Hdfs.ListFilesResponse;
import com.local.dfs.proto.Hdfs.OpenFileRequest;
import com.local.dfs.proto.Hdfs.OpenFileResponse;
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

	// Tells invoked RMI method ran successfully
	private static final int STATUS_OKAY = 0;

	// Tells invoked RMI method ran with errors
	private static final int STATUS_NOT_OKAY = 1;

	private static final int ERROR_FILE_HANDLE = -999;

	// Variable names are self explanatory
	private int registryPort;
	private int blockSize;
	private int replicationFactor;
	private int heartBeatTimeOut;
	private int curBlockNumber;
	private String serviceName;											// name of the RMI object
	private Map<Integer, Pair<String, Integer>> allDataNodes;			// Map DN id to the pair of ip and port
	private Map<String, Integer> dataNodeIDs;							// Map DN ip to its id
	private Map<Integer, Long> activeDataNodes;							// Map DN id to the last received heartbeat timestamp
	private Map<String, Vector<Integer>> allFiles;						// Map of file name to list of blocks
	private Map<String, Integer> openFiles;								// Map file name to handle for open files
	private Map<Integer, String> openFilesRev;							// Map handle to file name for open files
	private Map<Integer, ArrayList<DataNodeLocation>> blockLocations;	// Map block number to list of data nodes
	private Map<Integer, Integer> blocksFull;							// Map DN id to the number of block full
	private Map<Integer, ArrayList<Integer>> blockList;					// Map DN id to the list of blocks
	private int writeHandle;

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
	public static void main(String[] args) throws RemoteException, AlreadyBoundException 
	{
		if (args.length < 2) 
		{
			System.out.println("Too few arguments. Provide the configuration files!");
			System.exit(0);
		}

		NameNode server = new NameNode();
		server.loadNNConfig(args[0]).loadDNConfig(args[1]).launchServer();
		
		server.purgeDeadDataNodes();
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
			this.registryPort = Integer.parseInt(StringUtility.getSegment(1, line, ":"));
			
			// block size
			line = fr.readLine();
			this.blockSize = Integer.parseInt(StringUtility.getSegment(1, line, ":"));
			
			// replication factor
			line = fr.readLine();
			this.replicationFactor = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

			// heartbeat timeout
			line = fr.readLine();
			this.heartBeatTimeOut = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

			// current block number
			line = fr.readLine();
			this.curBlockNumber = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

			// service name
			line = fr.readLine();
			this.serviceName = StringUtility.getSegment(1, line, ":");
			
			// write handle initialization
			this.writeHandle = 0;
			
			// Other data structures initialization
			this.allDataNodes = new HashMap<Integer, Pair<String, Integer>>();
			this.activeDataNodes = new HashMap<Integer, Long>();
			this.allFiles = new HashMap<String, Vector<Integer>>();
			this.openFiles = new HashMap<String, Integer>();
			this.openFilesRev = new HashMap<Integer, String>();
			this.blockLocations = new HashMap<Integer, ArrayList<DataNodeLocation>>();
			this.blocksFull = new HashMap<Integer, Integer>();
			this.blockList = new HashMap<Integer, ArrayList<Integer>>();
			
			fr.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}

		return this;
	}

	
	/**
	 * Load data node configuration and populate the data structure of active data nodes
	 * @param configFile
	 * @return
	 */
	private NameNode loadDNConfig(String configFile) {
		
		BufferedReader fr = null;
		String line = null;
		
		try {
			fr = new BufferedReader(new FileReader(configFile));
			while ((line = fr.readLine()) != null) 
			{
				String ip = StringUtility.getSegment(1, line, ":");
				int id = Integer.parseInt(StringUtility.getSegment(0, line, ":"));
				int port = Integer.parseInt(StringUtility.getSegment(2, line, ":"));

				this.allDataNodes.put(id, new Pair<String, Integer>(ip, port));
				this.dataNodeIDs.put(ip, id);
			}
			fr.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return this;
	}

	/**
	 * Create a new registry at the given port and bind the name node RMI object to the given name
	 * @throws RemoteException
	 * @throws AlreadyBoundException
	 */
	private void launchServer() throws RemoteException, AlreadyBoundException {
		Registry registry = LocateRegistry.createRegistry(this.registryPort);
		registry.bind(this.serviceName, this);
		System.out.println("Name node bound to the object named - " + this.serviceName);
	}

	/**
	 * Making use of the recorded heartbeats, remove the dead data nodes from the active list 
	 */
	private void purgeDeadDataNodes() {
		try {
			while (true) {
				synchronized(this.activeDataNodes) {
					for (Integer id: this.activeDataNodes.keySet()) {
						// heartbeat for this data node timed out. so purge it.
						if (System.currentTimeMillis() - activeDataNodes.get(id) >= this.heartBeatTimeOut) {
							this.activeDataNodes.remove(id);
							System.out.println("Heartbeat for data node '" + id + "' timed out. so purge it.");
						}
					}
				}
					Thread.sleep(this.heartBeatTimeOut);
			}
		}
		catch (InterruptedException e) {
			System.out.println("Error purging dead data nodes!");
			e.printStackTrace();
		}
	}

	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
		OpenFileResponse.Builder ofResponse = OpenFileResponse.newBuilder();
		
		try {
			OpenFileRequest ofRequest = OpenFileRequest.parseFrom(inp);
			String file = ofRequest.getFileName();
			boolean rw = ofRequest.getForRead();
			
			// file already opened for writing
			if (openFiles.containsKey(file)) {
				ofResponse.setStatus(NameNode.STATUS_NOT_OKAY);
				ofResponse.setHandle(NameNode.ERROR_FILE_HANDLE);
				System.out.println("File already open in write mode!");
			}
			// file not opened for writing, read request and file exists - valid read request
			else if (rw && allFiles.containsKey(file)) {
				ofResponse.setStatus(NameNode.STATUS_OKAY);
				ofResponse.setHandle(0);
				
				for (Integer b: allFiles.get(file)) {
					ofResponse.addBlockNums(b);
				}
				System.out.println("File opened for read!");
			}
			// file not opened for writing, read request but does not exist
			else if (rw) {
				ofResponse.setStatus(NameNode.STATUS_NOT_OKAY);
				ofResponse.setHandle(NameNode.ERROR_FILE_HANDLE);
				System.out.println("File does not exist in the file system!");
			}
			// file not opened for writing, write request and does not exist - valid write request
			else if (!allFiles.containsKey(file)) {
				openFiles.put(file, ++this.writeHandle);
				openFilesRev.put(this.writeHandle, file);
				ofResponse.setStatus(NameNode.STATUS_OKAY);
				ofResponse.setHandle(this.writeHandle);
				System.out.println(file + " opened for writing");
			}
		}
		catch (InvalidProtocolBufferException e) {
			ofResponse.setStatus(NameNode.STATUS_NOT_OKAY);
			ofResponse.setHandle(NameNode.ERROR_FILE_HANDLE);
			e.printStackTrace();
		}
		return ofResponse.build().toByteArray();
	}

	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		CloseFileResponse.Builder cfRes = CloseFileResponse.newBuilder();

		try {
			CloseFileRequest cfReq = CloseFileRequest.parseFrom(inp);
			int fh = cfReq.getHandle();
			
			if (openFilesRev.containsKey(fh)) {
				String file = openFilesRev.get(fh);

				openFiles.remove(file);
				openFilesRev.remove(fh);
				
				cfRes.setStatus(NameNode.STATUS_OKAY);
				System.out.println(file + " closed successfully!");
			}
			else {
				System.out.println("Attempting to close a file that is not opened!");
				cfRes.setStatus(NameNode.STATUS_NOT_OKAY);
			}
		}
		catch (InvalidProtocolBufferException e1) {
			cfRes.setStatus(NameNode.STATUS_NOT_OKAY);
			e1.printStackTrace();
		}
		return cfRes.build().toByteArray();
	}

	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		BlockLocationResponse.Builder res = BlockLocationResponse.newBuilder();

		int status = NameNode.STATUS_NOT_OKAY;

		try {
			BlockLocationRequest req = BlockLocationRequest.parseFrom(inp);
			for (int i = 0; i < req.getBlockNumsCount(); i++) {
				int bno = req.getBlockNums(i);

				BlockLocations.Builder bLoc = BlockLocations.newBuilder();
				bLoc.setBlockNumber(bno);

				DataNodeLocation dn1 = this.blockLocations.get(bno).get(0);
				DataNodeLocation dn2 = this.blockLocations.get(bno).get(0);
				
				String ip = dn1.getIp();
				int id = this.dataNodeIDs.get(ip);
				if (this.activeDataNodes.containsKey(id)) {
					bLoc.addLocations(dn1);
					status = NameNode.STATUS_OKAY;
				}
				
				ip = dn2.getIp();
				id = this.dataNodeIDs.get(ip);
				if (this.activeDataNodes.containsKey(id)) {
					bLoc.addLocations(dn2);
					status = NameNode.STATUS_OKAY;
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		res.setStatus(status);
		return res.build().toByteArray();
	}

	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		AssignBlockResponse.Builder res = AssignBlockResponse.newBuilder();
		try {
			AssignBlockRequest req = AssignBlockRequest.parseFrom(inp);
			int fh = req.getHandle();
			String file = this.openFilesRev.get(fh);
			
			int bno = getNextBlockNumber();

			// Update the file name to block list data structure in an mutually exclusive manner
			synchronized(this.allFiles) {
				if (this.allFiles.containsKey(file)) {
					this.allFiles.get(file).add(bno);
				}
				else {
					Vector<Integer> vec = new Vector<Integer>();
					vec.add(bno);
					this.allFiles.put(file, vec);
				}
			}
			
			// To load balance select the data nodes that have received less number of blocks.
			// To do that first sort the data nodes based on the number of blocks that are full
			Set<Entry<Integer, Integer>> entries = this.blocksFull.entrySet();
			List<Entry<Integer, Integer>> listToSort = new ArrayList<Entry<Integer, Integer>>(entries);
			Collections.sort(listToSort, new Comparator<Map.Entry<Integer, Integer>>() {
				public int compare(Map.Entry<Integer, Integer> a, Map.Entry<Integer, Integer> b) {
					return a.getValue().compareTo(b.getValue());
				}
			});
			Vector<Integer> winnerIDs = new Vector<Integer>();
			int selected = 0;
			int n = listToSort.size();
			int i = 1;
			while (selected != 2) {
				int id = listToSort.get(n - i).getKey();
				if (this.activeDataNodes.containsKey(id)) {
					winnerIDs.add(id);
					selected++;
				}
				i++;
			}
			
			// update the blocks used on both the data nodes - this block should be mutually exclusive
			int id1 = winnerIDs.get(0), id2 = winnerIDs.get(1);
			synchronized(this.blocksFull) {
				this.blocksFull.put(id1, this.blocksFull.get(id1) + 1);
				this.blocksFull.put(id2, this.blocksFull.get(id2) + 1);
			}
			
			// prepare the response
			DataNodeLocation.Builder dn1 = DataNodeLocation.newBuilder();
			dn1.setIp(this.allDataNodes.get(id1).getLeft());
			dn1.setPort(this.allDataNodes.get(id1).getRight());

			DataNodeLocation.Builder dn2 = DataNodeLocation.newBuilder();
			dn2.setIp(this.allDataNodes.get(id2).getLeft());
			dn2.setPort(this.allDataNodes.get(id2).getRight());
			
			BlockLocations.Builder bLoc = BlockLocations.newBuilder();
			bLoc.setBlockNumber(bno);
			bLoc.addLocations(dn1);
			bLoc.addLocations(dn2);
			
			res.setStatus(NameNode.STATUS_OKAY);
			res.setNewBlock(bLoc);
			
		}
		catch (Exception e) {
			e.printStackTrace();
			res.setStatus(NameNode.STATUS_NOT_OKAY);
		}
		return res.build().toByteArray();
	}

	/**
	 * Returns the list of all files in the file system.
	 * Right now we are treating the directory name to be irrelevant.
	 */
	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		ListFilesResponse.Builder lsResponse = ListFilesResponse.newBuilder();

		System.out.println("Listing files");
		for (String file: allFiles.keySet()) {
			System.out.println(file);
			lsResponse.addFileNames(file);
		}
		
		lsResponse.setStatus(NameNode.STATUS_NOT_OKAY);

		return lsResponse.build().toByteArray();
	}

	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		BlockReportResponse.Builder res = BlockReportResponse.newBuilder();

		try {
			BlockReportRequest req = BlockReportRequest.parseFrom(inp);
			int dnID = req.getId();

			synchronized(this.blockLocations) {
				for (int i = 0; i < req.getBlockNumbersCount(); i++) {
					int bno = req.getBlockNumbers(i);
					if (this.blockLocations.containsKey(bno)) {
						DataNodeLocation.Builder dn = DataNodeLocation.newBuilder();
						dn.setIp(req.getLocation().getIp());
						dn.setPort(req.getLocation().getPort());
						this.blockLocations.get(bno).add(dn.build());
					}
					else {
						ArrayList<DataNodeLocation> list = new ArrayList<DataNodeLocation>();
						DataNodeLocation.Builder dn = DataNodeLocation.newBuilder();
						dn.setIp(req.getLocation().getIp());
						dn.setPort(req.getLocation().getPort());
						list.add(dn.build());
						
						this.blockLocations.put(bno, list);
					}
					res.addStatus(NameNode.STATUS_OKAY);
				}
			}
			
			synchronized(this.blockList) {
				if (this.blockList.containsKey(dnID)) {
					this.blockList.remove(dnID);
				}
				ArrayList<Integer> list = new ArrayList<Integer>();
				for (int i = 0; i < req.getBlockNumbersCount(); i++) {
					list.add(req.getBlockNumbers(i));
				}
				this.blockList.put(dnID, list);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return res.build().toByteArray();
	}

	/**
	 * Remote method to handle the incoming heartbeat messages.
	 * Record the timestamp for the sender data node which is used by
	 * the purgeDeadDataNodes method to update the list of active data nodes
	 */
	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		HeartBeatResponse.Builder hbResponse = HeartBeatResponse.newBuilder();
		HeartBeatRequest hbMessage;

		try {
			hbMessage = HeartBeatRequest.parseFrom(inp);
			synchronized(this.activeDataNodes) {
				this.activeDataNodes.put(hbMessage.getId(), System.currentTimeMillis());
			}
			hbResponse.setStatus(NameNode.STATUS_OKAY);
		}
		catch (InvalidProtocolBufferException e) {
			hbResponse.setStatus(NameNode.STATUS_NOT_OKAY);
			e.printStackTrace();
		}
		return hbResponse.build().toByteArray();
	}

	/**
	 * Make the block number update operation mutually exclusive
	 * @return
	 */
	synchronized int getNextBlockNumber() {
		return ++this.curBlockNumber;
	}
}
