package com.local.dfs.dataNode;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.local.dfs.nameNode.INameNode;
import com.local.dfs.proto.Hdfs.BlockLocations;
import com.local.dfs.proto.Hdfs.DataNodeLocation;
import com.local.dfs.proto.Hdfs.ReadBlockRequest;
import com.local.dfs.proto.Hdfs.ReadBlockResponse;
import com.local.dfs.proto.Hdfs.WriteBlockRequest;
import com.local.dfs.proto.Hdfs.WriteBlockResponse;
import com.local.dfs.utilities.StringUtility;

/**
 * @author abhi
 *
 */
public class DataNode implements IDataNode 
{
	// all the book keeping variables 
	private String dataNodeID;								// ID of the datanode (not much useful) 
	private String dataNodeIP, nameNodeIP;					// IP address of self and the NN
	private int blockSize;									// in MB
	private int dataNodePort, nameNodePort;					// port numbers for connection
	private int blockReportInterval, heartbeatInterval;		// in seconds
	private Registry reg;									// the rmi registry
	private INameNode stub;									// the stub
	private String blockDir;								// directory where the data would be stored

	// static status messages
	static final int SUCCESS = 0;
	static final int FAIL = 1;


	/**
	 * constructor 
	 */
	public DataNode() {
		super();
	}

	
	/**
	 * loads the configuration from file onto memory (maintain the same order in the config file)
	 * @param configFile
	 * @return current object
	 * @throws IOException 
	 */
	private DataNode loadDNConfig (String configFile) throws IOException
	{
		BufferedReader br = null;
		String line = null;

		br = new BufferedReader(new FileReader(configFile));

		// for datanode
		line = br.readLine().trim();
		this.dataNodeID = StringUtility.getSegment(1, line, ":");

		line = br.readLine().trim();
		this.dataNodeIP = StringUtility.getSegment(1, line, ":");

		line = br.readLine().trim();
		this.dataNodePort = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

		// for namenode
		line = br.readLine().trim();
		this.nameNodeIP = StringUtility.getSegment(1, line, ":");

		line = br.readLine().trim();
		this.nameNodePort = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

		// for setting the block directory
		line = br.readLine().trim();
		this.blockDir = StringUtility.getSegment(1, line, ":") + "/";

		// intervals
		line = br.readLine().trim();
		this.blockReportInterval = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

		line = br.readLine().trim();
		this.heartbeatInterval = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

		br.close();

		return this;
	}

	
	/**
	 * registers instance with the rmiregistry
	 * @throws RemoteException
	 * @throws NotBoundException
	 */
	private void registerInstance () throws RemoteException, NotBoundException
	{
		// connect to registry
		reg = LocateRegistry.getRegistry(this.nameNodeIP, this.nameNodePort);
		stub = (INameNode) reg.lookup("namenode");

		System.out.println("Connection to NN established");
	}

	
	/* (non-Javadoc)
	 * @see com.local.dfs.dataNode.IDataNode#readBlock(byte[])
	 */
	@Override
	public byte[] readBlock(byte[] inp) throws RemoteException 
	{
		ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
		ReadBlockRequest request = null;

		byte[] data = new byte[blockSize * 1024];
		try 
		{
			request = ReadBlockRequest.parseFrom(inp);
			int blockNumber = request.getBlockNumber();

			// now read the block
			FileInputStream fin = new FileInputStream(blockDir + blockNumber);
			int nBytes = fin.read (data);

			// prepare the response
			response.addData(ByteString.copyFrom(data));
			response.setStatus(DataNode.SUCCESS);

			fin.close();
		} 
		catch (IOException e) 
		{
			System.out.println("Error in readBlock()");
			e.printStackTrace();

			// need to sent failure message
			response.setStatus(DataNode.FAIL);
		} 

		return response.build().toByteArray();
	}


	/* (non-Javadoc)
	 * @see com.local.dfs.dataNode.IDataNode#writeBlock(byte[])
	 */
	@Override
	public byte[] writeBlock(byte[] inp) throws RemoteException 
	{
		WriteBlockResponse.Builder response = WriteBlockResponse.newBuilder();
		WriteBlockRequest request = null;

		try 
		{
			request = WriteBlockRequest.parseFrom(inp);
			BlockLocations info = request.getBlockInfo();

			int locationsCount = info.getLocationsCount();
			int blockNumber = info.getBlockNumber();

			ByteString data = request.getData(0);

			// write to file
			FileOutputStream fout = new FileOutputStream(blockDir + blockNumber);
			fout.write(data.toByteArray(), 0, data.size());
			fout.close ();
			
			// print some message
			System.out.println("Block number " + blockNumber + " written");
			
			// add status to response
			response.setStatus(DataNode.SUCCESS);
			
			// if number of locations is more than one, cascading
			if (locationsCount > 1)
			{
				DataNodeLocation newDN = info.getLocations(1);
				
				BlockLocations.Builder newBlockInfo = BlockLocations.newBuilder();
				newBlockInfo.setBlockNumber(blockNumber);
				
				for(int i = 1; i < info.getLocationsCount(); i++)
				{
					newBlockInfo.addLocations(info.getLocations(i));
				}
				
				WriteBlockRequest.Builder newRequest = WriteBlockRequest.newBuilder();
				newRequest.setBlockInfo(newBlockInfo);
				
				// registry lookup
				Registry registry = LocateRegistry.getRegistry(newDN.getIp(), newDN.getPort());
				IDataNode stub = (IDataNode) registry.lookup("datanode");
				
				// new writeBlock() call
				byte[] newResponse = stub.writeBlock(newRequest.build().toByteArray());
				
				// write the status back to the actual response
				int status = WriteBlockResponse.parseFrom(newResponse).getStatus();
				response.setStatus(status);
				
				if (status == DataNode.SUCCESS)
				{
					System.out.println("Replica creation successful!");
				}
				else
				{
					System.out.println("Replica creation failed!");
				}
			}
		} 
		catch (IOException e) 
		{
			System.out.println("Error in writeBlock() : error in writing the file");
			e.printStackTrace();
			
			// need to send status
			response.setStatus(DataNode.FAIL);
		}
		catch (NotBoundException e)
		{
			System.out.println("Error in writeBlock() : error in replica creation, datanode not bound");
			e.printStackTrace();
			
			// need to send status
			response.setStatus(DataNode.FAIL);
		}
		
		return response.build().toByteArray();
	}
	
}		// class
