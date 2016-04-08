package com.local.dfs.dataNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import com.google.protobuf.ByteString;
import com.local.dfs.nameNode.INameNode;
import com.local.dfs.proto.Hdfs.BlockLocations;
import com.local.dfs.proto.Hdfs.BlockReportRequest;
import com.local.dfs.proto.Hdfs.DataNodeLocation;
import com.local.dfs.proto.Hdfs.HeartBeatRequest;
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
	private int dataNodeID;									// ID of the datanode (not much useful) 
	private String dataNodeIP, nameNodeIP;					// IP address of self and the NN
	private int blockSize;									// in MB
	private int dataNodePort, nameNodePort;					// port numbers for connection
	private int blockReportInterval, heartbeatInterval;		// in seconds
	private Registry reg;									// the rmi registry
	private INameNode NNStub;								// stub for the NN
	private String blockDir;								// directory where the data would be stored

	// registry lookup strings
	private static String NNString = "NameNode";
	private static String DNString = "DataNode";
	
	// static status messages
	static final int SUCCESS = 0;
	static final int FAIL = 1;

	
	/**
	 * constructor 
	 */
	public DataNode() 
	{
		super();
	}

	
	/**
	 * main
	 * @param args
	 */
	public static void main (String[] args)
	{
		String configFile = args[0];
		
		DataNode dn = new DataNode();
		try 
		{
			// initialize
			dn.loadDNConfig(configFile);
			dn.registerInstance();
			
			Registry DNReg;
			try
			{
				DNReg = LocateRegistry.createRegistry(dn.dataNodePort);;
			}
			catch (RemoteException re)
			{
				DNReg = LocateRegistry.getRegistry(dn.dataNodePort);
			}
			
			IDataNode DNStub = (IDataNode) UnicastRemoteObject.exportObject(dn, 0);

			DNReg.rebind(DataNode.DNString, DNStub);
			System.out.println("DN started");
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		catch (NotBoundException e) 
		{
			e.printStackTrace();
		}
		
		// create threads for heart beat and block request
		HeartBeatRunnable hb = new HeartBeatRunnable(dn);
		Thread thb = new Thread(hb);
		thb.start();
		
		BlockReportRunnable br = new BlockReportRunnable(dn);
		Thread tbr = new Thread(br);
		tbr.start();
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
		this.dataNodeID = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

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
		this.blockDir = StringUtility.getSegment(1, line, ":");

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
		NNStub = (INameNode) reg.lookup(NNString);

		System.out.println("Connection to NN established");
	}


	/**
	 * sends heart-beat to NN
	 * @throws RemoteException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unused")
	public void sendHeartBeat () throws RemoteException, InterruptedException
	{		
		// to be continued in infinite loop
		for (;;)
		{
			HeartBeatRequest.Builder request = HeartBeatRequest.newBuilder();
			request.setId(dataNodeID);

			// invoking the remote method
			byte[] rawResponse = NNStub.heartBeat(request.build().toByteArray());

			// sleep for a while before repeating
			Thread.sleep(heartbeatInterval * 1000);
		}
	}

	
	/**
	 * sends block report to NN
	 * @throws RemoteException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unused")
	public void sendBlockReport () throws RemoteException, InterruptedException
	{
		// continue infinitely
		for (;;)
		{
			BlockReportRequest.Builder request = BlockReportRequest.newBuilder();

			// set id to the request
			request.setId(this.dataNodeID);

			// set location to the request
			DataNodeLocation.Builder location = DataNodeLocation.newBuilder();
			location.setIp(this.dataNodeIP);
			location.setPort(this.dataNodePort);
			request.setLocation(location);

			// add all the blocks in this DN
			File curFolder = new File(this.blockDir);
			
			File[] files = curFolder.listFiles();

			for (int i = 0; i < files.length; i++)
			{
				request.addBlockNumbers(Integer.parseInt(files[i].getName()));
			}

			// invoking the remote method
			byte[] rawResponse = NNStub.blockReport(request.build().toByteArray());

			// sleep for a while before repeating
			Thread.sleep(blockReportInterval * 1000);
		}
	}


	/* (non-Javadoc)
	 * @see com.local.dfs.dataNode.IDataNode#readBlock(byte[])
	 */
	@SuppressWarnings("unused")
	@Override
	public byte[] readBlock(byte[] inp) throws RemoteException 
	{
		ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
		ReadBlockRequest request = null;

		try 
		{
			request = ReadBlockRequest.parseFrom(inp);
			int blockNumber = request.getBlockNumber();

			// now read the block
			File f = new File(blockDir + "/" + blockNumber);
			FileInputStream fin = new FileInputStream(f);
			
			byte[] data = new byte[(int) f.length()];
			int nBytes = fin.read (data);

			fin.close();
			
			// prepare the response
			response.addData(ByteString.copyFrom(data));
			response.setStatus(DataNode.SUCCESS);

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
			FileOutputStream fout = new FileOutputStream(blockDir + "/" + blockNumber);
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
				newRequest.addData(data);
				
				// registry lookup
				Registry registry = LocateRegistry.getRegistry(newDN.getIp(), newDN.getPort());
				IDataNode stub = (IDataNode) registry.lookup(DNString);

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

	
}		// end class