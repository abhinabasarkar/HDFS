package com.local.dfs.client;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Scanner;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.local.dfs.dataNode.IDataNode;
import com.local.dfs.nameNode.INameNode;
import com.local.dfs.proto.Hdfs.AssignBlockRequest;
import com.local.dfs.proto.Hdfs.AssignBlockResponse;
import com.local.dfs.proto.Hdfs.BlockLocationRequest;
import com.local.dfs.proto.Hdfs.BlockLocationResponse;
import com.local.dfs.proto.Hdfs.BlockLocations;
import com.local.dfs.proto.Hdfs.CloseFileRequest;
import com.local.dfs.proto.Hdfs.CloseFileResponse;
import com.local.dfs.proto.Hdfs.DataNodeLocation;
import com.local.dfs.proto.Hdfs.ListFilesRequest;
import com.local.dfs.proto.Hdfs.ListFilesResponse;
import com.local.dfs.proto.Hdfs.OpenFileRequest;
import com.local.dfs.proto.Hdfs.OpenFileResponse;
import com.local.dfs.proto.Hdfs.ReadBlockRequest;
import com.local.dfs.proto.Hdfs.ReadBlockResponse;
import com.local.dfs.proto.Hdfs.WriteBlockRequest;
import com.local.dfs.proto.Hdfs.WriteBlockResponse;
import com.local.dfs.utilities.StringUtility;

public class Client {

	// you know what this means
	static int blockSize;

	// static status messages
	static final int SUCCESS = 0;
	static final int FAIL = 1;

	private static INameNode nnStub;
	private static Registry nnRegistry;

	// registry lookup strings
	private static String NNString = "NameNode";
	private static String DNString = "DataNode";

	// maintain book of open files
	private static HashMap<String,Integer> openfiles=new HashMap<String, Integer>();

	public static void main(String[] args) 
	{
		if (args.length < 1) 
		{
			System.out.println("Too few arguments. Provide the configuration file!");
			System.exit(0);
		}

		Client client = new  Client();
		try 
		{
			client.init(args[0]);
			client.run ();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		} 
		catch (NotBoundException e) 
		{
			e.printStackTrace();
		}

	}

	/**
	 * @throws NotBoundException
	 * @throws IOException
	 */
	private void run() throws NotBoundException, IOException 
	{
		System.out.println("Client started running! get, put, list, exit");
		Scanner scanner = new Scanner(new InputStreamReader(System.in));	

		while (true) {
			String line = scanner.nextLine();
			String cmd = StringUtility.getSegment(0, line, " ");
			String fileName = null;

			switch (cmd) {
			case "get":
				fileName = StringUtility.getSegment(1, line, " ");
				this.read(fileName);
				break;
			case "put":
				fileName = StringUtility.getSegment(1, line, " ");
				this.write(fileName);
				break;
			case "list":
				this.listFile();
				break;
			case "exit":
				scanner.close();
				System.exit(0);
			default:
				// do nothing
			}
		}
	}

	/**
	 * Initializes by reading config file and looks up registry
	 * @param configFile
	 * @throws IOException
	 * @throws NotBoundException
	 */
	private void init (String configFile) throws IOException, NotBoundException
	{
		String line;

		// read the config file first
		BufferedReader br = new BufferedReader(new FileReader(configFile));

		// get ip of the name node
		line = br.readLine().trim();
		String nnIP = StringUtility.getSegment(1, line, ":");

		// get port of the name node
		line = br.readLine().trim();
		int nnPort = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

		// get block size
		line = br.readLine().trim();
		blockSize = Integer.parseInt(StringUtility.getSegment(1, line, ":"));

		// now lookup remote object in the registry
		nnRegistry = LocateRegistry.getRegistry(nnIP, nnPort);
		nnStub = (INameNode) nnRegistry.lookup(NNString);

		br.close();
	}

	/**
	 * lists files in the HDFS
	 * @throws RemoteException
	 * @throws InvalidProtocolBufferException
	 */
	private void listFile () throws RemoteException, InvalidProtocolBufferException
	{
		ListFilesRequest.Builder req = ListFilesRequest.newBuilder();
		req.setDirName("");

		ListFilesResponse response = ListFilesResponse.parseFrom(nnStub.list(req.build().toByteArray()));

		if (response.getStatus() == SUCCESS)
		{
			System.out.println("List of Files - ");
			for (String file : response.getFileNamesList())
			{
				System.out.println(file);
			}
		}
		else
		{
			System.out.println("Error: Unable to list files in HDFS");
		}
	}


	/**
	 * @param fileName
	 * @throws NotBoundException
	 * @throws IOException
	 */
	private void read (String fileName) throws NotBoundException, IOException
	{
		// open file
		OpenFileRequest.Builder openReq = OpenFileRequest.newBuilder();
		openReq.setFileName(fileName);
		openReq.setForRead(true);

		OpenFileResponse openResponse = OpenFileResponse.parseFrom(nnStub.openFile(openReq.build().toByteArray()));

		// file read possible
		if (openResponse.getStatus() == SUCCESS)
		{
			int handle = openResponse.getHandle();

			// update book
			openfiles.put(fileName, handle);

			// now read
			BlockLocationRequest.Builder blockLocReq = BlockLocationRequest.newBuilder();

			// add the block numbers to the blockLocRequest
			for (int bNo : openResponse.getBlockNumsList())
			{
				blockLocReq.addBlockNums(bNo);
			}

			BlockLocationResponse blockLocResp = 
					BlockLocationResponse.parseFrom(nnStub.getBlockLocations(blockLocReq.build().toByteArray()));

			if (blockLocResp.getStatus() == SUCCESS)
			{
				// delete if exists
				File tmp = new File("output");
				if (tmp.exists())
				{
					tmp.delete();
				}
				
				// actual writing starts
				try(FileOutputStream fw = new FileOutputStream("output", true))
				{
					for (int i = 0; i < blockLocResp.getBlockLocationsCount(); i++)
					{
						BlockLocations b = blockLocResp.getBlockLocations(i);

						int blockNo = b.getBlockNumber();
						DataNodeLocation dnLoc = b.getLocations(0);		// okay to take the first one as we know it's up

						Registry dnReg = LocateRegistry.getRegistry(dnLoc.getIp(), dnLoc.getPort());
						IDataNode dnStub = (IDataNode) dnReg.lookup(DNString);

						// now send the read request
						ReadBlockRequest.Builder rbReq = ReadBlockRequest.newBuilder();
						rbReq.setBlockNumber(blockNo);

						ReadBlockResponse rbResponse = 
								ReadBlockResponse.parseFrom(dnStub.readBlock(rbReq.build().toByteArray()));

						if (rbResponse.getStatus() == SUCCESS)
						{
							ByteString byteString = rbResponse.getData(0);

							// write to local output file
							fw.write(byteString.toByteArray());
						}
						else
						{
							System.out.println("Read error: Unable to read block");
						}
					}

				}
			}
			else
			{
				System.out.println("Read error : Unable to get block locations");
			}

			// need to close the file
			CloseFileRequest.Builder closeRequest = CloseFileRequest.newBuilder();
			closeRequest.setHandle(handle);

			CloseFileResponse closeResponse = 
					CloseFileResponse.parseFrom(nnStub.closeFile(closeRequest.build().toByteArray()));

			if (closeResponse.getStatus() == SUCCESS)
			{
				openfiles.remove(fileName);
				System.out.println("Read: File closed successfully!");
			}
			else
			{
				System.out.println("Some error in closing the file");
			}
		}
		else
		{
			System.out.println("Read error : Unable to open file");
		}
	}


	/**
	 * @param fileName
	 * @throws NotBoundException
	 * @throws IOException
	 */
	private void write (String fileName) throws NotBoundException, IOException
	{
		// open file
		OpenFileRequest.Builder openReq = OpenFileRequest.newBuilder();
		openReq.setFileName(fileName);
		openReq.setForRead(false);

		OpenFileResponse openResponse = OpenFileResponse.parseFrom(nnStub.openFile(openReq.build().toByteArray()));

		// file read possible
		if (openResponse.getStatus() == SUCCESS)
		{
			int handle = openResponse.getHandle();

			// update book
			openfiles.put(fileName, handle);

			// get file size in MB
			long MB = (long) Math.ceil((double) new File(fileName).length() / (1024 * 1024));
			int nBlocks = (int) Math.ceil((double) MB / blockSize);

			System.out.println("Blocks assigned: " + nBlocks);

			byte[] buffer = new byte[blockSize * 1024 * 1024];		// assign a buffer for reading

			File file = new File(fileName);							// open a file handle
			try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file)))		// try blocks to avoid close
			{
				int tmp = 0;
				while ((tmp = bis.read(buffer)) > 0)
				{
					// request for assign block
					AssignBlockRequest.Builder abReq = AssignBlockRequest.newBuilder();
					abReq.setHandle(handle);

					AssignBlockResponse abResponse = 
							AssignBlockResponse.parseFrom(nnStub.assignBlock(abReq.build().toByteArray()));

					// block assignment successful
					if (abResponse.getStatus() == SUCCESS)
					{
						BlockLocations locations = abResponse.getNewBlock();
						DataNodeLocation primaryDN = locations.getLocations(0);

						// remote method call
						Registry dnReg = LocateRegistry.getRegistry(primaryDN.getIp(), primaryDN.getPort());
						IDataNode dnStub = (IDataNode) dnReg.lookup(DNString);

						WriteBlockRequest.Builder wbReq = WriteBlockRequest.newBuilder();
						wbReq.setBlockInfo(locations);
						wbReq.addData(ByteString.copyFrom(buffer, 0, tmp));

						// calling remote
						WriteBlockResponse wbResponse = 
								WriteBlockResponse.parseFrom(dnStub.writeBlock(wbReq.build().toByteArray()));

						if (wbResponse.getStatus() == SUCCESS)
						{
							System.out.println("Block Written!");
						}
						else
						{
							System.out.println("Failure in wrinting block!");
						}
					}
					else
					{
						System.out.println("Assign Block request failed!");
					}
				}
			}

			// need to close the file
			CloseFileRequest.Builder closeRequest = CloseFileRequest.newBuilder();
			closeRequest.setHandle(handle);

			CloseFileResponse closeResponse = 
					CloseFileResponse.parseFrom(nnStub.closeFile(closeRequest.build().toByteArray()));

			if (closeResponse.getStatus() == SUCCESS)
			{
				openfiles.remove(fileName);
				System.out.println("File closed successfully!");
			}
			else
			{
				System.out.println("Some error in closing the file");
			}
		}
		else
		{
			System.out.println("Read error : Unable to open file");
		}
	}

}		// end of class