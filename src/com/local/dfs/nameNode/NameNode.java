package com.local.dfs.nameNode;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class NameNode extends UnicastRemoteObject implements INameNode {

	// Auto-generated
	private static final long serialVersionUID = 5848865380480200406L;

	public NameNode() throws RemoteException {
		super();
	}

	public static void main(String[] args) throws RemoteException {
		// TODO Auto-generated method stub

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
