package com.local.dfs.test;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RMIDemoImpl extends UnicastRemoteObject implements RMIDemo {
	private static final long serialVersionUID = 1L;

	protected RMIDemoImpl() throws RemoteException {
		super();
	}

	@Override
	public String talk(String name) throws RemoteException {
		System.out.println("Client said: Yo " + name + " bro!");
		return "Server says: Hello " + name + " Mama!";
	}
}
