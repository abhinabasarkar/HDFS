package com.local.dfs.test;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RMIDemo extends Remote {
	public String talk(String name) throws RemoteException;
}