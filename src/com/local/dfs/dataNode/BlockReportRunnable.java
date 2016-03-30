package com.local.dfs.dataNode;

import java.rmi.RemoteException;

public class BlockReportRunnable implements Runnable {

	private DataNode dn;
	
	
	/**
	 * @param dn
	 */
	public BlockReportRunnable(DataNode dn) 
	{
		super();
		this.dn = dn;
	}

	
	@Override
	public void run() 
	{
		try 
		{
			dn.sendBlockReport();
		} 
		catch (RemoteException | InterruptedException e) 
		{
			e.printStackTrace();
		}
	}

}
