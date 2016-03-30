package com.local.dfs.dataNode;

import java.rmi.RemoteException;

public class HeartBeatRunnable implements Runnable 
{

	private DataNode dn;
	
	
	/**
	 * @param dn
	 */
	public HeartBeatRunnable(DataNode dn) 
	{
		super();
		this.dn = dn;
	}


	@Override
	public void run() 
	{
		try 
		{
			dn.sendHeartBeat();
		} 
		catch (RemoteException e) 
		{
			e.printStackTrace();
		} 
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
	}

}
