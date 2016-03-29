package com.local.dfs.test;

import java.rmi.Naming;

public class RMIDemoClient {

	public static void main(String[] args) throws Exception {
		String url = new String("rmi://" + args[0] + "/RMIDemo");
		System.out.println(url);
		
		RMIDemo rmiDemo = (RMIDemo) Naming.lookup(url);
		String serverReply = rmiDemo.talk(args[1]);
		System.out.println(serverReply);
	}

}
