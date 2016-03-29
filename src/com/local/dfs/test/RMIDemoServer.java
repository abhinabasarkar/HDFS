package com.local.dfs.test;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMIDemoServer {

	public static void main(String[] args) throws Exception {
		RMIDemoImpl rmiDemoImplObj = new RMIDemoImpl();
		
		Registry registry = LocateRegistry.createRegistry(1099);
		registry.bind("RMIDemo", rmiDemoImplObj);

		System.out.println("Object bound to the name RMIDemo and is ready for use!");
	}

}
