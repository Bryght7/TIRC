package fr.upem.net.tcp.main;

import java.io.IOException;
import java.net.InetSocketAddress;

import fr.upem.net.tcp.client.Client;

public class MainClient {
	public static void main(String[] args) throws IOException {

		if (args.length != 4) {
			Client.usage();
			return;
		}
		if (args[2].length() > Client.MAX_NICKLEN) {
			System.out.println("Nickname must be " + Client.MAX_NICKLEN + " or less.");
			return;
		}

		Client client;
		try {
			client = Client.create(new InetSocketAddress(args[0], Integer.parseInt(args[1])),
					args[2], Integer.parseInt(args[3]));
		} catch (NumberFormatException nfe) {
			System.err.println("Port: "+nfe);
			Client.usage();
			return;
		}

		if (!client.logMeIn()) {
			return;
		}
		client.launch();
	}
}
