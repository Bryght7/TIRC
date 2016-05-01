package fr.upem.net.tcp.client;

import static fr.upem.net.tcp.client.ScReaders.readByte;
import static fr.upem.net.tcp.client.ScReaders.readFileData;
import static fr.upem.net.tcp.client.ScReaders.readInt;
import static fr.upem.net.tcp.client.ScReaders.readLong;
import static fr.upem.net.tcp.client.ScReaders.readString;

import java.awt.Color;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientServer {
	private static final Logger LOGGER = Logger.getLogger("ClientLogger");
	private static final int MAX_THREADS = 10;
	/** Buffer to contain packets from client acting as server */
	private final ByteBuffer bbout = ByteBuffer.allocate(Client.BUFSIZ);
	private final ServerSocketChannel serverSocketChannel;
	/** {@link SocketChannel} array of clients connected to server. */
	private final SocketChannel[] scs;
	/** Array of thread for clients. */
	private final Thread[] threads;
	private final Object lock = new Object();
	/**
	 * Associate to a nickname an id that he will need to provide to
	 * authenticate.
	 */
	private final ConcurrentHashMap<String, Long> privateConnectionsId = new ConcurrentHashMap<>();
	/** Associate to a {@link SocketChannel}, nickname of user connected. */
	private final ConcurrentHashMap<SocketChannel, String> nicknamesFromSc = new ConcurrentHashMap<>();
	/** Associate to a nickname its {@link DualConnection}. */
	private final ConcurrentHashMap<String, DualConnection> socketChannelClients = new ConcurrentHashMap<>();

	/** Associates to a nickname its socket channel for private messages */
	private final ConcurrentHashMap<String, SocketChannel> scMessagesByNickname = new ConcurrentHashMap<>();
	/** Associates to a nickname its socket channel for file transfers */
	private final ConcurrentHashMap<String, SocketChannel> scFilesByNickname = new ConcurrentHashMap<>();
	/**
	 * Associates to a nickname the number of sockets that have been
	 * authenticated
	 */
	private final ConcurrentHashMap<String, Integer> nbAuthenticatedByNickname = new ConcurrentHashMap<>();

	private ClientGUI clientGUI;

	/**
	 * Associate a nickname with the name of the file to be received from him
	 */
	private HashMap<String, String> filesToReceive = new HashMap<>();
	/** Associate nickname to the file to be sent to him */
	private HashMap<String, Path> filesToSendAsServer = new HashMap<>();

	/* Core */

	/** Static factory constructor. */
	private ClientServer(ServerSocketChannel serverSocketChannel, SocketChannel[] scs, Thread[] threads) {
		this.serverSocketChannel = serverSocketChannel;
		this.scs = scs;
		this.threads = threads;
	}

	/**
	 * Create an instance of {@code ClientServer}.
	 * 
	 * @param port
	 *            to listen to
	 * @return instance created
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	public static ClientServer create(int port) throws IOException {
		ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.bind(new InetSocketAddress(port));
		SocketChannel[] scs = new SocketChannel[MAX_THREADS];
		Thread[] threads = new Thread[MAX_THREADS];
		return new ClientServer(serverSocketChannel, scs, threads);
	}

	/**
	 * Set user interface of server.
	 * 
	 * @param clientGUI
	 *            interface to use
	 */
	public void setUI(ClientGUI clientGUI) {
		this.clientGUI = clientGUI;
	}

	/**
	 * Link the files this client (acting as server) will send, with the files
	 * to send in the client.
	 * 
	 * @param map
	 *            the map to the files to send in the client
	 */
	public void setFilesToSend(HashMap<String, Path> map) {
		this.filesToSendAsServer = map;
	}

	/**
	 * Run method for a thread, one thread per client.
	 */
	private void run() {
		int id = Integer.parseInt(Thread.currentThread().getName());
		try {
			while (!Thread.interrupted()) {
				try {
					// accept thread-safe
					SocketChannel client = serverSocketChannel.accept();
					synchronized (lock) {
						scs[id] = client;
					}
					LOGGER.info("Connection accepted with" + Client.remoteAddressToString(client));
					try {
						serve(client);
					} catch (AsynchronousCloseException ace) {
						// client timeout continue
						continue;
					} catch (IOException ioe) { // disconnected with client
						LOGGER.log(Level.WARNING, ioe.toString(), ioe);
					} catch (InterruptedException ie) {
						LOGGER.log(Level.INFO, "Server interrupted: " + ie.toString(), ie);
						return;
					} finally {
						LOGGER.info("Private connection closed.");
						// close both messages and files connections
						DualConnection connection = getDualConnectionFromSc(client);
						if (null != connection) {
							silentlyClose(connection);
						}
					}
				} catch (ClosedChannelException ace) {
					return;
				}

			}
		} catch (IOException e) {
			return;
		}
	}

	private DualConnection getDualConnectionFromSc(SocketChannel client) {
		String nicknameServed = nicknamesFromSc.get(client);
		if (null == nicknameServed) {
			return null;
		}
		return socketChannelClients.get(nicknameServed);
	}

	/**
	 * Serve a client for messages.
	 * 
	 * @param sc
	 *            {@link SocketChannel} where message are received
	 * @param bbin
	 *            {@link ByteBuffer} where input is saved
	 * @throws IOException
	 *             if disconnected with client
	 */
	private void serveMessage(SocketChannel sc, ByteBuffer bbin) throws IOException {
		String nicknameServed = nicknamesFromSc.get(sc);
		if (null == nicknameServed) {
			LOGGER.warning("Unknown client attempted to connect.");
			return;
		}
		boolean hasClosed = false;
		while (true) {
			try {
				Byte opcode = readByte(sc, bbin);
				switch (opcode) {
				case 12:
					receivedMessage(sc, bbin, nicknameServed);
					break;
				case 13:
					hasClosed = true;
					clientGUI.println(nicknameServed + " has closed private connection.", Color.blue);
					return;
				default:
					LOGGER.warning("Unknown opcode: " + opcode + " from " + nicknameServed);
					return;
				}
			} catch (IOException ioe) {
				if (!hasClosed && socketChannelClients.containsKey(nicknameServed)) {
					clientGUI.println("Lost private connection with " + nicknameServed, Color.red);
					LOGGER.warning("Lost private connection with " + nicknameServed);
					unregisterClient(nicknameServed);
					throw ioe;
				} else {
					clientGUI.println("Closed private connection with " + nicknameServed, Color.blue);
					LOGGER.info("Closed private connection with " + nicknameServed);
					return;
				}
			}
		}
	}

	/**
	 * Serve a client for files.
	 * 
	 * @param sc
	 *            {@link SocketChannel} where files are received
	 * @param bbin
	 *            {@link ByteBuffer} where input is saved
	 * @throws IOException
	 *             if disconnected with client
	 */
	private void serveFile(SocketChannel sc, ByteBuffer bbin) throws IOException {
		String nicknameServed = nicknamesFromSc.get(sc);
		if (null == nicknameServed) {
			LOGGER.warning("Unknown client attempted to connect.");
			return;
		}
		boolean hasClosed = false;
		while (true) {
			try {
				Byte opcode = readByte(sc, bbin);
				switch (opcode) {
				case 13:
					hasClosed = true;
					return;
				case 14:
					receivedFileTransferRequest(sc, bbin, nicknameServed);
					break;
				case 15:
					receivedFileTransferReply(sc, bbin, nicknameServed);
					break;
				case 16:
					String filename = filesToReceive.get(nicknameServed);
					clientGUI.println("Transfer started \"" + filename + "\" from " + nicknameServed + ".",
							Color.magenta);
					receivedFile(sc, bbin, nicknameServed);
					break;
				case 17:
					clientGUI.println(nicknameServed + " has received the file \""
							+ filesToSendAsServer.get(nicknameServed) + "\".", Color.blue);
					forgetFileTransfer(nicknameServed);
					break;
				default:
					LOGGER.warning("Unknown opcode: " + opcode + " from " + nicknameServed);
					return;
				}
			} catch (IOException ioe) {
				if (!hasClosed && socketChannelClients.containsKey(nicknameServed)) {
					LOGGER.warning("Lost private connection with " + nicknameServed);
					unregisterClient(nicknameServed);
					throw ioe;
				} else {
					LOGGER.info("Closed private connection with " + nicknameServed);
					return;
				}
			}
		}
	}

	/**
	 * Serve a connected user.
	 * 
	 * @param sc
	 *            {@link SocketChannel} of client
	 * @throws IOException
	 *             if some I/O error occurs
	 * @throws InterruptedException
	 *             if thread was interrupted
	 */
	private void serve(SocketChannel sc) throws IOException, InterruptedException {
		ByteBuffer bbin = ByteBuffer.allocate(Client.BUFSIZ);
		byte opcode = readByte(sc, bbin);
		if (!authentication(sc, bbin, opcode)) {
			clientGUI.println("Could not authentificate client", Color.red);
			LOGGER.warning(Client.remoteAddressToString(sc) + ": attempted to connected with false token");
			return;
		}
		if (opcode == 10) {
			serveMessage(sc, bbin);
		} else if (opcode == 11) {
			serveFile(sc, bbin);
		} else {
			LOGGER.severe("Should not be here opcode: " + opcode);
			return;
		}
	}

	/**
	 * Close a private connection with a client.
	 * 
	 * @param nickname
	 *            of user to disconnect with
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	public void closePrivateConnection(String nickname) throws IOException {
		DualConnection connection = socketChannelClients.get(nickname);
		socketChannelClients.remove(nickname);
		nicknamesFromSc.values().remove(nickname);
		silentlyClose(connection);
	}

	/**
	 * Silently close all connections making a private connection, without
	 * throwing any exception.
	 * 
	 * @param connection
	 *            to close
	 */
	private static void silentlyClose(DualConnection connection) {
		if (connection != null) {
			try {
				connection.closeAll();
			} catch (IOException e) {
				// Do nothing
			}
		}
	}

	/**
	 * Shutdown now the server.
	 * 
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	public void shutdownNow() throws IOException {
		for (Thread t : threads) {
			t.interrupt();
		}
		serverSocketChannel.close();
	}

	/**
	 * Launch server in ready state.
	 * 
	 * @throws IOException
	 */
	public void launch() throws IOException {
		for (int i = 0; i < MAX_THREADS; i++) {
			Thread t = new Thread(() -> run());
			t.setName(i + "");
			threads[i] = t;
		}
		for (int i = 0; i < MAX_THREADS; i++) {
			threads[i].start();
		}
	}

	/**
	 * Register a client after a private communication was requested.
	 * 
	 * @param nickname
	 *            of registered client
	 * @param id
	 *            that user will need to send to authenticate
	 * @return {@code true} if client was registered, {@code false} if client is
	 *         already registered
	 */
	public boolean registerClient(String nickname, long id) {
		if (null != privateConnectionsId.putIfAbsent(nickname, id)) {
			return false;
		}
		return true;
	}

	private void unregisterClient(String nickname) {
		socketChannelClients.remove(nickname);
		nicknamesFromSc.values().remove(nickname);
	}

	public void revokeRequest(String nickname) {
		privateConnectionsId.remove(nickname);
	}

	/**
	 * Send a private request to a connected client, in both the private
	 * messages and the file transfers connections.
	 * 
	 * @param toNickname
	 *            nickname of client to send message to
	 * @param bbmsg
	 *            {@link ByteBuffer} containing the message
	 * @return {@code true} if message was sent, {@code false} if client was not
	 *         found
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	public boolean sendPrivateGlobal(String toNickname, ByteBuffer bb) throws IOException {
		DualConnection connection = socketChannelClients.get(toNickname);
		if (null == connection) {
			return false;
		}
		bb.flip();
		connection.writeInAll(bb);
		return true;
	}

	/**
	 * Send a private message to a connected client, in the private message
	 * connection only.
	 * 
	 * @param toNickname
	 *            nickname of client to send message to
	 * @param bbmsg
	 *            {@link ByteBuffer} containing the message
	 * @return {@code true} if message was sent, {@code false} if client was not
	 *         found
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	public boolean sendPrivateMessage(String toNickname, ByteBuffer bbmsg) throws IOException {
		DualConnection connection = socketChannelClients.get(toNickname);
		if (null == connection) {
			return false;
		}
		bbmsg.flip();
		connection.writeInMessages(bbmsg);
		return true;
	}

	/**
	 * Send a private file to a connected client, in the file transfers
	 * connection only.
	 * 
	 * @param toNickname
	 *            nickname of client to send the file to
	 * @param bbFile
	 *            {@link ByteBuffer} containing the file
	 * @return {@code true} if message was sent, {@code false} if client was not
	 *         found
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	public boolean sendPrivateFile(String toNickname, ByteBuffer bbFile) throws IOException {
		DualConnection connection = socketChannelClients.get(toNickname);
		if (null == connection) {
			return false;
		}
		bbFile.flip();
		connection.writeInFiles(bbFile);
		return true;
	}

	/**
	 * Check if a client is connected to server.
	 * 
	 * @param clientNickname
	 *            nickname of client to check
	 * @return {@code true} if client is connected, {@code false} otherwise
	 */
	public boolean isConnected(String clientNickname) {
		return socketChannelClients.containsKey(clientNickname);
	}

	/* Request from client */

	/**
	 * Attempt to authenticate a client that has joined.
	 * 
	 * @param sc
	 *            {@link SocketChannel} of client who joined
	 * @param bb
	 *            {@link ByteBuffer} to read from
	 * @return {@code true} if client was authenticated, {@code false} otherwise
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	private boolean authentication(SocketChannel sc, ByteBuffer bb, byte opcode) throws IOException {
		if (opcode != (byte) 10 && opcode != (byte) 11) {
			LOGGER.warning("Unexpected opcode: " + opcode);
		}
		int nicknameSize = readInt(sc, bb);
		String clientNickname = readString(sc, bb, nicknameSize, Client.CS_NICKNAME);
		long id = readLong(sc, bb);
		long givenId = privateConnectionsId.getOrDefault(clientNickname, (long) 0);

		if ((long) 0 == givenId || givenId != id) {
			LOGGER.warning(
					"Wrong token given from: " + clientNickname + " (given=" + givenId + " expected=" + id + ")");
			return false;
		}

		if (opcode == (byte) 10) {
			nicknamesFromSc.put(sc, clientNickname);
			// increment nbAuthenticated associated with clientNickname
			if ( nbAuthenticatedByNickname.containsKey(clientNickname) ) {
				// increment
				nbAuthenticatedByNickname.put(clientNickname, nbAuthenticatedByNickname.get(clientNickname).intValue() + 1);
			} else {
				// initialize and count 1
				nbAuthenticatedByNickname.put(clientNickname, 1);
			}
			// remember current sc as a scMessages
			scMessagesByNickname.put(clientNickname, sc); 
		}
		if (opcode == (byte) 11) {
			nicknamesFromSc.put(sc, clientNickname);
			if ( nbAuthenticatedByNickname.containsKey(clientNickname) ) {
				// increment
				nbAuthenticatedByNickname.put(clientNickname, nbAuthenticatedByNickname.get(clientNickname).intValue() + 1);
			} else {
				// initialize and count 1
				nbAuthenticatedByNickname.put(clientNickname, 1);
			}
			// remember current sc as a scFiles
			scFilesByNickname.put(clientNickname, sc);
		}
		// if received both opcode 10+11, and we remembered both channels :
		if (nbAuthenticatedByNickname.get(clientNickname) == 2) {
			nbAuthenticatedByNickname.put(clientNickname, 0); // reset
			DualConnection connection;
			synchronized (lock) {
				connection = DualConnection.createFromScs(scMessagesByNickname.get(clientNickname), scFilesByNickname.get(clientNickname));
			}
			socketChannelClients.put(clientNickname, connection);
			privateConnectionsId.remove(clientNickname); // no more needed
			clientGUI.println("Private connection established with " + clientNickname + ".", Color.blue);
			clientGUI.println("To send a private message, use: /w " + clientNickname, Color.blue);
			clientGUI.println("To send a file, use: /f " + clientNickname, Color.blue);
		}
		return true;
	}

	/* handle opcode */

	/**
	 * If opcode 12, a message was received.
	 * 
	 * @param sc
	 *            {@link SocketChannel} where message was received from
	 * @param bb
	 *            {@link ByteBuffer} to save output to
	 * @param nickname
	 *            of client who sent message
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	private void receivedMessage(SocketChannel sc, ByteBuffer bb, String nickname) throws IOException {
		int msgSize = readInt(sc, bb);
		String msg = readString(sc, bb, msgSize, Client.CS_UTF8);
		clientGUI.println("*" + nickname + "* " + msg, Color.orange);
	}

	/**
	 * If opcode 14, a file transfer request was received.
	 * 
	 * @param sc
	 *            {@link SocketChannel} where message was received from
	 * @param bb
	 *            {@link ByteBuffer} to save output to
	 * @param nickname
	 *            of client who sent message
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	private void receivedFileTransferRequest(SocketChannel sc, ByteBuffer bb, String nickname) throws IOException {
		int filenameSize = readInt(sc, bb);
		String filename = readString(sc, bb, filenameSize, Client.CS_UTF8);
		long filesize = readLong(sc, bb);
		clientGUI.println(nickname + " wants to send you the file \"" + filename + "\" (" + filesize + " B).",
				Color.magenta);
		clientGUI.println("Accept ? (/yf " + nickname + " or /nf " + nickname + ")", Color.magenta);
		// TODO get user input (help)
		String input = "yf";
		if (input.equals("yf")) {
			acceptFileTransfer(nickname);
			filesToReceive.put(nickname, filename);
		} else {
			refuseFileTransfer(nickname);
		}
	}

	/**
	 * If opcode 15, received file transfer reply
	 * 
	 * @param sc
	 *            {@link SocketChannel} where the reply came from
	 * @param bb
	 *            {@link ByteBuffer} to save output to
	 * @param nickname
	 *            of client who sent the reply
	 * @throws IOException
	 */
	private void receivedFileTransferReply(SocketChannel sc, ByteBuffer bb, String nickname) throws IOException {
		byte accept = readByte(sc, bb);
		switch (accept) {
		case 0: // received an approval
			clientGUI.println(nickname + " has accepted the file transfer.", Color.magenta);
			sendFile(nickname);
			break;
		case 1:
			clientGUI.println(nickname + " has refused the file transfer.", Color.magenta);
			forgetFileTransfer(nickname);
			break;
		default:
			System.err.println("Unknown opcode: " + accept);
			clientGUI.println("Private connection lost with " + nickname, Color.red);
			forgetFileTransfer(nickname);
			return;
		}
	}

	/**
	 * If opcode 16, a file was received.
	 * 
	 * @param sc
	 *            {@link SocketChannel} where message was received from
	 * @param bb
	 *            {@link ByteBuffer} to save output to
	 * @param nickname
	 *            of client who sent message
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	private void receivedFile(SocketChannel sc, ByteBuffer bb, String nickname) throws IOException {
		long filesize = readLong(sc, bb);
		byte[] data = readFileData(sc, Byte.BYTES * (int) filesize);
		String filename = filesToReceive.get(nickname);
		FileOutputStream fileStream = new FileOutputStream(filename);
		fileStream.write(data);
		fileStream.close();
		filesToReceive.remove(nickname); // done transferring the file
		clientGUI.println("Transfer complete \"" + filename + "\" (" + filesize + " B) from " + nickname + ".",
				Color.magenta);
		notifyTransferComplete(nickname);
	}

	/* packets to send as a SERVER */

	// TODO maybe this belongs in Client.java after implementing user input ?
	private void acceptFileTransfer(String nickname) throws IOException {
		DualConnection connection = socketChannelClients.get(nickname);
		if (null == connection) {
			return;
		}
		bbout.clear();
		bbout.put((byte) 15);
		bbout.put((byte) 0);
		bbout.flip();
		connection.writeInFiles(bbout);
	}

	private void refuseFileTransfer(String nickname) throws IOException {
		DualConnection connection = socketChannelClients.get(nickname);
		if (null == connection) {
			return;
		}
		bbout.clear();
		bbout.put((byte) 15);
		bbout.put((byte) 1);
		bbout.flip();
		connection.writeInFiles(bbout);
	}

	private void notifyTransferComplete(String nickname) throws IOException {
		DualConnection connection = socketChannelClients.get(nickname);
		if (null == connection) {
			return;
		}
		bbout.clear();
		bbout.put((byte) 17);
		bbout.flip();
		connection.writeInFiles(bbout);
	}

	private void sendFile(String nickname) throws IOException {
		DualConnection connection = socketChannelClients.get(nickname);
		if (null == connection) {
			return;
		}
		Path path = filesToSendAsServer.get(nickname);
		ByteBuffer bbFile = Client.packetFile(path);
		bbFile.flip();
		connection.writeInFiles(bbFile);
	}

	public void addFileToSendAsServer(String toNickname, Path path) {
		filesToSendAsServer.put(toNickname, path);
	}

	public void forgetFileTransfer(String nickname) {
		filesToSendAsServer.remove(nickname);
	}

}