package fr.upem.net.tcp.nonblocking;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Server for the cat in non-blocking mode.
 * 
 * @author Cheneau and Lee
 *
 */
public class Server {
	private static final Logger LOGGER = Logger.getLogger("ServerLogger");
	private FileHandler fh;
	/** Maximum nickname size in bytes (or length in ASCII). */
	public static final int MAX_NICKSIZ = 15;
	/** Maximum message size in bytes. */
	public static final int MAX_MSGSIZ = 2048;
	/** Maximum messages that a context can hold. */
	public static final int MAX_MSG = 100;
	/** {@link Charset} used for encoding nicknames. */
	public static final Charset CHARSET_NICKNAME = Charset.forName("ASCII");
	/** {@link Charset} used for encoding messages. */
	public static final Charset CHARSET_MSG = Charset.forName("UTF-8");
	/** TIMEOUT client inactivity */
	private static final int TIMEOUT = 5000;
	/** Max time a client can be inactive before timeout */
	public static final int MAX_INACTIVITY_COUNTER = 1;
	private final ServerSocketChannel serverSocketChannel;
	private final Selector selector;
	private final Set<SelectionKey> selectedKeys;
	/** {@link HashMap} associating a client's nickname with its context. **/
	private final HashMap<String, Context> clients = new HashMap<>();
	private int numberConnected;
	/** Last time timeout check was run */
	private long lastTimeoutCheck;

	/* Server core */

	/**
	 * Constructor.
	 * 
	 * @param port
	 *            where {@code Server} will listen
	 * @throws IOException
	 *             if some I/O errors occurs
	 */
	public Server(int port) throws IOException {
		serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.bind(new InetSocketAddress(port));
		selector = Selector.open();
		selectedKeys = selector.selectedKeys();
	}

	/**
	 * Launch server in ready state.
	 * 
	 */
	public void launch() {
		try {
			fh = new FileHandler("./Serverlogs", true);
			LOGGER.addHandler(fh);
			LOGGER.setLevel(Level.ALL);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);

			serverSocketChannel.configureBlocking(false);
			serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
			Set<SelectionKey> selectedKeys = selector.selectedKeys();
			LOGGER.info("Server launched");
			while (!Thread.interrupted()) {
				printKeys();
				LOGGER.fine("Starting select");
				selector.select(TIMEOUT);
				long time = System.currentTimeMillis();
				if (Thread.interrupted()) {
					LOGGER.info("Shutdown");
					shutdown();
					return;
				}
				LOGGER.fine("Select finished");
				printSelectedKey();
				try {
					processNonSelectedKeys(time);
					processSelectedKeys();
				} catch (IOException e) {
					LOGGER.info("Shutdown");
					shutdown();
					return;
				}
				selectedKeys.clear();
			}
		} catch (IOException ioe) {
			LOGGER.log(Level.SEVERE, ioe.toString(), ioe);
		}
	}

	/**
	 * Perform the right operation on each {@link SelectionKey} depending on its
	 * state.
	 * 
	 * @throws IOException
	 *             if some I/O error occurs on server's side
	 */
	private void processSelectedKeys() throws IOException {
		for (SelectionKey key : selectedKeys) {
			if (key.isValid() && key.isAcceptable()) {
				doAccept(key);
			}
			try {
				if (key.isValid() && key.isWritable()) {
					doWrite(key);
				}
				if (key.isValid() && key.isReadable()) {
					doRead(key);
				}
			} catch (IOException ioe) {
				SocketChannel sc = (SocketChannel) key.channel();
				LOGGER.warning(remoteAddressToString(sc) + ": " + ioe.toString());
				silentlyClose(sc);
			}
		}
	}

	/**
	 * If timeout has exceeded check on each non-selected keys for timeout.
	 * 
	 * @param time
	 *            current time
	 */
	private void processNonSelectedKeys(long time) {
		if (time - lastTimeoutCheck > TIMEOUT) {
			lastTimeoutCheck = time;
			Set<SelectionKey> tmp = new HashSet<>(selector.keys());
			tmp.removeAll(selectedKeys);
			for (SelectionKey key : selector.keys()) {
				Context context = (Context) key.attachment();
				if (null != context) { // not server key
					context.checkForTimeout();
				}
			}
		}
	}

	/**
	 * Accept a new client connection.
	 * 
	 * @param key
	 *            {@link SelectionKey} associated to new client.
	 * @throws IOException
	 *             if some I/O error occurs on server's side.
	 */
	private void doAccept(SelectionKey key) throws IOException {
		SocketChannel sc = serverSocketChannel.accept();
		if (sc == null) {
			return;
		}
		sc.configureBlocking(false);
		Context context = Context.create(this, sc);
		SelectionKey clientKey = sc.register(selector, SelectionKey.OP_READ, context);
		context.setSelectionKey(clientKey);
		LOGGER.info(remoteAddressToString(sc) + " connected");
	}

	/**
	 * Perform a read operation on {@link SelectionKey}.
	 * 
	 * @param key
	 *            to read from
	 * @throws IOException
	 *             if disconnected from client
	 */
	private void doRead(SelectionKey key) throws IOException {
		Context context = (Context) key.attachment();
		context.doRead();
	}

	/**
	 * Perform a write operation on {@link SelectionKey}.
	 * 
	 * @param key
	 *            to write to
	 * @throws IOException
	 *             if disconnected from client
	 */
	private void doWrite(SelectionKey key) throws IOException {
		Context context = (Context) key.attachment();
		context.doWrite();
	}

	/**
	 * Silently close a {@link SocketChannel} without throwing any exception.
	 * 
	 * @param socketChannel
	 *            to close
	 */
	public static void silentlyClose(SocketChannel socketChannel) {
		if (socketChannel != null) {
			try {
				socketChannel.close();
			} catch (IOException e) {
				// Do nothing
			}
		}
	}

	/**
	 * Shutdown server.
	 *
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	public void shutdown() throws IOException {
		serverSocketChannel.close();
	}

	/**
	 * Print server's usage.
	 */
	public static void usage() {
		System.out.println("Usage server: port");
	}

	/* Trigger */

	/**
	 * Notify all connected clients that a new client has joined.
	 * 
	 * @param nickname
	 *            of client who joined
	 */
	private void notifyClientHasJoined(String nickname) {
		for (SelectionKey key : selector.keys()) {
			Context context = (Context) key.attachment();
			if (context == null) {
				// server key
				continue;
			}
			context.clientHasJoined(nickname);
		}
	}

	/**
	 * Notify all connected clients that a new client has left.
	 * 
	 * @param bbNickname
	 *            {@link ByteBuffer} containing client's nickname who left
	 */
	private void notifyClientHasLeft(ByteBuffer bbNickname) {
		for (SelectionKey key : selector.keys()) {
			if (key.isValid()) {
				Context context = (Context) key.attachment();
				if (context == null) {
					// server key
					continue;
				}
				context.clientHasLeft(bbNickname.duplicate());
			}
		}
	}

	/* Request from Context */

	/**
	 * Send a message to all connected clients.
	 * 
	 * @param bbmsg
	 *            {@link ByteBuffer} containing message to send.
	 */
	public void sendMessage(ByteBuffer bbmsg) {
		for (SelectionKey key : selector.keys()) {
			Context context = (Context) key.attachment();
			if (context == null) {
				// server key
				continue;
			}
			context.registerMessage(bbmsg.duplicate());
		}
	}

	/**
	 * Register a new client on server.
	 * 
	 * @param nickname
	 *            of registered client
	 * @param context
	 *            associated with this client
	 * @return {@code true} if client has been registered, {@code false}
	 *         otherwise.
	 */
	public boolean registerClient(String nickname, Context context) {
		if (null != clients.putIfAbsent(nickname, context)) {
			return false;
		}
		numberConnected++;
		notifyClientHasJoined(nickname);
		LOGGER.info(context.remoteAddressToString() + " has joined as " + nickname);
		return true;
	}

	/**
	 * Unregister a client on server.
	 * 
	 * @param nickname
	 *            of unregistered client
	 * @param context
	 *            associated with this client
	 */
	public void unregisterClient(String nickname, Context context) {
		if (null != clients.remove(nickname)) {
			numberConnected--;
			notifyClientHasLeft(context.getBbNickname().duplicate());
			LOGGER.info(nickname + " has left");
		}
	}

	/**
	 * Getter.
	 * 
	 * @return number of connected clients
	 */
	public int getNumberConnected() {
		return numberConnected;
	}

	/**
	 * Give nicknames of all connected clients.
	 * 
	 * @return {@link ByteBuffer} containing each connected client's nickname
	 *         prefixed by its size.
	 */
	public ByteBuffer getConnectedNicknames() {
		ArrayList<ByteBuffer> list = new ArrayList<>();
		int totalSize = 0;
		for (SelectionKey key : selector.keys()) {
			Context context = (Context) key.attachment();
			if (context == null) {
				// server key
				continue;
			}
			ByteBuffer bbNickname = context.getBbNickname();
			if (null == bbNickname) {
				continue;
			}
			bbNickname.flip();
			totalSize += bbNickname.remaining();
			list.add(bbNickname);
		}
		ByteBuffer bbmsg = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + Integer.BYTES * list.size() + totalSize);
		bbmsg.put((byte) 3);
		bbmsg.putInt(list.size());
		list.forEach(bb -> {
			bbmsg.putInt(bb.remaining());
			bbmsg.put(bb);
		});
		return bbmsg;
	}

	/**
	 * Transmit a private connection request from client A to client B.
	 * 
	 * @param fromNickname
	 *            nickname of client A
	 * @param toNickname
	 *            nickname of client B
	 */
	public void askPermissionPrivateConnection(String fromNickname, String toNickname) {
		Context context = clients.get(toNickname);
		if (null == context) {
			LOGGER.warning("Asking for private connection from " + fromNickname + "with unknown client " + toNickname);
			return;
		}
		context.askPrivateCommunication(fromNickname);
	}

	/**
	 * Transmit accept private connection request from client B to client A.
	 * 
	 * @param fromNickname
	 *            nickname of client B
	 * @param toNickname
	 *            nickname of client A
	 * @param inet
	 *            {@link InetAddress} of client B
	 * @param port
	 *            where client B will listen
	 * @param id
	 *            that client A will need to provide to authenticate
	 */
	public void acceptPrivateConnection(String fromNickname, String toNickname, InetAddress inet, int port, long id) {
		Context context = clients.get(toNickname);
		if (null == context) {
			LOGGER.warning("Accept for private connection from " + fromNickname + "with unknown client " + toNickname);
			return;
		}
		context.acceptPrivateCommunication(fromNickname, inet, port, id);
	}

	/**
	 * Transmit refuse private connection request from client B to client A.
	 * 
	 * @param fromNickname
	 *            nickname of client B
	 * @param toNickname
	 *            nickname of client A
	 */
	public void refusePrivateConnection(String fromNickname, String toNickname) {
		Context context = clients.get(toNickname);
		if (null == context) {
			LOGGER.warning("Refuse for private connection from " + fromNickname + "with unknown client " + toNickname);
			return;
		}
		context.refusePrivateCommunication(fromNickname);
	}

	/* Print debug */

	/**
	 * Build a {@code String} containing all operations the {@link SelectionKey}
	 * is interested in.
	 * 
	 * @param key
	 *            to check
	 * @return {@code String} with all interested operations
	 */
	private String interestOpsToString(SelectionKey key) {
		if (!key.isValid()) {
			return "CANCELLED";
		}
		int interestOps = key.interestOps();
		ArrayList<String> list = new ArrayList<>();
		if ((interestOps & SelectionKey.OP_ACCEPT) != 0)
			list.add("OP_ACCEPT");
		if ((interestOps & SelectionKey.OP_READ) != 0)
			list.add("OP_READ");
		if ((interestOps & SelectionKey.OP_WRITE) != 0)
			list.add("OP_WRITE");
		return String.join("|", list);
	}

	/**
	 * Print for each keys on server its interested operations.
	 */
	private void printKeys() {
		Set<SelectionKey> selectionKeySet = selector.keys();
		if (selectionKeySet.isEmpty()) {
			LOGGER.fine("The selector contains no key : this should not happen!");
			return;
		}
		LOGGER.fine("The selector contains:");
		for (SelectionKey key : selectionKeySet) {
			SelectableChannel channel = key.channel();
			if (channel instanceof ServerSocketChannel) {
				LOGGER.fine("\tKey for ServerSocketChannel : " + interestOpsToString(key));
			} else {
				SocketChannel sc = (SocketChannel) channel;
				LOGGER.fine("\tKey for Client " + remoteAddressToString(sc) + " : " + interestOpsToString(key));
			}

		}
	}

	/**
	 * Return {@code String} representation of a {@link SocketChannel}.
	 * 
	 * @param socketChannel
	 *            to convert in {@code String}
	 * @return {@code String} of the {@link SocketChannel}
	 */
	public static String remoteAddressToString(SocketChannel socketChannel) {
		try {
			return socketChannel.getRemoteAddress().toString();
		} catch (IOException ioe) {
			return "???";
		}
	}

	/**
	 * Print for each selected keys action that it can performs.
	 */
	private void printSelectedKey() {
		if (selectedKeys.isEmpty()) {
			LOGGER.fine("There were not selected keys.");
			return;
		}
		LOGGER.fine("The selected keys are :");
		for (SelectionKey key : selectedKeys) {
			SelectableChannel channel = key.channel();
			if (channel instanceof ServerSocketChannel) {
				LOGGER.fine("\tServerSocketChannel can perform : " + possibleActionsToString(key));
			} else {
				SocketChannel sc = (SocketChannel) channel;
				LOGGER.fine("\tClient " + remoteAddressToString(sc) + " can perform : " + possibleActionsToString(key));
			}
		}
	}

	/**
	 * Build a {@code String} containing all operations the {@link SelectionKey}
	 * can perform.
	 * 
	 * @param key
	 *            to check
	 * @return {@code String} with all interested operations
	 */
	private String possibleActionsToString(SelectionKey key) {
		if (!key.isValid()) {
			return "CANCELLED";
		}
		ArrayList<String> list = new ArrayList<>();
		if (key.isAcceptable())
			list.add("ACCEPT");
		if (key.isReadable())
			list.add("READ");
		if (key.isWritable())
			list.add("WRITE");
		return String.join(" and ", list);
	}
}