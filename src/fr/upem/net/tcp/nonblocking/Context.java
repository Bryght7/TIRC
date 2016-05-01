package fr.upem.net.tcp.nonblocking;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.logging.Logger;

import fr.upem.net.tcp.reader.CommandReader;

public class Context {
	private static final Logger LOGGER = Logger.getLogger("ServerLogger");
	private static final int BUFSIZ = 4096;
	private final Server server;
	private final SocketChannel sc;
	private SelectionKey key;
	private final ByteBuffer bbin;
	private final ByteBuffer bbout;
	/** {@code boolean}: connection with server is closed or not. **/
	private boolean isClosed;
	/** {@link Queue} containing messages not yet sent. **/
	private final Queue<ByteBuffer> queue;
	/** {@link CommandReader} process ridden data from client. **/
	private CommandReader commandReader;
	/**
	 * {@link HashMap} that associate an opcode ({@code byte}) with the right
	 * method to call.
	 **/
	private final HashMap<Byte, Runnable> commands = new HashMap<>();
	private String nickname;
	/** {@link ByteBuffer} with client nickname. **/
	private ByteBuffer bbNickname;
	/** Private port where client listen for private communication. **/
	private int privatePort;
	/** {@code boolean}: is registered to server or not. **/
	private boolean isRegistered = false;
	private int inactivityCounter;

	/* Core */

	/**
	 * Constructor.
	 * 
	 * @param bbin
	 *            {@link ByteBuffer} for input
	 * @param bbout
	 *            {@link ByteBuffer} for output
	 * @param queue
	 *            {@link Queue} for messages to send
	 * @param server
	 *            {@link Server} where {@code Context} will be attached
	 * @param sc
	 *            {@link SocketChannel} where will communicate with client.
	 */
	private Context(ByteBuffer bbin, ByteBuffer bbout, Queue<ByteBuffer> queue, Server server,
			SocketChannel sc) {
		this.bbin = bbin;
		this.bbout = bbout;
		this.queue = queue;
		this.sc = sc;
		this.server = server;
		initCommands();
		commandReader = new CommandReader(bbin, Collections.unmodifiableMap(commands));
	}

	/**
	 * Static factory method to create a {@code Context}.
	 * 
	 * @param server
	 *            where context is set
	 * @param sc
	 *            {@link SocketChannel} associated to context
	 * @return an instance of {@code Context}
	 */
	public static Context create(Server server, SocketChannel sc) {
		ByteBuffer bbin = ByteBuffer.allocate(BUFSIZ);
		ByteBuffer bbout = ByteBuffer.allocate(BUFSIZ);
		Queue<ByteBuffer> queue = new LinkedList<>();
		return new Context(bbin, bbout, queue, server, sc);
	}

	/**
	 * Setter for {@link SelectionKey}.
	 * 
	 * @param key
	 *            {@link SelectionKey} to set.
	 */
	public void setSelectionKey(SelectionKey key) {
		this.key = key;
	}

	/**
	 * Getter for {@link ByteBuffer} containing client's nickname.
	 * 
	 * @return {@link ByteBuffer} of client's nickname.
	 */
	public ByteBuffer getBbNickname() {
		return bbNickname.asReadOnlyBuffer();
	}

	/**
	 * Associate to an opcode the right method to call.
	 */
	private void initCommands() {
		commands.put((byte) 0, () -> registerNickname());
		commands.put((byte) 4, () -> receivedMessage());
		commands.put((byte) 6, () -> privateCommunicationRequest());
		commands.put((byte) 8, () -> privateCommunicationAnswer());
		commands.put((byte) 18, () -> disconnect());
		commands.put((byte) 20, () -> keepAlive());
	}

	public String remoteAddressToString() {
		return Server.remoteAddressToString(sc);
	}

	public void checkForTimeout() {
		if (inactivityCounter >= Server.MAX_INACTIVITY_COUNTER) {
			LOGGER.warning(remoteAddressToString() + " (" + nickname + ") has been timeout");
			isClosed = true;
			unregister();
		} else {
			inactivityCounter++;
		}
	}

	/**
	 * Performs a read operation.
	 * 
	 * @throws IOException
	 *             if disconnected from client.
	 */
	public void doRead() throws IOException {
		inactivityCounter = 0;
		if (-1 == sc.read(bbin) || isClosed) {
			Server.silentlyClose(sc);
			unregister();
			return;
		}
		switch (commandReader.process()) {
		case ERROR:
			LOGGER.warning(
					remoteAddressToString() + " (" + nickname + ") did not respect protocol");
			Server.silentlyClose(sc);
			unregister();
			return;
		case DONE:
			break;
		case REFILL:
			break;
		default:
			throw new IllegalStateException("this case should never happen");
		}
		updateInterestOps();
	}

	/**
	 * Performs a write operation.
	 * 
	 * @throws IOException
	 *             if disconnected from client.
	 */
	public void doWrite() throws IOException {
		if (!queue.isEmpty()) {
			int size = queue.peek().position();
			if (bbout.remaining() >= size) {
				ByteBuffer bb = queue.poll();
				bb.flip();
				bbout.put(bb);
			}
		}
		bbout.flip();
		if (-1 == sc.write(bbout) || isClosed) {
			Server.silentlyClose(sc);
			unregister();
			return;
		}
		bbout.clear();
		updateInterestOps();
	}

	/**
	 * Update {@code Context} interest operations depending on what it can
	 * performs.
	 */
	private void updateInterestOps() {
		if (!key.isValid()) {
			return;
		}
		int newInterestOps = 0;
		if (bbout.position() > 0 || !queue.isEmpty()) {
			newInterestOps |= SelectionKey.OP_WRITE;
		}
		if (!isClosed && bbin.hasRemaining()) {
			newInterestOps |= SelectionKey.OP_READ;
		}
		key.interestOps(newInterestOps);
	}

	/**
	 * Register a message to send to client.
	 * 
	 * @param bbmsg
	 *            {@link ByteBuffer} containing the message.
	 */
	public void registerMessage(ByteBuffer bbmsg) {
		if (queue.size() > Server.MAX_MSG) {
			isClosed = true;
			return;
		}
		queue.offer(Objects.requireNonNull(bbmsg));
		key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
	}

	/**
	 * Unregister {@code Context} to server.
	 */
	private void unregister() {
		Server.silentlyClose(sc);
		key.cancel();
		if (isRegistered) {
			server.unregisterClient(nickname, this);
		}
	}

	/**
	 * Packet server send to client after a request connection.
	 * 
	 * @param accept
	 *            {@code true} if accept, {@code false} otherwise
	 */
	private void confirmConnection(boolean accept) {
		byte confirmationByte = (accept) ? (byte) 0 : 1;
		bbout.put((byte) 1);
		bbout.put((byte) confirmationByte);
		bbout.putInt(server.getNumberConnected());
	}

	/* Commands */

	/**
	 * <p>
	 * If received opcode 0, a connection was requested from a client, try to
	 * register him.
	 * </p>
	 * <ul>
	 * <li>If registration has been made send confirm connection packet.</li>
	 * 
	 * <li>If nickname was too long, client did not respect protocol, close
	 * connection.</li>
	 * 
	 * <li>If nickname already taken send refuse connection packet.</li>
	 * </ul>
	 */
	private void registerNickname() {
		nickname = (String) commandReader.get();
		privatePort = (int) commandReader.get();
		if (server.registerClient(nickname, this)) {
			confirmConnection(true);
			bbNickname = Server.CHARSET_NICKNAME.encode(nickname);
			bbNickname.compact(); // always end of data
			isRegistered = true;
			ByteBuffer bbmsg = server.getConnectedNicknames();
			registerMessage(bbmsg);
		} else {
			confirmConnection(false);
			isClosed = true;
		}
	}

	/**
	 * <p>
	 * If received opcode 4, a message was received from client.
	 * </p>
	 * 
	 * <p>
	 * Transfer message to server so it can be sent to all connected clients.
	 * </p>
	 */
	private void receivedMessage() {
		ByteBuffer bbmsg = (ByteBuffer) commandReader.get();
		bbmsg.flip();
		bbNickname.flip();
		ByteBuffer bb = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + bbNickname.remaining()
				+ Integer.BYTES + bbmsg.remaining());
		bb.put((byte) 5);
		bb.putInt(bbNickname.remaining());
		bb.put(bbNickname);
		bb.putInt(bbmsg.remaining());
		bb.put(bbmsg);
		server.sendMessage(bb);
	}

	/**
	 * If received opcode 6, a private connection was requested from client A,
	 * ask client B if he accepts request.
	 */
	private void privateCommunicationRequest() {
		ByteBuffer bbDestNickname = (ByteBuffer) commandReader.get();
		bbDestNickname.flip();
		String destNickname = Server.CHARSET_NICKNAME.decode(bbDestNickname).toString();
		server.askPermissionPrivateConnection(nickname, destNickname);
	}

	/**
	 * If received opcode 8, a private connection answer was received from
	 * client B.
	 * <ul>
	 * <li>Send accept private connection packet to client A if accepted with
	 * his IP address, port and session ID.</li>
	 * <li>Send refuse private connection packet otherwise.
	 */
	private void privateCommunicationAnswer() {
		Byte accept = (Byte) commandReader.get();
		String withNickname = (String) commandReader.get();
		if (accept != (byte) 0) { // refuse
			server.refusePrivateConnection(nickname, withNickname);
			return;
		}
		long sessionId = (long) commandReader.get();
		InetAddress inet = sc.socket().getInetAddress();
		server.acceptPrivateConnection(nickname, withNickname, inet, privatePort, sessionId);
	}

	/**
	 * If received opcode 17, client has left.
	 */
	private void disconnect() {
		unregister();
	}

	private void keepAlive() {
		// just here to update inactivityCounter in doRead
	}

	/* Notification from server */

	/**
	 * Server notify {@code Context} a client has joined.
	 * 
	 * @param nickname
	 *            of client who joined
	 */
	public void clientHasJoined(String nickname) {
		ByteBuffer bbNickname = Server.CHARSET_NICKNAME.encode(nickname);
		ByteBuffer bbmsg = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + bbNickname.remaining());
		bbmsg.put((byte) 2);
		bbmsg.putInt(bbNickname.remaining());
		bbmsg.put(bbNickname);
		registerMessage(bbmsg);
	}

	/**
	 * Server notify {@code Context} a client has left.
	 * 
	 * @param bbNickname
	 *            containing nickname of client who left
	 */
	public void clientHasLeft(ByteBuffer bbNickname) {
		bbNickname.flip();
		int size = bbNickname.remaining();
		ByteBuffer bbmsg = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + size);
		bbmsg.put((byte) 19);
		bbmsg.putInt(size);
		bbmsg.put(bbNickname);
		registerMessage(bbmsg);
	}

	/**
	 * Server notify {@code Context} that a client requested a private
	 * connection.
	 * 
	 * @param fromNickname
	 *            of client who requested the private connection
	 */
	public void askPrivateCommunication(String fromNickname) {
		ByteBuffer bbNickname = Server.CHARSET_NICKNAME.encode(fromNickname);
		int size = bbNickname.remaining();
		ByteBuffer bbmsg = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES + size);
		bbmsg.put((byte) 7);
		bbmsg.putInt(size);
		bbmsg.put(bbNickname);
		registerMessage(bbmsg);
	}

	/**
	 * Accept private connection with client A.
	 * 
	 * @param fromNickName
	 *            of client A who requested a private connection
	 * @param inet
	 *            {@link InetAddress} of client B who accepted
	 * @param port
	 *            of client B who accepted
	 * @param id
	 *            that client A will need to provide to authenticate to client B
	 */
	public void acceptPrivateCommunication(String fromNickName, InetAddress inet, int port,
			long id) {
		ByteBuffer bbNickname = Server.CHARSET_NICKNAME.encode(fromNickName);
		byte[] addr = inet.getAddress();
		int nicknameSize = bbNickname.remaining();
		int addrSize = addr.length;
		Byte ipVersion = 4;
		if (inet instanceof Inet6Address) {
			ipVersion = 6;
		}
		ByteBuffer bb = ByteBuffer.allocate(Byte.BYTES + Byte.BYTES + Integer.BYTES + nicknameSize
				+ Byte.BYTES + addrSize + Integer.BYTES + Long.BYTES);
		bb.put((byte) 9);
		bb.put((byte) 0);
		bb.putInt(nicknameSize);
		bb.put(bbNickname);	
		bb.put(ipVersion);
		bb.put(addr);
		bb.putInt(port);
		bb.putLong(id);
		registerMessage(bb);
	}

	/**
	 * Refuse private connection with client A.
	 * 
	 * @param fromNickName
	 *            of client A
	 */
	public void refusePrivateCommunication(String fromNickName) {
		ByteBuffer bbNickname = Server.CHARSET_NICKNAME.encode(fromNickName);
		int nicknameSize = bbNickname.remaining();
		ByteBuffer bb = ByteBuffer.allocate(Byte.BYTES + Byte.BYTES + Integer.BYTES + nicknameSize);
		bb.put((byte) 9);
		bb.put((byte) 1);
		bb.putInt(nicknameSize);
		bb.put(bbNickname);
		registerMessage(bb);
	}
}
