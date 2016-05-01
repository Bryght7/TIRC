package fr.upem.net.tcp.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Objects;

/**
 * Class describing the two connections which make a private connection. It has
 * a connection for private messages, and another for file transfers.
 * 
 * @author Cheneau & Lee
 *
 */
public class DualConnection {
	private final SocketChannel scMessages;
	private final SocketChannel scFiles;

	private DualConnection(SocketChannel scMessages, SocketChannel scFiles) {
		this.scMessages = scMessages;
		this.scFiles = scFiles;
	}

	/**
	 * Creates an instance of {@code DualConnection}, provided two already
	 * opened {@code SocketChannel}s.
	 * 
	 * @param scMessages
	 *            the socket to be used for private messages
	 * @param scFiles
	 *            the socket to be used for file transfers
	 * @return an instance of {@code DualSocketChannel}
	 */
	public static DualConnection createFromScs(SocketChannel scMessages, SocketChannel scFiles) {
		return new DualConnection(Objects.requireNonNull(scMessages), Objects.requireNonNull(scFiles));
	}

	/**
	 * Opens two connections on the specified server. One for private messages,
	 * one for file transfers.
	 * 
	 * @param server
	 *            the server to open the connections on
	 * @param client
	 *            the {@code Client} who is opening those connections
	 * @return an instance of {@code DualSocketChannel}
	 * @throws IOException
	 *             if an I/O occurred on open
	 */
	public static DualConnection createFromServer(InetSocketAddress server) throws IOException {
		SocketChannel scMessages = SocketChannel.open(server);
		SocketChannel scFiles = SocketChannel.open(server);
		return createFromScs(scMessages, scFiles);
	}

	/**
	 * Write the content of the given {@link ByteBuffer} on the messages
	 * connection.
	 * 
	 * @param bb
	 *            {@link ByteBuffer} to be written
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public void writeInMessages(ByteBuffer bb) throws IOException {
		scMessages.write(bb);
	}

	/**
	 * Write the content of the given {@link ByteBuffer} on the file transfers
	 * connection.
	 * 
	 * @param bb
	 *            {@link ByteBuffer} to be written
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public void writeInFiles(ByteBuffer bb) throws IOException {
		scFiles.write(bb);
	}

	/**
	 * Write the content of the given {@link ByteBuffer} on all connections :
	 * private messages and file transfers.
	 * 
	 * @param bb
	 *            {@link ByteBuffer} to be written
	 * @throws IOException
	 */
	public void writeInAll(ByteBuffer bb) throws IOException {
		scMessages.write(bb);
		bb.flip();
		scFiles.write(bb);
	}

	/**
	 * Get threads which will read the specified nickname's private connections
	 * 
	 * @param monitoredNickname
	 *            the nickname to get monitors from
	 * @param clientGUI
	 * @return an array of readers threads ready to be started
	 */
	public Thread[] getReaders(String monitoredNickname, ClientGUI clientGUI, Client client) {
		Runnable rMessages = new ThreadPrivateConnection(scMessages, monitoredNickname, clientGUI, client);
		Runnable rFiles = new ThreadPrivateConnection(scFiles, monitoredNickname, clientGUI, client);
		Thread[] readers = { new Thread(rMessages), new Thread(rFiles) };
		return readers;
	}

	/**
	 * Close all connections making this private connection.
	 * 
	 * @throws IOException
	 *             if I/O error while closing
	 */
	public void closeAll() throws IOException {
		scMessages.close();
		scFiles.close();
	}

}
