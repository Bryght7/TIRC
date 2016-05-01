package fr.upem.net.tcp.client;

import static fr.upem.net.tcp.client.ScReaders.readByte;
import static fr.upem.net.tcp.client.ScReaders.readFileData;
import static fr.upem.net.tcp.client.ScReaders.readInt;
import static fr.upem.net.tcp.client.ScReaders.readLong;
import static fr.upem.net.tcp.client.ScReaders.readString;

import java.awt.Color;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Thread monitoring a connection, receives packets for the client.
 * 
 * @author Cheneau and Lee
 *
 */
public class ThreadPrivateConnection implements Runnable {
	private static final Logger LOGGER = Logger.getLogger("ClientLogger");
	private final SocketChannel sc;
	private final String nickname;
	private final ByteBuffer bbin = ByteBuffer.allocate(Client.BUFSIZ);
	private final ClientGUI clientGUI;
	private final Client client;
	private final boolean isMessageThread;
	/**
	 * Associate a nickname with the name of the file to be received from him
	 */
	private HashMap<String, String> filesToReceive = new HashMap<>();

	/**
	 * Constructor.
	 * 
	 * @param sc
	 *            {@link SocketChannel} to monitor
	 * @param nickname
	 *            of client to monitor
	 * @param clientGUI
	 *            GUI where to print
	 * @param client
	 *            to monitor
	 * @param messageThread
	 *            identifies this thread as the thread reading for the private
	 *            message connection
	 */
	public ThreadPrivateConnection(SocketChannel sc, String nickname, ClientGUI clientGUI, Client client,
			boolean messageThread) {
		this.sc = sc;
		this.nickname = nickname;
		this.clientGUI = clientGUI;
		this.client = client;
		this.isMessageThread = messageThread;
	}

	/**
	 * If opcode 12, a private message was received.
	 * 
	 * @param sc
	 *            {@link SocketChannel} where message was received from
	 * @param bb
	 *            {@link ByteBuffer} to read from
	 * @param nickname
	 *            of client who sent private message
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	private void receivedPrivateMessage(SocketChannel sc, ByteBuffer bb, String nickname) throws IOException {
		int msgSize = readInt(sc, bb);
		String msg = readString(sc, bb, msgSize, Client.CS_UTF8);
		clientGUI.println("*" + nickname + "* " + msg, Color.orange);
	}

	/**
	 * If opcode 14, a file transfer request was received
	 * 
	 * @param sc
	 *            {@link SocketChannel} where request was received from
	 * @param bb
	 *            {@link ByteBuffer} to read from
	 * @param nickname
	 *            of client who sent file transfer request
	 * @throws IOException
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
			client.replyFileTransfer(nickname, true);
			filesToReceive.put(nickname, filename);
		} else {
			client.replyFileTransfer(nickname, false);
		}
	}

	/**
	 * If opcode 15, received answer for file transfer request.
	 * 
	 * @param sc
	 *            {@link SocketChannel} where the packet was received from
	 * @param bb
	 *            {@link ByteBuffer} to read from
	 * @param nickname
	 *            of client who sent private message
	 * @throws IOException
	 *             if some I/O error occurs
	 */
	private void proceedFileTransfer(SocketChannel sc, ByteBuffer bb, String nickname) throws IOException {
		byte accept = readByte(sc, bbin);
		switch (accept) {
		case 0: // received an approval
			clientGUI.println(nickname + " has accepted the file transfer.", Color.magenta);
			client.sendFile(nickname);
			break;
		case 1:
			clientGUI.println(nickname + " has refused the file transfer.", Color.magenta);
			client.forgetFileTransfer(nickname);
			break;
		default:
			System.err.println("Unknown opcode: " + accept);
			clientGUI.println("Private connection lost with " + nickname, Color.red);
			client.forgetPrivateConnection(nickname);
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
		client.notifyTransferComplete(nickname);
	}

	private void runMessage() {
		while (!Thread.interrupted()) {
			try {
				byte opcode = readByte(sc, bbin);
				switch (opcode) {
				case 12:
					receivedPrivateMessage(sc, bbin, nickname);
					break;
				case 13:
					clientGUI.println(nickname + " has closed private connection.", Color.blue);
					client.forgetPrivateConnection(nickname);
					return;
				default:
					System.err.println("Unknown opcode: " + opcode);
					clientGUI.println("Private connection lost with " + nickname + ".", Color.red);
					LOGGER.warning("Private connection lost with " + nickname);
					client.forgetPrivateConnection(nickname);
					return;
				}
			} catch (IOException ioe) {
				if (!Thread.interrupted()) {
					clientGUI.println("Private connection lost with " + nickname + ".", Color.red);
					LOGGER.warning("Private connection lost with " + nickname);
				} else {
					clientGUI.println("Private connection closed with " + nickname + ".", Color.blue);
					LOGGER.info("Private connection closed with " + nickname);
				}
				client.forgetPrivateConnection(nickname);
				return;
			}
		}
	}

	private void runFile() {
		while (!Thread.interrupted()) {
			try {
				byte opcode = readByte(sc, bbin);
				switch (opcode) {
				case 13:
					client.forgetPrivateConnection(nickname);
					return;
				case 14:
					receivedFileTransferRequest(sc, bbin, nickname);
					break;
				case 15:
					proceedFileTransfer(sc, bbin, nickname);
					break;
				case 16:
					String filename = filesToReceive.get(nickname);
					clientGUI.println("Transfer started \"" + filename + "\" from " + nickname + ".", Color.magenta);
					receivedFile(sc, bbin, nickname);
					break;
				case 17:
					clientGUI.println(
							nickname + " has received the file \"" + client.getFilenameWithNickname(nickname) + "\".",
							Color.blue);
					client.forgetFileTransfer(nickname);
					break;
				default:
					clientGUI.println("Private connection lost with " + nickname, Color.red);
					LOGGER.warning("Unknown opcode: " + opcode + " received from " + nickname);
					client.forgetPrivateConnection(nickname);
					return;
				}
			} catch (IOException ioe) {
				if (!Thread.interrupted()) {
					clientGUI.println("Private connection lost with " + nickname, Color.red);
					LOGGER.warning("Private connection lost with " + nickname);
				} else {
					LOGGER.info("Private connection closed with " + nickname);
				}
				client.forgetPrivateConnection(nickname);
				return;
			}
		}
	}

	@Override
	public void run() {
		if (isMessageThread) {
			runMessage();
		} else {
			runFile();
		}
	}
}