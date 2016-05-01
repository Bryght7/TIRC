package fr.upem.net.tcp.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

/**
 * Utility class for reading {@code ByteBuffer}s.
 * 
 * @author Cheneau and Lee
 *
 */
public class ScReaders {

	private ScReaders() {
		// Utils class, no constructor
	}

	/**
	 * Read until buffer is full.
	 * 
	 * @param sc
	 *            {@code SocketChannel} to read from.
	 * @param bb
	 *            {@code ByteBuffer} to save data in.
	 * @return {@code true} if buffer is full, {@code false} if an error
	 *         occurred.
	 * @throws IOException
	 *             If some other I/O error occurs.
	 */
	public static boolean readFully(SocketChannel sc, ByteBuffer bb) throws IOException {
		while (bb.hasRemaining()) {
			if (-1 == sc.read(bb)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Read a {@code byte}.
	 * 
	 * @param sc
	 *            {@code SocketChannel} to read from.
	 * @param bb
	 *            {@code ByteBuffer} to save data in.
	 * @return The read {@code byte}.
	 * @throws IOException
	 *             If some other I/O error occurs.
	 */
	public static byte readByte(SocketChannel sc, ByteBuffer bb) throws IOException {
		bb.clear();
		bb.limit(Byte.BYTES);
		if (!readFully(sc, bb)) {
			throw new IOException("connection lost (readfully byte)");
		}
		bb.flip();
		return bb.get();
	}

	/**
	 * Read an {@code int}.
	 * 
	 * @param sc
	 *            {@code SocketChannel} to read from.
	 * @param bb
	 *            {@code ByteBuffer} to save data in.
	 * @return The read {@code int} value.
	 * @throws IOException
	 *             If some other I/O error occurs.
	 */
	public static int readInt(SocketChannel sc, ByteBuffer bb) throws IOException {
		bb.clear();
		bb.limit(Integer.BYTES);
		if (!readFully(sc, bb)) {
			throw new IOException("connection lost");
		}
		bb.flip();
		return bb.getInt();
	}

	/**
	 * Read a {@code long}.
	 * 
	 * @param sc
	 *            {@code SocketChannel} to read from.
	 * @param bb
	 *            {@code ByteBuffer} to save data in.
	 * @return The read {@code long} value.
	 * @throws IOException
	 *             If some other I/O error occurs.
	 */
	public static long readLong(SocketChannel sc, ByteBuffer bb) throws IOException {
		bb.clear();
		bb.limit(Long.BYTES);
		if (!readFully(sc, bb)) {
			throw new IOException("connection lost");
		}
		bb.flip();
		return bb.getLong();
	}

	/**
	 * Read a string of given size and {@code charset}.
	 * 
	 * @param sc
	 *            {@code SocketChannel} to read from.
	 * @param bb
	 *            {@code ByteBuffer} to save data in.
	 * @param size
	 *            The size of the string to read.
	 * @param cs
	 *            The {@code charset} to use to decode the {@code String}.
	 * @return The {@code String} read.
	 * @throws IOException
	 *             If some other I/O error occurs.
	 */
	public static String readString(SocketChannel sc, ByteBuffer bb, int size, Charset cs) throws IOException {
		bb.clear();
		bb.limit(size);
		if (!readFully(sc, bb)) {
			throw new IOException("connection lost");
		}
		bb.flip();
		return cs.decode(bb).toString();
	}

	/**
	 * Read an IPv4 or IPv6 address.
	 * 
	 * @param sc
	 *            {@code SocketChannel} to read from.
	 * @param bb
	 *            {@code ByteBuffer} to save data in.
	 * @param isIpv4
	 *            {@code true} if it's an IPv4 address, false if it's an IPv6
	 *            address.
	 * @return An array of {@code byte} containing the address.
	 * @throws IOException
	 *             If some other I/O error occurs.
	 */
	public static byte[] readAddress(SocketChannel sc, ByteBuffer bb, boolean isIpv4) throws IOException {
		int size = (isIpv4) ? 4 : 32;
		bb.clear();
		bb.limit(size);
		if (!readFully(sc, bb)) {
			throw new IOException("connection lost");
		}
		bb.flip();
		byte[] addr = new byte[bb.remaining()];
		bb.get(addr);
		return addr;
	}

	/**
	 * Read the specified amount of {@code byte}s.
	 * 
	 * @param sc
	 *            {@code SocketChannel} to read from.
	 * @param size
	 *            of the file to be read
	 * @return the read bytes as an array of bytes
	 * @throws IOException
	 *             If some other I/O error occurs.
	 */
	public static byte[] readFileData(SocketChannel sc, int size) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(size);
		if (!readFully(sc, bb)) {
			throw new IOException("connection lost (readfully byte)");
		}
		bb.flip();
		return bb.array();
	}

}