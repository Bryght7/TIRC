package fr.upem.net.tcp.reader;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import fr.upem.net.tcp.nonblocking.Server;

public class CommandReader implements Reader {
	private enum State {
		OPCODE, COMMAND;
	}

	private State state;
	private final ByteBuffer bb;
	byte opcode;
	/**
	 * {@link HashMap} containing the method to call in Context after reading
	 * data.
	 **/
	private final Map<Byte, Runnable> commands;
	/**
	 * {@link HashMap} associate to each opcode ({@code byte}) the right reader
	 * to call.
	 **/
	private final HashMap<Byte, Reader> readers = new HashMap<>();

	public CommandReader(ByteBuffer bb, Map<Byte, Runnable> commands) {
		state = State.OPCODE;
		this.bb = Objects.requireNonNull(bb);
		this.commands = commands;
		init();
	}

	/**
	 * Initialize {@code readers} {@link HashMap}.
	 */
	private void init() {
		readers.put((byte) 0, new LoginReader(bb, Server.MAX_NICKSIZ)); // co_req
		readers.put((byte) 4, new StringReader(bb, Server.MAX_MSGSIZ)); // pub_msg_req
		readers.put((byte) 6, new StringReader(bb, Server.MAX_NICKSIZ)); // serv_priv_com_req
		readers.put((byte) 8, new PrivateConnectionReader(bb, Server.MAX_NICKSIZ));
	}

	@Override
	public Status process() {
		switch (state) {
		case OPCODE:
			if (bb.position() < Byte.BYTES) {
				return Status.REFILL;
			}
			bb.flip();
			opcode = bb.get();
			bb.compact();
			state = State.COMMAND;
		case COMMAND:
			return processCommand();
		default:
			throw new IllegalStateException("should not be here");
		}
	}

	@Override
	public void reset() {
		// state = State.OPCODE;
		readers.get(opcode).reset();
	}

	private Status processCommand() {
		Reader reader = readers.get(opcode);
		if (null != reader) { // if need more than opcode
			reader.reset();
			Status status = reader.process();
			if (status != Status.DONE) {
				return status;
			}
		}
		Runnable runnable = commands.get(opcode);
		if(null == runnable) {
			return Status.ERROR;
		}
		runnable.run();
		state = State.OPCODE;
		return Status.DONE;
	}

	/**
	 * @return {@link Object}: the next field of current {@code Reader}
	 */
	@Override
	public Object get() {
		return readers.get(opcode).get();
	}
}
