package fr.upem.net.tcp.reader;

import java.nio.ByteBuffer;

import fr.upem.net.tcp.nonblocking.Server;

public class LoginReader implements Reader {
	private enum State {
		USERNAME, PORT;
	}

	private State state;
	private final ByteBuffer bb;
	private final StringReader stringReader;
	private String nickname;
	private int port;
	private int nbget;

	private void processInt() {
		bb.flip();
		port = bb.getInt();
		bb.compact();
	}

	public LoginReader(ByteBuffer bb, int maxLoginSize) {
		state = State.USERNAME;
		this.bb = bb;
		stringReader = new StringReader(bb, maxLoginSize);
	}

	@Override
	public Status process() {
		switch (state) {
		case USERNAME:
			Status status = stringReader.process();
			if (status != Status.DONE) {
				return status;
			}
			ByteBuffer bbNickname = (ByteBuffer) stringReader.get();
			bbNickname.flip();
			nickname = Server.CHARSET_NICKNAME.decode(bbNickname).toString();
			state = State.PORT;
		case PORT:
			if (bb.position() < Integer.BYTES) {
				return Status.REFILL;
			}
			processInt();
			if (port < 0 || port > 65535) {
				return Status.ERROR;
			}
			break;
		default:
			throw new IllegalStateException("should not be here");
		}
		return Status.DONE;
	}

	/**
	 * @return {@link Object}:
	 * <ul>
	 * 	<li>{@code String} nickname the first time</li>
	 * 	<li>{@code Integer} port the second time</li>
	 * </ul>
	 */
	@Override
	public Object get() {
		if (nbget++%2 == 0) {
			return nickname;
		}
		return port;
	}

	@Override
	public void reset() {
		state = State.USERNAME;
		nbget = 0;
	}

}
