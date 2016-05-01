package fr.upem.net.tcp.reader;

import java.nio.ByteBuffer;

import fr.upem.net.tcp.nonblocking.Server;

public class PrivateConnectionReader implements Reader {
	private enum State {
		ACCEPT, LOGIN, SESSIONID,
	}

	private State state;
	private final ByteBuffer bb;
	private final StringReader stringReader;
	private byte accept;
	private String fromNickname;
	private long sessionId;
	private int nbget;

	public PrivateConnectionReader(ByteBuffer bb, int maxLoginSize) {
		state = State.ACCEPT;
		this.bb = bb;
		stringReader = new StringReader(bb, maxLoginSize);
	}

	private void processAccept() {
		bb.flip();
		accept = bb.get();
		bb.compact();
	}

	private void processSessionId() {
		bb.flip();
		sessionId = bb.getLong();
		bb.compact();
	}

	@Override
	public Status process() {
		switch (state) {
		case ACCEPT:
			if (bb.position() < Byte.BYTES) {
				return Status.REFILL;
			}
			processAccept();
			state = State.LOGIN;
		case LOGIN:
			Status status = stringReader.process();
			if (status != Status.DONE) {
				return status;
			}
			ByteBuffer bbNickname = (ByteBuffer) stringReader.get();
			bbNickname.flip();
			fromNickname = Server.CHARSET_NICKNAME.decode(bbNickname).toString();
			// Did not accept
			if (accept != (byte) 0) {
				return Status.DONE;
			}
			state = State.SESSIONID;
		case SESSIONID:
			if (bb.position() < Long.BYTES) {
				return Status.REFILL;
			}
			processSessionId();
			break;
		default:
			throw new IllegalStateException("this case should never happen");
		}
		return Status.DONE;
	}

	/**
	 * @return {@link Object}:
	 * <ul>
	 * 	<li>{@code Byte} accept the first time</li>
	 * 	<li>{@code String} nickname the second time</li>
	 * 	<li>{@code Long} session ID the third time</li>
	 * </ul>
	 */
	@Override
	public Object get() {
		if (nbget%3 == 0) {
			nbget++;
			return accept;
		} else if (nbget%3 == 1) {
			nbget++;
			return fromNickname;
		}
		nbget++;
		return sessionId;
	}

	@Override
	public void reset() {
		state = State.ACCEPT;
		nbget = 0;
	}

}
