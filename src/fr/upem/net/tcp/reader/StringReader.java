package fr.upem.net.tcp.reader;

import java.nio.ByteBuffer;

public class StringReader implements Reader {
	private enum State {
		READINT, READSTR;
	}

	private final ByteBuffer bb;
	private State state = State.READINT;
	private int size;
	private int maxSize;
	private final ByteBuffer bbstr;

	public StringReader(ByteBuffer bb, int maxSize) {
		this.bb = bb;
		this.maxSize = maxSize;
		bbstr = ByteBuffer.allocate(maxSize);
	}

	private void processInt() {
		bb.flip();
		size = bb.getInt();
		bb.compact();
	}

	private void processString() {
		bbstr.clear();
		bb.flip();
		int oldlimit = bb.limit();
		bb.limit(size);
		bbstr.put(bb);
		bb.limit(oldlimit);
		bb.compact();
	}

	@Override
	public Status process() {
		switch (state) {
		case READINT:
			if (bb.position() < Integer.BYTES) {
				return Status.REFILL;
			}
			processInt();
			if (size <= 0 || size > maxSize) {
				System.err.println("Invalide size: " + size + " / " + maxSize);
				return Status.ERROR;
			}
			state = State.READSTR;
			// no break !
		case READSTR:
			if (bb.position() < size) {
				return Status.REFILL;
			}
			processString();
			state = State.READINT;
			break;
		default:
			throw new IllegalStateException("this case should never happen");
		}
		return Status.DONE;
	}

	/**
	 * @return {@link Object} containing the {@link ByteBuffer} of the ridden string
	 */
	@Override
	public Object get() {
		return bbstr;
	}

	@Override
	public void reset() {
		state = State.READINT;
	}

}
