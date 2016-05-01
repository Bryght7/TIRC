package fr.upem.net.tcp.reader;

public interface Reader {
	
	enum Status {
		DONE, REFILL, ERROR;
	}

	/**
	 * Process ridden data.
	 * 
	 * @return {@code Status}:
	 * <ul>
	 * 	<li>{@code DONE}: if could process all data</li>
	 * 	<li>{@code REFILL}: if missing data to process</li>
	 * 	<li>{@code ERROR}: if some error occurs while processing.</li>
	 * </ul>
	 */
	public Status process();
	
	/**
	 * <p>Get data from {@code Reader} once it has finished processing. Each time this method is called
	 * the next field in the reader is returned.</p>
	 * 
	 * <p>Loop once finished.</p>
	 * 
	 * @return the next field to get.
	 */
	public Object get();
	
	/**
	 * Reset the {@code Reader}:
	 * <ul>
	 * 	<li>Reset the current {@code Status} of the reader.</li>
	 * 	<li>Reset the current field for {@code get}.</li>
	 * </ul>
	 */
	public void reset();
}
