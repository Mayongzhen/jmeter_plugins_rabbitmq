package cn.i2cool.jmeter.protocol.amqp;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.property.TestElementProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

/**
 * JMeter creates an instance of a sampler class for every occurrence of the
 * element in every thread. [some additional copies may be created before the
 * test run starts]
 *
 * Thus each sampler is guaranteed to be called by a single thread - there is no
 * need to synchronize access to instance variables.
 *
 * However, access to class fields must be synchronized.
 */
public class AMQPPublisher extends AMQPSampler implements Interruptible {

	private static final long serialVersionUID = -8420658040465788497L;

	private static final Logger log = LoggerFactory.getLogger(AMQPPublisher.class);

	// ++ These are JMX names, and must not be changed
	private static final String MESSAGE = "AMQPPublisher.Message";
	private static final String MESSAGE_ROUTING_KEY = "AMQPPublisher.MessageRoutingKey";
	private static final String MESSAGE_TYPE = "AMQPPublisher.MessageType";
	private static final String REPLY_TO_QUEUE = "AMQPPublisher.ReplyToQueue";
	private static final String CONTENT_TYPE = "AMQPPublisher.ContentType";
	private static final String CORRELATION_ID = "AMQPPublisher.CorrelationId";
	private static final String MESSAGE_ID = "AMQPPublisher.MessageId";
	private static final String HEADERS = "AMQPPublisher.Headers";

	public static boolean DEFAULT_PERSISTENT = false;
	private static final String PERSISTENT = "AMQPPublisher.Persistent";

	public static boolean DEFAULT_USE_TX = false;
	private static final String USE_TX = "AMQPPublisher.UseTx";

	private transient Channel channel;

	public AMQPPublisher() {
		super();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SampleResult sample(Entry e) {
		SampleResult result = new SampleResult();
		result.setSampleLabel(getName());
		result.setSuccessful(false);
		result.setResponseCode("500");
		try {
			initChannel();
		} catch (Exception ex) {
			log.error("Failed to initialize channel : ", ex);
			result.setResponseMessage(ex.toString());
			return result;
		}
		String data = getMessage(); // Sampler data
		result.setSampleLabel(getTitle());
		int loop = getIterationsAsInt();
		result.sampleStart(); // Start timing
		try {
			AMQP.BasicProperties messageProperties = getProperties();
			byte[] messageBytes = getMessageBytes();

			for (int idx = 0; idx < loop; idx++) {
				channel.basicPublish(getExchange(), getMessageRoutingKey(), messageProperties, messageBytes);

			}
			if (getUseTx()) {
				channel.txCommit();
			}
			result.setSamplerData(data);
			result.setResponseData(new String(messageBytes, "UTF-8"), "UTF-8");
			result.setDataType(SampleResult.TEXT);
			result.setResponseCodeOK();
			result.setResponseMessage("OK");
			result.setSuccessful(true);
		} catch (Exception ex) {
			log.debug(ex.getMessage(), ex);
			result.setResponseCode("000");
			result.setResponseMessage(ex.toString());
		} finally {
			result.sampleEnd(); // End timimg
		}

		return result;
	}

	private byte[] getMessageBytes() {
		return getMessage().getBytes();
	}

	/**
	 * @return the message routing key for the sample
	 */
	public String getMessageRoutingKey() {
		return getPropertyAsString(MESSAGE_ROUTING_KEY);
	}

	public void setMessageRoutingKey(String content) {
		setProperty(MESSAGE_ROUTING_KEY, content);
	}

	/**
	 * @return the message for the sample
	 */
	public String getMessage() {
		return getPropertyAsString(MESSAGE);
	}

	public void setMessage(String content) {
		setProperty(MESSAGE, content);
	}

	/**
	 * @return the message type for the sample
	 */
	public String getMessageType() {
		return getPropertyAsString(MESSAGE_TYPE);
	}

	public void setMessageType(String content) {
		setProperty(MESSAGE_TYPE, content);
	}

	/**
	 * @return the reply-to queue for the sample
	 */
	public String getReplyToQueue() {
		return getPropertyAsString(REPLY_TO_QUEUE);
	}

	public void setReplyToQueue(String content) {
		setProperty(REPLY_TO_QUEUE, content);
	}

	public String getContentType() {
		return getPropertyAsString(CONTENT_TYPE);
	}

	public void setContentType(String contentType) {
		setProperty(CONTENT_TYPE, contentType);
	}

	/**
	 * @return the correlation identifier for the sample
	 */
	public String getCorrelationId() {
		return getPropertyAsString(CORRELATION_ID);
	}

	public void setCorrelationId(String content) {
		setProperty(CORRELATION_ID, content);
	}

	/**
	 * @return the message id for the sample
	 */
	public String getMessageId() {
		return getPropertyAsString(MESSAGE_ID);
	}

	public void setMessageId(String content) {
		setProperty(MESSAGE_ID, content);
	}

	public Arguments getHeaders() {
		return (Arguments) getProperty(HEADERS).getObjectValue();
	}

	public void setHeaders(Arguments headers) {
		setProperty(new TestElementProperty(HEADERS, headers));
	}

	public Boolean getPersistent() {
		return getPropertyAsBoolean(PERSISTENT, DEFAULT_PERSISTENT);
	}

	public void setPersistent(Boolean persistent) {
		setProperty(PERSISTENT, persistent);
	}

	public Boolean getUseTx() {
		return getPropertyAsBoolean(USE_TX, DEFAULT_USE_TX);
	}

	public void setUseTx(Boolean tx) {
		setProperty(USE_TX, tx);
	}

	@Override
	public boolean interrupt() {
		cleanup();
		return true;
	}

	@Override
	protected Channel getChannel() {
		return channel;
	}

	@Override
	protected void setChannel(Channel channel) {
		this.channel = channel;
	}

	protected AMQP.BasicProperties getProperties() {
		final AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

		final int deliveryMode = getPersistent() ? 2 : 1;
		final String contentType = StringUtils.defaultIfEmpty(getContentType(), "text/plain");

		builder.contentType(contentType).deliveryMode(deliveryMode).priority(0).correlationId(getCorrelationId())
				.replyTo(getReplyToQueue()).type(getMessageType()).headers(prepareHeaders()).build();
		if (getMessageId() != null && !getMessageId().isEmpty()) {
			builder.messageId(getMessageId());
		}
		return builder.build();
	}

	@Override
	protected boolean initChannel() throws IOException, NoSuchAlgorithmException, KeyManagementException {
		boolean ret = super.initChannel();
		if (getUseTx()) {
			channel.txSelect();
		}
		return ret;
	}

	private Map<String, Object> prepareHeaders() {
		Map<String, Object> result = new HashMap<>();
		Map<String, String> source = getHeaders().getArgumentsAsMap();
		for (Map.Entry<String, String> item : source.entrySet()) {
			result.put(item.getKey(), item.getValue());
		}
		return result;
	}
}
