package cn.i2cool.jmeter.protocol.amqp;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;

public class AMQPConsumer extends AMQPSampler implements Interruptible, TestStateListener {
	private static final int DEFAULT_PREFETCH_COUNT = 0; // unlimited

	public static final boolean DEFAULT_READ_RESPONSE = true;
	public static final String DEFAULT_PREFETCH_COUNT_STRING = Integer.toString(DEFAULT_PREFETCH_COUNT);

	private static final long serialVersionUID = 7480863561320459091L;

	private static final Logger log = LoggerFactory.getLogger(AMQPConsumer.class);

	// ++ These are JMX names, and must not be changed
	private static final String PREFETCH_COUNT = "AMQPConsumer.PrefetchCount";
	private static final String READ_RESPONSE = "AMQPConsumer.ReadResponse";
	private static final String PURGE_QUEUE = "AMQPConsumer.PurgeQueue";
	private static final String AUTO_ACK = "AMQPConsumer.AutoAck";
	private static final String RECEIVE_TIMEOUT = "AMQPConsumer.ReceiveTimeout";
	public static final String TIMESTAMP_PARAMETER = "Timestamp";
	public static final String EXCHANGE_PARAMETER = "Exchange";
	public static final String ROUTING_KEY_PARAMETER = "Routing Key";
	public static final String DELIVERY_TAG_PARAMETER = "Delivery Tag";

	public static boolean DEFAULT_USE_TX = false;
	private static final String USE_TX = "AMQPConsumer.UseTx";

	private transient Channel channel;
	private transient String consumerTag;

	public AMQPConsumer() {
		super();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SampleResult sample(Entry entry) {
		SampleResult result = new SampleResult();
		result.sampleStart();
		result.setSampleLabel(getName());
		result.setSuccessful(false);
		result.setResponseCode("500");
		try {
			initChannel();
			GetResponse get = channel.basicGet(getQueue(), autoAck());
			if (getReadResponseAsBoolean()) {
				String response = new String(get.getBody(), "UTF-8");
				result.setSamplerData(String.valueOf(get.getMessageCount()));
				result.setResponseData(response, "UTF-8");
				result.setResponseMessage("OK");
			} else {
				result.setSamplerData("Read response is false.");
			}

			result.setDataType(SampleResult.TEXT);
			result.setResponseHeaders(formatHeaders(get.getEnvelope(), get.getProps()));
			result.setResponseCodeOK();
			result.setSuccessful(true);
			if (!autoAck())
				channel.basicAck(get.getEnvelope().getDeliveryTag(), false);
			if (getUseTx()) {
				channel.txCommit();
			}
		} catch (ShutdownSignalException e) {
			log.warn("AMQP consumer failed to ShutdownSignalException", e);
			result.setResponseCode("400");
			result.setResponseMessage(e.getMessage());
			interrupt();
		} catch (ConsumerCancelledException e) {
			log.warn("AMQP consumer failed to ConsumerCancelledException", e);
			result.setResponseCode("300");
			result.setResponseMessage(e.getMessage());
			interrupt();
		} catch (IOException e) {
			log.warn("AMQP consumer failed to IOException", e);
			result.setResponseCode("100");
			result.setResponseMessage(e.getMessage());
		} catch (KeyManagementException e) {
			log.warn("AMQP consumer failed to KeyManagementException", e);
			result.setResponseMessage(e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			log.warn("AMQP consumer failed to NoSuchAlgorithmException", e);
			result.setResponseMessage(e.getMessage());
		} finally {
			result.sampleEnd(); // End timimg
		}
		return result;
	}

	@Override
	protected Channel getChannel() {
		return channel;
	}

	@Override
	protected void setChannel(Channel channel) {
		this.channel = channel;
	}

	/**
	 * @return the whether or not to purge the queue
	 */
	public String getPurgeQueue() {
		return getPropertyAsString(PURGE_QUEUE);
	}

	public void setPurgeQueue(String content) {
		setProperty(PURGE_QUEUE, content);
	}

	public void setPurgeQueue(Boolean purgeQueue) {
		setProperty(PURGE_QUEUE, purgeQueue.toString());
	}

	public boolean purgeQueue() {
		return Boolean.parseBoolean(getPurgeQueue());
	}

	/**
	 * @return the whether or not to auto ack
	 */
	public String getAutoAck() {
		return getPropertyAsString(AUTO_ACK);
	}

	public void setAutoAck(String content) {
		setProperty(AUTO_ACK, content);
	}

	public void setAutoAck(Boolean autoAck) {
		setProperty(AUTO_ACK, autoAck.toString());
	}

	public boolean autoAck() {
		return getPropertyAsBoolean(AUTO_ACK);
	}

	protected int getReceiveTimeoutAsInt() {
		if (getPropertyAsInt(RECEIVE_TIMEOUT) < 1) {
			return DEFAULT_TIMEOUT;
		}
		return getPropertyAsInt(RECEIVE_TIMEOUT);
	}

	public String getReceiveTimeout() {
		return getPropertyAsString(RECEIVE_TIMEOUT, DEFAULT_TIMEOUT_STRING);
	}

	public void setReceiveTimeout(String s) {
		setProperty(RECEIVE_TIMEOUT, s);
	}

	public String getPrefetchCount() {
		return getPropertyAsString(PREFETCH_COUNT, DEFAULT_PREFETCH_COUNT_STRING);
	}

	public void setPrefetchCount(String prefetchCount) {
		setProperty(PREFETCH_COUNT, prefetchCount);
	}

	public int getPrefetchCountAsInt() {
		return getPropertyAsInt(PREFETCH_COUNT);
	}

	public Boolean getUseTx() {
		return getPropertyAsBoolean(USE_TX, DEFAULT_USE_TX);
	}

	public void setUseTx(Boolean tx) {
		setProperty(USE_TX, tx);
	}

	/**
	 * set whether the sampler should read the response or not
	 *
	 * @param read whether the sampler should read the response or not
	 */
	public void setReadResponse(Boolean read) {
		setProperty(READ_RESPONSE, read);
	}

	/**
	 * return whether the sampler should read the response
	 *
	 * @return whether the sampler should read the response
	 */
	public String getReadResponse() {
		return getPropertyAsString(READ_RESPONSE);
	}

	/**
	 * return whether the sampler should read the response as a boolean value
	 *
	 * @return whether the sampler should read the response as a boolean value
	 */
	public boolean getReadResponseAsBoolean() {
		return getPropertyAsBoolean(READ_RESPONSE);
	}

	@Override
	public boolean interrupt() {
		testEnded();
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void testEnded() {

		if (purgeQueue()) {
			log.info("Purging queue {}", getQueue());
			try {
				channel.queuePurge(getQueue());
			} catch (IOException e) {
				log.error("Failed to purge queue " + getQueue(), e);
			}
		}
	}

	@Override
	public void cleanup() {
		try {
			if (consumerTag != null) {
				channel.basicCancel(consumerTag);
			}
		} catch (IOException e) {
			log.error("Couldn't safely cancel the sample " + consumerTag, e);
		}
		super.cleanup();
	}

	@Override
	protected boolean initChannel() throws IOException, NoSuchAlgorithmException, KeyManagementException {
		boolean ret = super.initChannel();
		channel.basicQos(getPrefetchCountAsInt());
		if (getUseTx()) {
			channel.txSelect();
		}
		return ret;
	}

	private String formatHeaders(Envelope envelope, AMQP.BasicProperties properties) {
		Map<String, Object> headers = properties.getHeaders();
		StringBuilder sb = new StringBuilder();
		sb.append(TIMESTAMP_PARAMETER).append(": ")
				.append(properties.getTimestamp() != null ? properties.getTimestamp().getTime() : "").append("\n");
		sb.append(EXCHANGE_PARAMETER).append(": ").append(envelope.getExchange()).append("\n");
		sb.append(ROUTING_KEY_PARAMETER).append(": ").append(envelope.getRoutingKey()).append("\n");
		sb.append(DELIVERY_TAG_PARAMETER).append(": ").append(envelope.getDeliveryTag()).append("\n");
		for (Map.Entry<String, Object> entry : headers.entrySet()) {
			sb.append(entry.getKey()).append(": ").append(headers.get(entry.getKey())).append("\n");
		}
		return sb.toString();
	}

	@Override
	public void testStarted() {
		log.info("testStarted");

	}

	@Override
	public void testStarted(String host) {
		log.info("testStarted  {}", host);

	}

	@Override
	public void testEnded(String host) {
		log.info("testEnded  {}", host);

	}
}
