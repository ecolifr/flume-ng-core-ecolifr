package org.apache.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 重写flume的LoggerSink类
 *
 * @author Xie ZuoZhi
 * @date 2018/10/25 9:26
 * @description 在flume配置文件中配置agent1.sinks.avroSink.type=org.apache.flume.sink.LoggerOverrideSink可使用
 */
public class LoggerOverrideSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(LoggerOverrideSink.class);

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        try {
            transaction.begin();
            event = channel.take();
            if (event != null) {
                logger.info(new String(event.getBody()));
            } else {
                result = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Exception ex) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to log event: " + event, ex);
        } finally {
            transaction.close();
        }
        return result;
    }

    @Override
    public void configure(Context context) {

    }
}
