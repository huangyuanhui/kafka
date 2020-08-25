package com.hyh.kafka.controller;

import com.alibaba.fastjson.JSON;
import com.hyh.kafka.pojo.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Date;

/** 消息发送方
 * @author jingshiyu
 * @date 2019/7/31 14:04:21
 * @desc
 */
@RestController
@RequestMapping("/kafka")
public class KafkaSender {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping(value = "/send", method = RequestMethod.GET)
    public void sendKafka(HttpServletRequest request, HttpServletResponse response) {
        try {
            String url = request.getParameter("message");
            logger.info("kafka的消息={}", url);

            //消息实体
            Message message = new Message();
            message.setId(1L);
            message.setMsg(url);
            message.setSendTime(new Date());

            // 发送消息
            kafkaTemplate.send("test", "key", JSON.toJSONString(message));

            logger.info("发送kafka成功.");
        } catch (Exception e) {
            logger.error("发送kafka失败", e);
        }
    }

}