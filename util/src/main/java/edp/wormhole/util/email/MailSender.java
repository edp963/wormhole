package edp.wormhole.util.email;

import org.apache.log4j.Logger;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.Properties;

public class MailSender {
	private static final Logger LOGGER = Logger.getLogger(MailSender.class);

	public static void sendTextMail(Email mail) throws GeneralSecurityException {
		// 判断是否需要身份认证
		MailAuthenticator authenticator = null;

		Properties pro = mail.getProperties();

		if (mail.isValidate()) {
			// 如果需要身份认证，则创建一个密码验证器
			authenticator = new MailAuthenticator(mail.getSmtpUsername(), mail.getSmtpPassword());
		}

		// 根据邮件会话属性和密码验证器构造一个发送邮件的session
		Session sendMailSession = Session.getDefaultInstance(pro, authenticator);

		try {
			// 根据session创建一个邮件消息
			Message mailMessage = new MimeMessage(sendMailSession);
			// 创建邮件发送者地址
			Address from = new InternetAddress(mail.getFromAddress());
			// 设置邮件消息的发送者
			mailMessage.setFrom(from);
			// 创建邮件的接收者地址，并设置到邮件消息中
			mailMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mail.getToAddress()));
			// 设置邮件消息的主题
			mailMessage.setSubject(mail.getSubject());
			// 设置邮件消息发送的时间
			mailMessage.setSentDate(new Date());

			if (mail.getAttachments() == null) {// 无附件
				mailMessage.setText(mail.getContent());
			} else {
				// MiniMultipart类是一个容器类，包含MimeBodyPart类型的对象
				Multipart mainPart = new MimeMultipart();
				MimeBodyPart mbp = new MimeBodyPart();

				// 设置HTML内容
				mbp.setText(mail.getContent());
				mainPart.addBodyPart(mbp);

				// 设置信件的附件(用本地上的文件作为附件)
				FileDataSource fds = null;
				DataHandler dh = null;
				for (File file : mail.getAttachments()) {
					mbp = new MimeBodyPart();
					fds = new FileDataSource(file);
					dh = new DataHandler(fds);
					mbp.setFileName(MimeUtility.encodeText(file.getName()));
					mbp.setDataHandler(dh);
					mainPart.addBodyPart(mbp);
				}

				// 将MiniMultipart对象设置为邮件内容
				mailMessage.setContent(mainPart);
			}

			mailMessage.saveChanges();

			// 发送邮件
			Transport.send(mailMessage);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public static void sendHtmlMail(Email mail) throws GeneralSecurityException, UnsupportedEncodingException {
		// 判断是否需要身份认证
		MailAuthenticator authenticator = null;

		Properties props = mail.getProperties();

		// 如果需要身份认证，则创建一个密码验证器
		if (mail.isValidate()) {
			authenticator = new MailAuthenticator(mail.getSmtpUsername(), mail.getSmtpPassword());
		}

		// 根据邮件会话属性和密码验证器构造一个发送邮件的session
		Session sendMailSession = Session.getDefaultInstance(props, authenticator);
		sendMailSession.setDebug(true);
		try {
			// 根据session创建一个邮件消息
			Message mailMessage = new MimeMessage(sendMailSession);
			// 创建邮件发送者地址
			Address from = new InternetAddress(mail.getFromAddress(),
					mail.getFromNickName() == null ? "" : mail.getFromNickName());
			// 设置邮件消息的发送者
			mailMessage.setFrom(from);
			// 创建邮件的接收者地址，并设置到邮件消息中,可以设置多个收件人，逗号隔开
			// Message.RecipientType.TO属性表示接收者的类型为TO,CC表示抄送,BCC暗送
			mailMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mail.getToAddress()));
			// 设置邮件消息的主题
			mailMessage.setSubject(mail.getSubject());
			// 设置邮件消息发送的时间
			mailMessage.setSentDate(new Date());

			if (mail.getAttachments() == null) {
				mailMessage.setContent(mail.getContent(), "text/html; charset=utf-8");
			} else {
				// MiniMultipart类是一个容器类，包含MimeBodyPart类型的对象
				Multipart mainPart = new MimeMultipart();
				// 创建一个包含HTML内容的MimeBodyPart
				MimeBodyPart html = new MimeBodyPart();
				// 设置HTML内容
				html.setContent(mail.getContent(), "text/html; charset=utf-8");
				mainPart.addBodyPart(html);

				// 设置信件的附件(用本地上的文件作为附件)
				FileDataSource fds = null;
				DataHandler dh = null;
				for (File file : mail.getAttachments()) {
					html = new MimeBodyPart();
					fds = new FileDataSource(file);
					dh = new DataHandler(fds);
					html.setFileName(MimeUtility.encodeText(file.getName()));
					html.setDataHandler(dh);
					mainPart.addBodyPart(html);
				}

				// 将MiniMultipart对象设置为邮件内容
				mailMessage.setContent(mainPart);
			}

			mailMessage.saveChanges();

			// 发送邮件
			Transport.send(mailMessage);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

}
