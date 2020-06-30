package edp.wormhole.util.email;

import java.io.File;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Properties;

public class Email {
	// 发送邮件的服务器的IP
	private String smtpHost;

	// 发送邮件的服务器的端口
	private String smtpPort;

	// 登陆邮件发送服务器的用户名
	private String smtpUsername;

	// 登陆邮件发送服务器密码
	private String smtpPassword;

	// 邮件发送者的地址
	private String fromAddress;

	// 邮件发送者名称
	private String fromNickName;

	// 邮件接收者的地址
	private String toAddress;

	// 是否需要身份验证
	private boolean validate = false;

	// 邮件主题
	private String subject;

	// 邮件的文本内容
	private String content;

	// 邮件附件的文件名
	private List<File> attachments;

	public Email(String smtpHost, String smtpPort, String smtpUsername, String smtpPassword, String fromAddress) {
		this.smtpHost = smtpHost;
		this.smtpPort = smtpPort;
		this.smtpUsername = smtpUsername;
		this.smtpPassword = smtpPassword;
		this.fromAddress = fromAddress;
	}

	public Properties getProperties() throws GeneralSecurityException {
		Properties props = new Properties();
		props.put("mail.smtp.host", this.smtpHost);
		props.put("mail.smtp.port", this.smtpPort);
		props.put("mail.smtp.auth", validate ? "true" : "false");
		return props;
	}

	public String getSmtpHost() {
		return smtpHost;
	}

	public void setSmtpHost(String smtpHost) {
		this.smtpHost = smtpHost;
	}

	public String getSmtpPort() {
		return smtpPort;
	}

	public void setSmtpPort(String smtpPort) {
		this.smtpPort = smtpPort;
	}

	public String getSmtpUsername() {
		return smtpUsername;
	}

	public void setSmtpUsername(String smtpUsername) {
		this.smtpUsername = smtpUsername;
	}

	public String getSmtpPassword() {
		return smtpPassword;
	}

	public void setSmtpPassword(String smtpPassword) {
		this.smtpPassword = smtpPassword;
	}

	public String getFromAddress() {
		return fromAddress;
	}

	public void setFromAddress(String fromAddress) {
		this.fromAddress = fromAddress;
	}

	public String getFromNickName() {
		return fromNickName;
	}

	public void setFromNickName(String fromNickName) {
		this.fromNickName = fromNickName;
	}

	public String getToAddress() {
		return toAddress;
	}

	public void setToAddress(String toAddress) {
		this.toAddress = toAddress;
	}

	public boolean isValidate() {
		return validate;
	}

	public void setValidate(boolean validate) {
		this.validate = validate;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public List<File> getAttachments() {
		return attachments;
	}

	public void setAttachments(List<File> attachments) {
		this.attachments = attachments;
	}

}