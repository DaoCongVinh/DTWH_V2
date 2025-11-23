# notification.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime  # <--- 1. Thêm dòng này
import config
from logging_setup import logger

def send_notification(status, message, detail_info=None):
    """
    Gửi email thông báo trạng thái Job (Success/Failed).
    """
    if not config.MAIL_SENDER or not config.MAIL_PASSWORD:
        logger.warning("Email config missing. Skipping email notification.")
        return

    # 2. Lấy thời gian hiện tại và format theo cấu hình
    try:
        current_time = datetime.now().strftime(config.DATE_FORMAT)
    except:
        # Fallback nếu config sai format
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Xác định màu sắc và tiêu đề
    color = "green" if status == "SUCCESS" else "red"
    subject = f"[{status}] Crawler Report - {config.DEVICE_ID}"

    # Soạn nội dung HTML (Sử dụng biến current_time vừa tạo)
    body = f"""
    <h3 style="color: {color};">Báo cáo trạng thái: {status}</h3>
    <ul>
        <li><b>Device ID:</b> {config.DEVICE_ID}</li>
        <li><b>Time:</b> {current_time}</li> 
        <li><b>Message:</b> {message}</li>
    </ul>
    <hr>
    <p><b>Chi tiết:</b></p>
    <pre>{detail_info if detail_info else 'No details provided.'}</pre>
    """

    msg = MIMEMultipart()
    msg['From'] = config.MAIL_SENDER
    msg['To'] = config.MAIL_RECEIVER
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    # Gửi mail
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(config.MAIL_SENDER, config.MAIL_PASSWORD)
        text = msg.as_string()
        server.sendmail(config.MAIL_SENDER, config.MAIL_RECEIVER, text)
        server.quit()
        logger.info(f"Email notification ({status}) sent successfully.")
    except Exception as e:
        logger.error("Failed to send email notification: %s", e)