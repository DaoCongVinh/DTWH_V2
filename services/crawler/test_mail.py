# test_email.py
import notification
import config

print(f"Testing email sending...")
print(f"Sender: {config.MAIL_SENDER}")
print(f"Receiver: {config.MAIL_RECEIVER}")

try:
    notification.send_notification(
        status="TEST", 
        message="Đây là email kiểm tra kết nối.", 
        detail_info="Nếu bạn nhận được mail này, cấu hình SMTP đã đúng."
    )
    print("✅ Đã gửi lệnh gửi mail. Hãy kiểm tra inbox/spam.")
except Exception as e:
    print("❌ Lỗi gửi mail:")
    print(e)