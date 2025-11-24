import os
import subprocess
import shutil

# ==========================
# Cấu hình chung
# ==========================
MYSQL_HOST = os.getenv("MYSQL_HOST", "db")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
AGG_DB = "dbAgg"

# File paths trong container, mapped ra host thông qua volume
BACKUP_DIR = "/storage/backup"
DUMP_FILE = os.path.join(BACKUP_DIR, "dump_file.sql")
BACKUP_FILE = os.path.join(BACKUP_DIR, "backup_file.sql")

# Tạo folder backup nếu chưa tồn tại
os.makedirs(BACKUP_DIR, exist_ok=True)

# ==========================
# 1) Dump database dbAgg
# ==========================
def dump_database():
    print("🔄 Đang dump database dbAgg...")

    cmd = [
        "mysqldump",
        f"-h{MYSQL_HOST}",
        f"-u{MYSQL_USER}",
        f"-p{MYSQL_PASSWORD}",
        AGG_DB,
        "--column-statistics=0",  # tránh cảnh báo nếu MySQL >=8
        "--skip-triggers"
    ]

    try:
        with open(DUMP_FILE, "w") as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE)

        if result.returncode == 0:
            print(f"✔ Dump thành công! File: {DUMP_FILE}")
            return True
        else:
            print("❌ Dump thất bại!")
            print(result.stderr.decode())
            return False

    except Exception as e:
        print(f"❌ Lỗi dump: {e}")
        return False


# ==========================
# 2) Backup mới nếu dump thành công
# ==========================
def save_as_backup():
    shutil.copy(DUMP_FILE, BACKUP_FILE)
    print(f"✔ Đã cập nhật backup mới tại: {BACKUP_FILE}")


# ==========================
# 3) Nếu dump thất bại → dùng backup
# ==========================
def restore_backup():
    if not os.path.exists(BACKUP_FILE):
        print("❌ Không có backup_file.sql để phục hồi!")
        return False

    shutil.copy(BACKUP_FILE, DUMP_FILE)
    print("⚠ Dump thất bại — đã khôi phục lại file backup.")
    print(f"➡ File được dùng: {DUMP_FILE}")
    return True


# ==========================
# 4) Main logic
# ==========================
if __name__ == "__main__":
    print("=== SAFE DUMP AGGREGATE START ===")

    ok = dump_database()

    if ok:
        save_as_backup()
        print("🎉 Luôn có file cuối cùng: backup mới.")
    else:
        restore_backup()
        print("🎉 Luôn có file cuối cùng: backup cũ (dump lỗi).")

    print("=== SAFE DUMP AGGREGATE END ===")
