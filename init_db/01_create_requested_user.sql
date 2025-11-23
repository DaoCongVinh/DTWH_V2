-- Create requested application user and grant database permissions
CREATE USER IF NOT EXISTS 'user'@'%' IDENTIFIED BY 'dwhtiktok';
GRANT ALL PRIVILEGES ON dwh_tiktok.* TO 'user'@'%';
GRANT ALL PRIVILEGES ON dbStaging.* TO 'user'@'%';
GRANT ALL PRIVILEGES ON metadata_tiktok.* TO 'user'@'%';
GRANT ALL PRIVILEGES ON staging_tiktok.* TO 'user'@'%';
GRANT ALL PRIVILEGES ON warehouse_tiktok.* TO 'user'@'%';
FLUSH PRIVILEGES;


