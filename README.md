# audio-shrink

Chuyển toàn bộ MP3 chương truyện trên R2 sang 48 kbps mono 24 kHz (giữ nguyên đường dẫn, website không phải sửa gì).

Chạy ở tab **Actions → Shrink audio on R2 → Run workflow**. Mặc định là **chạy thử** (chuyển và kiểm tra nhưng không ghi gì lên R2).
Bỏ tick "dry_run" để chạy thật. Dừng giữa chừng lúc nào cũng được; chạy lại sẽ bỏ qua file đã chuyển.

Cần 3 secrets (Settings → Secrets and variables → Actions): `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`.
