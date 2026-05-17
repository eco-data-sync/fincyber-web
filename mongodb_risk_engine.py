import os
import sys
from pymongo import MongoClient, UpdateMany

print("--- KHỞI ĐỘNG ĐỘNG CƠ CHẤM ĐIỂM TURBO (V5.1) ---")

# 1. Kéo chìa khóa bảo mật
MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    print("❌ LỖI BẢO MẬT: Không tìm thấy MONGO_URI.")
    sys.exit(1)

try:
    # Tăng thời gian chờ (Timeout) lên để tránh bị rớt mạng khi làm việc với 2.7 triệu Data
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000, socketTimeoutMS=60000)
    db = client['Elite_Eyes']
    radar_col = db['Global_Enterprise_Radar']
    print("✅ KẾT NỐI DB THÀNH CÔNG.")
except Exception as e:
    print(f"❌ LỖI KẾT NỐI DB: {e}")
    sys.exit(1)

def calculate_density_score(ip_count):
    if ip_count == 1: return 35
    elif ip_count <= 10: return 40 + (ip_count * 3)
    elif ip_count <= 50: return 70 + int(ip_count * 0.4)
    else: return min(90 + int(ip_count * 0.05), 99)

def evaluate_dynamic_risk():
    # ---------------------------------------------------------
    # ĐÒN BẨY QUAN TRỌNG NHẤT: TẠO INDEX TRƯỚC KHI CHẠY
    # ---------------------------------------------------------
    print("[*] Đang xây dựng Mục Lục (Index) cho 2.7 triệu bản ghi... (Có thể mất 1-3 phút)")
    try:
        radar_col.create_index("corporate_owner")
        print("[+] Mục Lục đã sẵn sàng! Tốc độ quét sẽ tăng gấp 10.000 lần.")
    except Exception as e:
        print(f"[-] Lỗi tạo Index (có thể bỏ qua nếu đã có): {e}")

    print("[*] Kích hoạt Aggregation Pipeline đếm IP...")
    
    pipeline = [
        {"$group": {"_id": "$corporate_owner", "ip_count": {"$sum": 1}}}
    ]
    
    company_stats = list(radar_col.aggregate(pipeline))
    total_companies = len(company_stats)
    
    if total_companies == 0:
        print("[-] Không có dữ liệu.")
        return

    print(f"[*] Tìm thấy {total_companies} Tập đoàn. Bắt đầu xả đạn cập nhật điểm số...")
    
    operations = []
    processed_count = 0
    
    for company in company_stats:
        owner_name = company['_id']
        count = company['ip_count']
        
        # Bỏ qua các bản ghi không có tên công ty hợp lệ
        if not owner_name:
            continue
            
        new_score = calculate_density_score(count)
        
        operations.append(
            UpdateMany(
                {"corporate_owner": owner_name},
                {"$set": {"risk_score": new_score}}
            )
        )
        processed_count += 1
        
        # Gom đủ 1000 tập đoàn thì bắn 1 lần để chống nghẽn mạng
        if len(operations) >= 1000:
            radar_col.bulk_write(operations, ordered=False)
            print(f"> Đã chấm điểm xong: {processed_count}/{total_companies} Tập đoàn...")
            operations = []

    # Xả đạn phần còn lại
    if operations:
        radar_col.bulk_write(operations, ordered=False)
        print(f"> Đã chấm điểm xong: {processed_count}/{total_companies} Tập đoàn...")

    print("✅ CHIẾN DỊCH HOÀN TẤT MƯỢT MÀ!")

if __name__ == "__main__":
    evaluate_dynamic_risk()
