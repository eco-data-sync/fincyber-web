import os
import sys
from pymongo import MongoClient, UpdateMany

print("--- KHỞI ĐỘNG ĐỘNG CƠ CHẤM ĐIỂM DỰA TRÊN MẬT ĐỘ PHƠI NHIỄM (V5.0) ---")

# 1. BẢO MẬT: Kéo chìa khóa từ GitHub Secrets
MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    print("❌ LỖI BẢO MẬT: Không tìm thấy MONGO_URI trong Secrets.")
    sys.exit(1)

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client['Elite_Eyes']
    radar_col = db['Global_Enterprise_Radar']
    print("✅ KẾT NỐI DB THÀNH CÔNG. Bắt đầu phân tích cấu trúc...")
except Exception as e:
    print(f"❌ LỖI KẾT NỐI DB: {e}")
    sys.exit(1)

def calculate_density_score(ip_count):
    """
    Thuật toán định lượng rủi ro theo số lượng IP bị lộ của một Doanh nghiệp.
    Mật độ càng cao -> Điểm càng sát ngưỡng 100.
    """
    if ip_count == 1:
        return 35  # Lộ 1 IP: Rủi ro mức độ chú ý
    elif ip_count <= 10:
        # Lộ từ 2 đến 10 IP: Điểm tăng dần từ 46 đến 70
        return 40 + (ip_count * 3)  
    elif ip_count <= 50:
        # Lộ từ 11 đến 50 IP: Nguy hiểm, điểm từ 74 đến 90
        return 70 + int(ip_count * 0.4) 
    else:
        # Lộ trên 50 IP: Thảm họa quản trị hệ thống, chắc chắn trên 90 điểm
        score = 90 + int(ip_count * 0.05)
        return min(score, 99)  # Khóa trần ở 99 để tạo cảm giác thực tế

def evaluate_dynamic_risk():
    print("[*] Đang kích hoạt Aggregation Pipeline để đếm số lượng IP của từng Doanh nghiệp...")
    
    # Gộp nhóm dữ liệu: Đếm xem mỗi corporate_owner có tổng cộng bao nhiêu IP trong Radar
    pipeline = [
        {"$group": {"_id": "$corporate_owner", "ip_count": {"$sum": 1}}}
    ]
    
    company_stats = list(radar_col.aggregate(pipeline))
    total_companies = len(company_stats)
    
    if total_companies == 0:
        print("[-] Không có dữ liệu để chấm điểm.")
        return

    print(f"[*] Tìm thấy {total_companies} Tập đoàn/Tổ chức. Đang tiến hành áp đặt Điểm Rủi Ro...")
    
    operations = []
    
    for company in company_stats:
        owner_name = company['_id']
        count = company['ip_count']
        
        # Tính điểm rủi ro cho công ty này dựa trên tổng số IP lộ
        new_score = calculate_density_score(count)
        
        # Lệnh UpdateMany: Cập nhật cùng một mức điểm cao cho TẤT CẢ các IP thuộc công ty này
        operations.append(
            UpdateMany(
                {"corporate_owner": owner_name},
                {"$set": {"risk_score": new_score}}
            )
        )
        
        # Ghi hàng loạt mỗi 500 doanh nghiệp một lần để tối ưu RAM của GitHub
        if len(operations) >= 500:
            radar_col.bulk_write(operations, ordered=False)
            operations = []

    # Bắn nốt băng đạn dữ liệu cuối cùng vào MongoDB
    if operations:
        radar_col.bulk_write(operations, ordered=False)

    print("✅ CHIẾN DỊCH HOÀN TẤT! Toàn bộ 2.7 triệu IP đã được chấm điểm tự động dựa trên quy mô phơi nhiễm.")

if __name__ == "__main__":
    evaluate_dynamic_risk()
