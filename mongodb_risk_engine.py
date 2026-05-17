import os
import sys
from pymongo import MongoClient, UpdateOne

print("--- KHỞI ĐỘNG SIÊU ĐỘNG CƠ CHẤM ĐIỂM RỦI RO (RISK SCORING ENGINE V4.0) ---")

# 1. BẢO MẬT TUYỆT ĐỐI: Kéo chìa khóa từ GitHub Secrets, không để lộ trên code
MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    print("❌ LỖI BẢO MẬT: Không tìm thấy MONGO_URI trong Secrets.")
    sys.exit(1)

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client['Elite_Eyes']
    radar_col = db['Global_Enterprise_Radar']
    print("✅ KẾT NỐI MONGODB THÀNH CÔNG. Đang chuẩn bị rà quét hệ thống...")
except Exception as e:
    print(f"❌ LỖI KẾT NỐI DB: {e}")
    sys.exit(1)

def calculate_risk_score(ports):
    """Thuật toán định lượng rủi ro"""
    score = 10  # Điểm sàn mặc định
    
    if not isinstance(ports, list):
        return score

    risk_weights = {
        21: 20,   # FTP
        22: 30,   # SSH
        23: 35,   # Telnet
        80: 10,   # HTTP
        443: 5,   # HTTPS
        3389: 40, # RDP 
        3306: 40, # MySQL Database
        27017: 50 # MongoDB Database 
    }

    for port in ports:
        if port in risk_weights:
            score += risk_weights[port]
        else:
            score += 2

    return min(score, 99)

def evaluate_all_records():
    # CHUẨN MỰC TỰ ĐỘNG KHÔI PHỤC: Chỉ tìm những IP CHƯA CÓ ĐIỂM
    # Nếu GitHub bị ngắt giữa chừng, lần chạy sau nó sẽ tự động chạy tiếp từ chỗ đang dở dang.
    query = {"risk_score": {"$exists": False}}
    total_targets = radar_col.count_documents(query)
    
    if total_targets == 0:
        print("[TRẠNG THÁI] Toàn bộ 2.7 triệu IP đã được chấm điểm xong. Động cơ ngủ đông.")
        return

    print(f"[*] TÌM THẤY {total_targets} THỰC THỂ CẦN CHẤM ĐIỂM. KHAI HỎA BULK WRITE...")
    
    cursor = radar_col.find(query)
    operations = []
    processed_count = 0

    for doc in cursor:
        open_ports = doc.get('open_ports', [])
        final_score = calculate_risk_score(open_ports)
        
        # Đưa vào băng chuyền chờ cập nhật hàng loạt
        operations.append(
            UpdateOne({"_id": doc['_id']}, {"$set": {"risk_score": final_score}})
        )
        processed_count += 1

        # Cứ gom đủ 5.000 IP thì xả đạn 1 lần vào MongoDB
        if len(operations) >= 5000:
            radar_col.bulk_write(operations, ordered=False)
            print(f"> Đã chấm điểm và đồng bộ: {processed_count} / {total_targets} IPs...")
            operations = [] # Làm sạch băng chuyền

    # Xả đạn những IP còn sót lại cuối cùng
    if operations:
        radar_col.bulk_write(operations, ordered=False)
        print(f"> Đã chấm điểm và đồng bộ: {processed_count} / {total_targets} IPs...")

    print("✅ CHIẾN DỊCH HOÀN TẤT! Toàn bộ cơ sở dữ liệu đã được nạp Điểm Rủi Ro.")

if __name__ == "__main__":
    evaluate_all_records()
