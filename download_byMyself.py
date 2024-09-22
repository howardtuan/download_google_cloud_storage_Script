from google.cloud import storage
import os
import concurrent.futures
import time
from tqdm import tqdm

def download_blob(bucket, blob, destination_folder, pbar):
    """Downloads a blob from the bucket."""
    try:
        destination_path = os.path.join(destination_folder, blob.name)
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        blob.download_to_filename(destination_path)
        pbar.update(1)
    except Exception as e:
        print(f"下載檔案 {blob.name} 時發生錯誤：{e}")

def download_bucket(bucket_name, destination_folder):
    """Downloads all files from the specified GCS bucket."""
    try:
        storage_client = storage.Client.create_anonymous_client()
        bucket = storage_client.bucket(bucket_name)

        # 獲取所有 blob
        blobs = list(bucket.list_blobs())

        if not blobs:
            print("錯誤：存儲桶中沒有文件。")
            return

        # 確保資料夾存在
        if not os.path.exists(destination_folder):
            os.makedirs(destination_folder)

        total_blobs = len(blobs)
        print(f"總共找到 {total_blobs} 個文件待下載。")

        # 使用進度條和多線程進行下載
        with tqdm(total=total_blobs, unit='file') as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for blob in blobs:
                    if not blob.name.endswith('/'):  # 只下載檔案，排除資料夾
                        future = executor.submit(download_blob, bucket, blob, destination_folder, pbar)
                        futures.append(future)

                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"下載過程中發生錯誤：{e}")

    except Exception as e:
        print(f"無法存取 bucket '{bucket_name}'：{e}")

# 設置您的 bucket 名稱
bucket_name = "finngen-public-data-r11"

# 設置外接硬碟的路徑
usb_path = "E:\\finngen-public-data-r11"  # 將 E: 替換為您的外接硬碟盤符
destination_folder = usb_path

# 檢查外接硬碟是否已連接
if not os.path.exists(usb_path):
    print(f"錯誤：找不到外接硬碟 '{usb_path}'。請確保外接硬碟已正確連接。")
    exit(1)

start_time = time.time()
download_bucket(bucket_name, destination_folder)
end_time = time.time()

print(f"\n下載完成。總耗時: {end_time - start_time:.2f} 秒")
