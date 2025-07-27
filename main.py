import os
from ftplib import FTP
from databento import DBNStore

def download_from_ftp(ftp_host, ftp_path, filename, local_dir, user='', passwd=''):
    local_path = os.path.join(local_dir, filename)
    os.makedirs(local_dir, exist_ok=True)
    
    ftp = FTP(ftp_host)
    ftp.login(user=user, passwd=passwd)
    ftp.cwd(ftp_path)
    
    with open(local_path, 'wb') as f:
        print(f"Downloading {filename} from FTP...")
        ftp.retrbinary(f"RETR {filename}", f.write)
    
    ftp.quit()
    print(f"Downloaded to {local_path}")
    return local_path

def convert_dbn_to_parquet(dbn_path, parquet_path):
    print(f"Converting {dbn_path} to Parquet...")
    store = DBNStore.from_file(dbn_path)
    store.to_parquet(parquet_path)
    print(f"Saved Parquet to {parquet_path}")

def main():
    ftp_host = "ftp.databento.com"
    ftp_dir = "/M8PMTAQS"  # example directory, adjust accordingly
    filename = "GLBX-20250725-7XQKQYDCLN"  # your exact file name on FTP
    local_dir = "./downloads"
    parquet_output = "./outputs/GLBX-20250725-7XQKQYDCLN.parquet"
    
    # If needed, provide FTP credentials; empty strings for anonymous
    ftp_user = ""
    ftp_pass = ""
    
    # Step 1: Download
    dbn_file = download_from_ftp(ftp_host, ftp_dir, filename, local_dir, ftp_user, ftp_pass)
    
    # Step 2: Convert
    os.makedirs(os.path.dirname(parquet_output), exist_ok=True)
    convert_dbn_to_parquet(dbn_file, parquet_output)

if __name__ == "__main__":
    main()
