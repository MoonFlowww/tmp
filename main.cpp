#include <iostream>
#include <filesystem>
#include <curl/curl.h>
#include <fstream>

#include "databento/dbn_store.hpp"
#include "arrow/io/file.h"
#include "arrow/table.h"
#include <parquet/arrow/writer.h> 

size_t write_data(void* ptr, size_t size, size_t nmemb, void* stream) {
    std::ofstream* ofs = static_cast<std::ofstream*>(stream);
    size_t written = 0;
    if (ofs->is_open()) {
        ofs->write(static_cast<const char*>(ptr), size * nmemb);
        written = size * nmemb;
    }
    return written;
}

bool download_ftp(const std::string& ftp_url, const std::string& local_path,
                  const std::string& user = "", const std::string& password = "") {
    CURL* curl = curl_easy_init();
    if (!curl) {
        std::cerr << "Failed to init curl\n";
        return false;
    }

    std::ofstream ofs(local_path, std::ios::binary);
    if (!ofs) {
        std::cerr << "Failed to open file " << local_path << "\n";
        curl_easy_cleanup(curl);
        return false;
    }

    curl_easy_setopt(curl, CURLOPT_URL, ftp_url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ofs);

    if (!user.empty()) {
        std::string userpwd = user + ":" + password;
        curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
    }

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    ofs.close();

    if (res != CURLE_OK) {
        std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << "\n";
        return false;
    }

    return true;
}

int main() {
    std::string ftp_host = "ftp.databento.com";
    std::string ftp_dir = "/M8PMTAQS";
    std::string filename = "GLBX-20250725-7XQKQYDCLN";
    std::string local_dir = "/ssd/databases/parquet/data/futures";
    std::string local_path = local_dir + "/" + filename;
    std::string parquet_output = "/ssd/databases/parquet/data/futures/MNQ_L1.parquet";

    std::string ftp_url = "ftp://" + ftp_host + ftp_dir + "/" + filename;

    std::filesystem::create_directories(local_dir);

    std::cout << "Downloading " << ftp_url << "...\n";
    if (!download_ftp(ftp_url, local_path)) {
        std::cerr << "FTP download failed\n";
        return 1;
    }
    std::cout << "Downloaded to " << local_path << "\n";

    try {
        auto store = databento::dbn::DBNStore::from_file(local_path);

        auto table = store.to_arrow_table();

        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        ARROW_THROW_NOT_OK(arrow::io::FileOutputStream::Open(parquet_output, &outfile));

        std::unique_ptr<parquet::arrow::FileWriter> parquet_writer;
        ARROW_THROW_NOT_OK(parquet::arrow::FileWriter::Open(*table->schema(), arrow::default_memory_pool(), outfile, &parquet_writer));

        ARROW_THROW_NOT_OK(parquet_writer->WriteTable(*table, table->num_rows()));

        ARROW_THROW_NOT_OK(parquet_writer->Close());

        std::cout << "Converted and saved Parquet to " << parquet_output << "\n";
    } catch (const std::exception& e) {
        std::cerr << "Conversion failed: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
