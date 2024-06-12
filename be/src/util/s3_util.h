// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <bvar/bvar.h>
#include <fmt/format.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "gutil/hash/hash.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace bvar {
template <typename T>
class Adder;
}

namespace doris {

namespace s3_bvar {
extern bvar::LatencyRecorder s3_get_latency;
extern bvar::LatencyRecorder s3_put_latency;
extern bvar::LatencyRecorder s3_delete_latency;
extern bvar::LatencyRecorder s3_head_latency;
extern bvar::LatencyRecorder s3_multi_part_upload_latency;
extern bvar::LatencyRecorder s3_list_latency;
extern bvar::LatencyRecorder s3_list_object_versions_latency;
extern bvar::LatencyRecorder s3_get_bucket_version_latency;
extern bvar::LatencyRecorder s3_copy_object_latency;
}; // namespace s3_bvar

class S3URI;

const static std::string S3_AK = "AWS_ACCESS_KEY";
const static std::string S3_SK = "AWS_SECRET_KEY";
const static std::string S3_ENDPOINT = "AWS_ENDPOINT";
const static std::string S3_REGION = "AWS_REGION";
const static std::string S3_TOKEN = "AWS_TOKEN";
const static std::string S3_MAX_CONN_SIZE = "AWS_MAX_CONN_SIZE";
const static std::string S3_REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
const static std::string S3_CONN_TIMEOUT_MS = "AWS_CONNECTION_TIMEOUT_MS";

struct S3ClientConf {
    std::string endpoint;
    std::string region;
    std::string ak;
    std::string sk;
    std::string token;
    // For azure we'd better support the bucket at the first time init azure blob container client
    std::string bucket;
    io::ObjStorageType provider = io::ObjStorageType::AWS;
    int max_connections = -1;
    int request_timeout_ms = -1;
    int connect_timeout_ms = -1;
    bool use_virtual_addressing = true;

    std::string to_string() const {
        return fmt::format(
                "(ak={}, token={}, endpoint={}, region={}, bucket={}, max_connections={}, "
                "request_timeout_ms={}, connect_timeout_ms={}, use_virtual_addressing={}",
                ak, token, endpoint, region, bucket, max_connections, request_timeout_ms,
                connect_timeout_ms, use_virtual_addressing);
    }
};

struct S3Conf {
    std::string bucket;
    std::string prefix;
    S3ClientConf client_conf;

    bool sse_enabled = false;
    static S3Conf get_s3_conf(const cloud::ObjectStoreInfoPB&);
    static S3Conf get_s3_conf(const TS3StorageParam&);

    std::string to_string() const {
        return fmt::format("(bucket={}, prefix={}, client_conf={}, sse_enabled={})", bucket, prefix,
                           client_conf.to_string(), sse_enabled);
    }
};

class S3ClientFactory {
public:
    ~S3ClientFactory();

    static S3ClientFactory& instance();

    std::shared_ptr<io::ObjStorageClient> create(const S3ClientConf& s3_conf);

    static Status convert_properties_to_s3_conf(const std::map<std::string, std::string>& prop,
                                                const S3URI& s3_uri, S3Conf* s3_conf);

private:
    S3ClientFactory();
    static std::string get_valid_ca_cert_path();

    Aws::SDKOptions _aws_options;
    std::mutex _lock;
    std::unordered_map<uint64_t, std::shared_ptr<io::ObjStorageClient>> _cache;
    std::string _ca_cert_file_path;
};

} // end namespace doris
