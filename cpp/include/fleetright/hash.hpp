#pragma once

#include <openssl/evp.h>

#include <memory>
#include <stdexcept>
#include <string>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace fleetright {

template <typename Message> std::string DeterministicBytes(const Message &message) {
  std::string bytes;
  bytes.reserve(static_cast<std::size_t>(message.ByteSizeLong()));
  google::protobuf::io::StringOutputStream output(&bytes);
  google::protobuf::io::CodedOutputStream coded(&output);
  coded.SetSerializationDeterministic(true);
  if (!message.SerializeToCodedStream(&coded) || coded.HadError()) {
    throw std::runtime_error("protobuf deterministic serialization failed");
  }
  return bytes;
}

inline std::string Sha256(const std::string &bytes) {
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(EVP_MD_CTX_new(),
                                                                  EVP_MD_CTX_free);
  if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1 ||
      EVP_DigestUpdate(context.get(), bytes.data(), bytes.size()) != 1) {
    throw std::runtime_error("SHA-256 initialization failed");
  }
  unsigned char digest[EVP_MAX_MD_SIZE]{};
  unsigned int length = 0;
  if (EVP_DigestFinal_ex(context.get(), digest, &length) != 1 || length != 32U) {
    throw std::runtime_error("SHA-256 finalization failed");
  }
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  result.reserve(64);
  for (unsigned int i = 0; i < length; ++i) {
    result.push_back(hex[digest[i] >> 4U]);
    result.push_back(hex[digest[i] & 0x0fU]);
  }
  return result;
}

template <typename Message> std::string MessageSha256(const Message &message) {
  return Sha256(DeterministicBytes(message));
}

} // namespace fleetright
