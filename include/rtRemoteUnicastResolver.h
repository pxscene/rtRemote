
#ifndef RTREMOTE_UNICAST_RESOLVER_H
#define RTREMOTE_UNICAST_RESOLVER_H

#include "rtRemoteIResolver.h"

#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <thread>

class rtRemoteEnvironment;

class rtRemoteUnicastResolver : public rtRemoteIResolver
{
public:
  rtRemoteUnicastResolver(rtRemoteEnvironment* env);
  ~rtRemoteUnicastResolver();

public:
  rtError open(sockaddr_storage const& rpc_endpoint) override;
  rtError close() override;
  rtError registerObject(std::string const& name, sockaddr_storage const& endpoint) override;
  rtError locateObject(std::string const& name, sockaddr_storage& endpoint, uint32_t timeout) override;
  rtError unregisterObject(std::string const& name) override;

private:
  void readIncomingMessages();
  rtError connectToResolverServer();

  rtError sendRequest(rapidjson::Document const& req, rtRemoteMessagePtr& res, uint32_t timeout);

  struct RequestContext
  {
    RequestContext(rtRemoteCorrelationKey const& seqId)
      : SeqId(seqId) { }
    rtRemoteCorrelationKey  SeqId;
    rtRemoteMessagePtr      Response;
  };

  using RequestContextMap = std::map< rtRemoteCorrelationKey, RequestContext >;

private:
  pid_t                     m_pid;
  std::mutex                m_mutex;
  std::condition_variable   m_cond;
  std::thread*              m_message_reader;
  int                       m_daemon_connection;
  sockaddr_storage          m_daemon_endpoint;
  sockaddr_storage          m_rpc_endpoint;
  rtRemoteEnvironment*      m_env;
  rtRemoteSocketBuffer      m_buff;
  RequestContextMap         m_outstanding_requests;
};

#endif
