/**
* Copyright 2021 Comcast Cable Communications Management, LLC
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* SPDX-License-Identifier: Apache-2.0
*/

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
  rtError reconnect();
  rtError reregisterObjects();

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
  std::thread*              m_reconnection;
  int                       m_daemon_connection;
  sockaddr_storage          m_daemon_endpoint;
  sockaddr_storage          m_rpc_endpoint;
  rtRemoteEnvironment*      m_env;
  rtRemoteSocketBuffer      m_buff;
  RequestContextMap         m_outstanding_requests;
};

#endif
