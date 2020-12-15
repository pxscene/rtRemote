
#include "rtRemote.h"
#include "rtRemoteEnvironment.h"
#include "rtRemoteConfig.h"
#include "rtRemoteConfigBuilder.h"
#include "rtRemoteMulticastResolver.h"
#include "rtRemoteSocketUtils.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <vector>

#include <rapidjson/memorystream.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <getopt.h>
#include <unistd.h>

namespace
{
  // XXX: the locateObject() call return sockaddr_storage, we don't know if that means
  // tcp or udp. we should (at some point) replace sockaddr_storage with
  // rtRemoteEndPoint everywhere

  // XXX: peer-to-peer with UDP wont work!
  std::string
  sockaddr_to_string(sockaddr_storage const& soc)
  {
    std::unique_ptr<rtRemoteEndPoint> tempEndpoint;
    if (soc.ss_family == AF_UNIX)
      tempEndpoint.reset(rtRemoteFileEndPoint::fromSockAddr(soc));
    else
      tempEndpoint.reset(rtRemoteIPEndPoint::fromSockAddr("tcp", soc));
    return tempEndpoint->toString();
  }
}

struct rtRemoteConnectedClient
{
  rtRemoteConnectedClient()
      : fd(-1)
      , status(RT_OK)
      , last_keep_alive(std::chrono::system_clock::now())
  {
    remote_endpoint_length = sizeof(sockaddr_storage);
    memset(&remote_endpoint, 0, sizeof(sockaddr_storage));
    read_buffer.reserve(1024 * 16);
    read_buffer.resize(1024 * 16);
  }

  int fd;
  sockaddr_storage remote_endpoint;
  socklen_t remote_endpoint_length;
  rtRemoteSocketBuffer read_buffer;
  rtError status;
  std::chrono::time_point<std::chrono::system_clock> last_keep_alive;
};

class rtRemoteResolver
{
public:
  using RequestHandler = std::function<rtError(
    rtRemoteConnectedClient const& from_client, 
    rtRemoteMessagePtr const& req,
    rtRemoteMessagePtr& res)>;

  rtRemoteResolver(rtRemoteEnvironment* env)
    : m_listen_fd(-1)
    , m_multicast_resolver(nullptr)
    , m_env(env)
    , m_pid(getpid())
  {
    memset(&m_listen_endpoint, 0, sizeof(m_listen_endpoint));
    m_multicast_resolver = new rtRemoteMulticastResolver(m_env);

    using namespace std::placeholders;

    setRequestHandler(kNsMessageTypeLookup, std::bind(&rtRemoteResolver::doLookup, this, _1, _2, _3));
    setRequestHandler(kNsMessageTypeDeregister, std::bind(&rtRemoteResolver::doDeregister, this, _1, _2, _3));
    setRequestHandler(kNsMessageTypeRegister, std::bind(&rtRemoteResolver::doRegister, this, _1, _2, _3));
  }

public:
  void setRequestHandler(std::string const &command_name, RequestHandler handler)
  {
    m_request_handlers.insert(std::make_pair(command_name, handler));
  }

  rtError run()
  {
    rtRemoteMessagePtr keepAlive;
    std::chrono::milliseconds const keepAliveDuration(
      m_env->Config->stream_keep_alive_interval() * 1000);

    while (true)
    {
      int maxFd = 0;

      fd_set readFds;
      fd_set errFds;

      FD_ZERO(&readFds);
      rtPushFd(&readFds, m_listen_fd, &maxFd);

      FD_ZERO(&errFds);
      rtPushFd(&errFds, m_listen_fd, &maxFd);

      timeval timeout;
      timeout.tv_sec = 1;
      timeout.tv_usec = 0;

      for (rtRemoteConnectedClient client : m_connected_clients)
      {
        rtPushFd(&readFds, client.fd, &maxFd);
        rtPushFd(&errFds, client.fd, &maxFd);
      }

      int ret = select(maxFd + 1, &readFds, nullptr, &errFds, &timeout);
      if (ret == -1)
      {
        rtError e = rtErrorFromErrno(errno);
        rtLogWarn("select failed:%s", rtStrError(e));
        continue;
      }
      else if (ret > 0)
      {
        if (FD_ISSET(m_listen_fd, &readFds))
          doAccept();

        for (rtRemoteConnectedClient& client : m_connected_clients)
        {
          if (FD_ISSET(client.fd, &readFds))
          {
            rtError err = doRead(client);
            if (err != RT_OK)
              client.status = err;
          }
          if (FD_ISSET(client.fd, &errFds))
            client.status = RT_FAIL;
        }
      }
      
      if (m_connected_clients.size() > 0)
      {
        std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
        for (rtRemoteConnectedClient& client : m_connected_clients)
        {
          if ((now - client.last_keep_alive) >  keepAliveDuration)
          {
            if (!keepAlive)
            {
              keepAlive.reset(new rapidjson::Document());
              keepAlive->SetObject();
              keepAlive->AddMember(kFieldNameMessageType, kMessageTypeKeepAliveRequest, keepAlive->GetAllocator());
              keepAlive->AddMember(kFieldNameSenderId, m_pid, keepAlive->GetAllocator());
              rtRemoteCorrelationKey seqId = rtMessage_GetNextCorrelationKey();
              keepAlive->AddMember(kFieldNameCorrelationKey, seqId.toString(), keepAlive->GetAllocator());
            }
            if (sendKeepAlive(client, *keepAlive) != RT_OK)
              client.status == RT_FAIL;
            else {
              client.last_keep_alive = now;
            }
          }
        }

        keepAlive.reset();
      }

      m_connected_clients.erase(std::remove_if(m_connected_clients.begin(), m_connected_clients.end(),
        [](rtRemoteConnectedClient& client)
        {
          if (client.status != RT_OK)
            ::close(client.fd);
          return client.status != RT_OK;
        }), std::end(m_connected_clients));

      if (FD_ISSET(m_listen_fd, &errFds))
      {
        rtLogWarn("error on listen fd?");
        continue;
      }
    }

    return RT_OK;
  }

  rtError sendKeepAlive(rtRemoteConnectedClient& client, rapidjson::Document const& keepAlive)
  {
    return rtSendDocument(keepAlive, client.fd, nullptr);
  }

  rtError doRead(rtRemoteConnectedClient& client)
  {
    rtRemoteMessagePtr req;
    rtError err = rtReadMessage(client.fd, client.read_buffer, req);
    if (err != RT_OK)
    {
      rtLogInfo("failed to read incoming message from client. %s", rtStrError(err));
      return err;
    }

    char const *message_type = rtMessage_GetMessageType(*req);
    if (!message_type)
    {
      rtLogError("got incoming message without a type field");
      return RT_FAIL;
    }

    auto itr = m_request_handlers.find(std::string(message_type));
    if (itr != m_request_handlers.end())
    {
      rtRemoteMessagePtr res;
      err = itr->second(client, req, res);
      if ((err == RT_OK) && (res != nullptr))
      {
        err = rtSendDocument(*res, client.fd, nullptr);
        if (err != RT_OK)
        {
          // TODO: close client connection down? 
          rtLogWarn("failed to send response to %s. %s", rtSocketToString(client.remote_endpoint).c_str(),
            rtStrError(err));
          client.status = err;
        }
      }
    }
    else
    {
      rtLogError("couldn't find message handler for message with type '%s'",
        message_type);
      err = RT_FAIL;
    }    

    return err;
  }

  rtError doDeregister(rtRemoteConnectedClient const& from_client, rtRemoteMessagePtr const& req, rtRemoteMessagePtr& res)
  {
    std::string const object_name = (*req)[kFieldNameObjectId].GetString();

    rtError err = m_multicast_resolver->unregisterObject(object_name);

    res.reset(new rapidjson::Document());
    res->SetObject();
    res->AddMember(kFieldNameMessageType, kNsMessageTypeRegisterResponse, res->GetAllocator());
    res->AddMember(kFieldNameSenderId, m_pid, res->GetAllocator());
    res->AddMember(kFieldNameCorrelationKey, (*req)[kFieldNameCorrelationKey], res->GetAllocator());
    res->AddMember(kNsFieldNameStatusCode, err, res->GetAllocator());

    return err;
  }

  rtError doRegister(rtRemoteConnectedClient const& from_client, rtRemoteMessagePtr const& req, rtRemoteMessagePtr& res)
  {
    std::string const object_name = (*req)[kFieldNameObjectId].GetString();
    sockaddr_storage endpoint = rtRemoteEndPoint::fromString((*req)[kFieldNameEndPoint].GetString())
        ->toSockAddr();

    rtError err = m_multicast_resolver->registerObject(object_name, endpoint);

    res.reset(new rapidjson::Document());
    res->SetObject();
    res->AddMember(kFieldNameMessageType, kNsMessageTypeRegisterResponse, res->GetAllocator());
    res->AddMember(kFieldNameSenderId, m_pid, res->GetAllocator());
    res->AddMember(kFieldNameCorrelationKey, (*req)[kFieldNameCorrelationKey], res->GetAllocator());
    res->AddMember(kNsFieldNameStatusCode, err, res->GetAllocator());

    return err;
  }

  rtError doLookup(rtRemoteConnectedClient const& from_client, rtRemoteMessagePtr const& req, rtRemoteMessagePtr& res)
  {

    std::string const objectName = (*req)[kFieldNameObjectId].GetString();

    uint32_t timeout = 5000;
    if (req->HasMember(kFieldNameTimeout))
      timeout = (*req)[kFieldNameTimeout].GetUint();

    rtLogDebug("doing lookup of %s for remote client, timeout:%u",
      objectName.c_str(), timeout);

    sockaddr_storage endpoint;
    memset(&endpoint, 0, sizeof(endpoint));

    rtError err = m_multicast_resolver->locateObject(objectName, endpoint, timeout);

    res.reset(new rapidjson::Document());
    res->SetObject();
    res->AddMember(kFieldNameMessageType, kMessageTypeLocate, res->GetAllocator());
    res->AddMember(kFieldNameSenderId, m_pid, res->GetAllocator());
    res->AddMember(kFieldNameCorrelationKey, (*req)[kFieldNameCorrelationKey], res->GetAllocator());
    res->AddMember(kFieldNameObjectId, objectName, res->GetAllocator());

    if (err == RT_OK)
    {
      std::string endpointAsString = sockaddr_to_string(endpoint);
      res->AddMember(kFieldNameEndPoint, endpointAsString, res->GetAllocator());
    }

    return err;
  }

  rtError open()
  {
    rtError err = rtParseAddress(m_listen_endpoint, m_env->Config->resolver_unicast_address().c_str(),
      m_env->Config->resolver_unicast_port(), nullptr);
    if (err != RT_OK)
    {
      rtLogError("failed to parse unicast listen address/port %s/%u. %s", m_env->Config->resolver_unicast_address().c_str(),
        m_env->Config->resolver_unicast_port(), rtStrError(err));
      return err;
    }

    std::string endpoint_as_string = rtSocketToString(m_listen_endpoint);

    m_listen_fd = socket(m_listen_endpoint.ss_family, SOCK_STREAM, 0);
    if (m_listen_fd == -1)
    {
      err = rtErrorFromErrno(errno);

      rtLogError("failed to create tcp socket %s. %s", endpoint_as_string.c_str(), rtStrError(err));
      return err;
    }
    
    socklen_t len = (m_listen_endpoint.ss_family == AF_INET 
      ? sizeof(sockaddr_in)
      : sizeof(sockaddr_in6));

    int enable = 1;
    setsockopt(m_listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

    int ret = bind(m_listen_fd, reinterpret_cast<sockaddr *>(&m_listen_endpoint),len);
    if (ret == -1)
    {
      err = rtErrorFromErrno(errno);
      rtLogError("failed to bind listen socket to %s. %s", endpoint_as_string.c_str(), rtStrError(err));
      return err;
    }

    ret = listen(m_listen_fd, 1);
    if (ret == -1)
    {
      rtLogError("failed to put socket in listen mode. %s", rtStrError(err));
      ::close(m_listen_fd);
      return err;
    }

    if (err == RT_OK)
    {
      rtLogInfo("opened listen socket on:%s", endpoint_as_string.c_str());
    }

    err = rtParseAddress(m_rpc_endpoint, m_env->Config->server_listen_interface().c_str(), 0, nullptr);
    if (err != RT_OK)
    {
      rtLogError("failed to parse locatl rpc interface. %s. %s",
        m_env->Config->server_listen_interface().c_str(),
        rtStrError(err));
      ::close(m_listen_fd);
      return err;
    }

    m_multicast_resolver->open(m_rpc_endpoint);

    return err;
  }

  void doAccept()
  {
    rtLogInfo("accepting new client connection");
    rtRemoteConnectedClient new_client;

    int ret = accept(m_listen_fd, reinterpret_cast<sockaddr *>(&new_client.remote_endpoint), &new_client.remote_endpoint_length);
    if (ret == -1)
    {
      rtError err = rtErrorFromErrno(errno);
      rtLogWarn("accept failed. %s", rtStrError(err));
    }
    else
    {
      std::string const s = rtSocketToString(new_client.remote_endpoint);
      rtLogInfo("accepted new client connection from:%s", s.c_str());

      new_client.fd = ret;
      m_connected_clients.push_back(new_client);
    }
  }

private:
  int                                   m_listen_fd;
  sockaddr_storage                      m_listen_endpoint;
  std::vector<rtRemoteConnectedClient>  m_connected_clients;
  rtRemoteMulticastResolver*            m_multicast_resolver;
  rtRemoteEnvironment*                  m_env;
  std::map<std::string, RequestHandler> m_request_handlers;
  pid_t                                 m_pid;

  // TODO: probably not needed
  sockaddr_storage                      m_rpc_endpoint;
};



int main(int argc, char* argv[])
{
  rtError                       err;
  rtRemoteConfig*               config;
  rtRemoteEnvironment*          env;

  config = nullptr;
  env = nullptr;

  while (true)
  {
    int option_index = 0;
    static struct option long_options[] =
    {
      {"config",        required_argument,  0,  'c'},
      {0, 0, 0, 0}
    };

    int c = getopt_long(argc, argv, "c:", long_options, &option_index);
    if (c == -1)
      break;

    switch (c)
    {
      case 'c':
      {
        std::unique_ptr<rtRemoteConfigBuilder> builder(rtRemoteConfigBuilder::fromFile(optarg));
        if (!builder)
        {
          std::cerr << "failed to read configuration file:" << optarg << std::endl;
          exit(1);
        }
        config = builder->build();
      }
      break;
    }
  }

  if (!config)
  {
    std::unique_ptr<rtRemoteConfigBuilder> builder(rtRemoteConfigBuilder::getDefaultConfig());
    config = builder->build();
  }

  env = new rtRemoteEnvironment(config);
  rtRemoteResolver server(env);

  err = server.open();
  if (err != RT_OK)
  {
    rtLogError("failed to open server. %s", rtStrError(err));
    exit(1);
  }

  server.run();

  delete env;

  return 0;
}
