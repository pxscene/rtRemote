/*

pxCore Copyright 2005-2018 John Robinson

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

#include "rtRemoteValueReader.h"
#include "rtRemoteObjectCache.h"
#include "rtRemoteClient.h"
#include "rtRemoteObject.h"
#include "rtRemoteFunction.h"
#include "rtRemoteMessage.h"

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

// maybe use some type of identifier to indicate remote, with reference
// to transport. I don't like transport being a member of rtRemoteObject
// and rtRemoteFunction.
// either of these should be able simply include a reference to a transport
// with a handle returned by server in json message.

#if 0
static std::string toString(rapidjson::Value const& v)
{
  rapidjson::StringBuffer buff;
  rapidjson::Writer< rapidjson::GenericStringBuffer< rapidjson::UTF8<> > > writer(buff);
  v.Accept(writer);

  char const* s = buff.GetString();
  return (s != nullptr ? std::string(s) : std::string());
}
#endif

rtError
rtRemoteValueReader::read(rtRemoteEnvironment* env, rtValue& to, rapidjson::Value const& from,
  std::shared_ptr<rtRemoteClient> const& client)
{
  auto type = from.FindMember(kFieldNameValueType);
  if (type  == from.MemberEnd())
  {
    rtLogError("read: failed to find member '%s'. RT_ERROR_PROTOCOL_ERROR", kFieldNameValueType);
    return RT_ERROR_PROTOCOL_ERROR;
  }

  int const typeId = type->value.GetInt();

  auto val = from.FindMember(kFieldNameValueValue);
  if (((typeId != RT_functionType) && (typeId != RT_voidType)) && (val == from.MemberEnd()))
  {
    rtLogError("read: failed to find member '%s'. RT_ERROR_PROTOCOL_ERROR", kFieldNameValueValue);
    return RT_ERROR_PROTOCOL_ERROR;
  }

  switch (type->value.GetInt())
  {
    case RT_voidType:
    to.setEmpty();
    break;

    case RT_valueType:
    RT_ASSERT(false);
    break;

    case RT_boolType:
    to = rtValue(val->value.GetBool());
    break;

    case RT_int8_tType:
    to.setInt8(static_cast<int8_t>(val->value.GetInt()));
    break;

    case RT_uint8_tType:
    to.setUInt8(static_cast<uint8_t>(val->value.GetInt()));
    break;

    case RT_int32_tType:
    to.setInt32(static_cast<int32_t>(val->value.GetInt()));
    break;

    case RT_uint32_tType:
    to.setUInt32(static_cast<uint32_t>(val->value.GetUint()));
    break;

    case RT_int64_tType:
    to.setInt64(static_cast<int64_t>(val->value.GetInt64()));
    break;
    
    case RT_uint64_tType:
    to.setUInt64(static_cast<uint64_t>(val->value.GetUint64()));
    break;

    case RT_floatType:
    to.setFloat(static_cast<float>(val->value.GetDouble()));
    break;
    
    case RT_doubleType:
    to.setFloat(static_cast<double>(val->value.GetDouble()));
    break;

    case RT_stringType:
    {
      rtString s(val->value.GetString());
      to.setString(s);
    }
    break;

    case RT_objectType:
    {
      RT_ASSERT(client != NULL);
      if (!client)
      {
        rtLogError("read: no client. RT_ERROR_PROTOCOL_ERROR");
        return RT_ERROR_PROTOCOL_ERROR;
      }

      auto const& obj = from.FindMember("value");
      RT_ASSERT(obj != from.MemberEnd());

      auto id = obj->value.FindMember(kFieldNameObjectId);
      if (strcmp(id->value.GetString(), kNullObjectId) == 0)
      {
        to.setObject(rtObjectRef());
      }
      else
      {
        rtObjectRef ref = env->ObjectCache->findObject(id->value.GetString());
        if (ref)
          to.setObject(ref);
        else
          to.setObject(new rtRemoteObject(id->value.GetString(), client));
      }
    }
    break;

    case RT_functionType:
    {
      RT_ASSERT(client != NULL);
      if (!client)
      {
        rtLogError("read: no client. RT_ERROR_PROTOCOL_ERROR");
        return RT_ERROR_PROTOCOL_ERROR;
      }

      std::string objectId;
      std::string functionId;

      auto const& func = from.FindMember("value");
      if (func != from.MemberEnd())
      {
        auto itr = func->value.FindMember(kFieldNameObjectId);
        RT_ASSERT(itr != func->value.MemberEnd());
        objectId = itr->value.GetString();

        itr = func->value.FindMember(kFieldNameFunctionName);
        RT_ASSERT(itr != func->value.MemberEnd());
        functionId = itr->value.GetString();
      }
      else
      {
        auto itr = from.FindMember(kFieldNameObjectId);
        RT_ASSERT(itr != from.MemberEnd());
        objectId = itr->value.GetString();

        itr = from.FindMember(kFieldNameFunctionName);
        RT_ASSERT(itr != from.MemberEnd());
        functionId = itr->value.GetString();
      }

      if (strcmp(functionId.c_str(), kNullObjectId) == 0)
      {
        to.setFunction(rtFunctionRef());
      }
      else
      {
        // check object for cache first
        rtFunctionRef ref = env->ObjectCache->findFunction(functionId);
        if (ref)
          to.setFunction(ref);
        else
          to.setFunction(new rtRemoteFunction(objectId, functionId, client));
      }
    }
    break;

    case RT_voidPtrType:
    {
#if __x86_64 || __aarch64__
      to.setVoidPtr((void *) val->value.GetUint64());
#else
      to.setVoidPtr((void *) val->value.GetUint());
#endif
    }
    break;
  }

  return RT_OK;
}
