# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: PATH/babushkaproto.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x18PATH/babushkaproto.proto\x12\rbabushkaproto\"\xb5\x01\n\x08Response\x12/\n\x04slot\x18\x01 \x03(\x0b\x32!.babushkaproto.Response.SlotRange\x1a\x17\n\x04Node\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x1a_\n\tSlotRange\x12\x13\n\x0bstart_range\x18\x01 \x01(\r\x12\x11\n\tend_range\x18\x02 \x01(\r\x12*\n\x04node\x18\x03 \x03(\x0b\x32\x1c.babushkaproto.Response.Node\"\n\n\x08NullResp\"\xa5\x01\n\x0c\x43ommandReply\x12\x14\n\x0c\x63\x61llback_idx\x18\x01 \x01(\r\x12\x12\n\x05\x65rror\x18\x02 \x01(\tH\x01\x88\x01\x01\x12+\n\x05resp1\x18\x03 \x01(\x0b\x32\x1a.babushkaproto.StrResponseH\x00\x12(\n\x05resp2\x18\x04 \x01(\x0b\x32\x17.babushkaproto.NullRespH\x00\x42\n\n\x08responseB\x08\n\x06_error\"\x1d\n\x0eRepStrResponse\x12\x0b\n\x03\x61rg\x18\x01 \x03(\t\"\x1a\n\x0bStrResponse\x12\x0b\n\x03\x61rg\x18\x01 \x01(\t\"H\n\x04Node\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\r\x12\x0f\n\x07node_id\x18\x03 \x01(\t\x12\x10\n\x08hostname\x18\x04 \x01(\t\"{\n\x04Slot\x12\x13\n\x0bstart_range\x18\x01 \x01(\r\x12\x11\n\tend_range\x18\x02 \x01(\r\x12$\n\x07primary\x18\x03 \x01(\x0b\x32\x13.babushkaproto.Node\x12%\n\x08replicas\x18\x04 \x03(\x0b\x32\x13.babushkaproto.Node\"6\n\x10\x63lusterSlotsResp\x12\"\n\x05slots\x18\x01 \x03(\x0b\x32\x13.babushkaproto.Slot\"B\n\x07Request\x12\x14\n\x0c\x63\x61llback_idx\x18\x01 \x01(\r\x12\x14\n\x0crequest_type\x18\x02 \x01(\r\x12\x0b\n\x03\x61rg\x18\x03 \x03(\tb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'PATH.babushkaproto_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _RESPONSE._serialized_start=44
  _RESPONSE._serialized_end=225
  _RESPONSE_NODE._serialized_start=105
  _RESPONSE_NODE._serialized_end=128
  _RESPONSE_SLOTRANGE._serialized_start=130
  _RESPONSE_SLOTRANGE._serialized_end=225
  _NULLRESP._serialized_start=227
  _NULLRESP._serialized_end=237
  _COMMANDREPLY._serialized_start=240
  _COMMANDREPLY._serialized_end=405
  _REPSTRRESPONSE._serialized_start=407
  _REPSTRRESPONSE._serialized_end=436
  _STRRESPONSE._serialized_start=438
  _STRRESPONSE._serialized_end=464
  _NODE._serialized_start=466
  _NODE._serialized_end=538
  _SLOT._serialized_start=540
  _SLOT._serialized_end=663
  _CLUSTERSLOTSRESP._serialized_start=665
  _CLUSTERSLOTSRESP._serialized_end=719
  _REQUEST._serialized_start=721
  _REQUEST._serialized_end=787
# @@protoc_insertion_point(module_scope)
