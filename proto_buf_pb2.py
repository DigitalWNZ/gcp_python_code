# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto_buf.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0fproto_buf.proto\"\x9b\x02\n\x0bSomeMessage\x12!\n\x05\x61sset\x18\x01 \x01(\x0b\x32\x12.SomeMessage.Asset\x12+\n\npriorAsset\x18\x02 \x01(\x0b\x32\x17.SomeMessage.Priorasset\x12\x17\n\x0fpriorAssetState\x18\x03 \x01(\t\x12#\n\x06window\x18\x04 \x01(\x0b\x32\x13.SomeMessage.Window\x1a-\n\x05\x41sset\x12\x11\n\tancestors\x18\x01 \x03(\t\x12\x11\n\tassetType\x18\x02 \x01(\t\x1a\x32\n\nPriorasset\x12\x11\n\tancestors\x18\x01 \x03(\t\x12\x11\n\tassetType\x18\x02 \x01(\t\x1a\x1b\n\x06Window\x12\x11\n\tstartTime\x18\x01 \x01(\tb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto_buf_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _SOMEMESSAGE._serialized_start=20
  _SOMEMESSAGE._serialized_end=303
  _SOMEMESSAGE_ASSET._serialized_start=177
  _SOMEMESSAGE_ASSET._serialized_end=222
  _SOMEMESSAGE_PRIORASSET._serialized_start=224
  _SOMEMESSAGE_PRIORASSET._serialized_end=274
  _SOMEMESSAGE_WINDOW._serialized_start=276
  _SOMEMESSAGE_WINDOW._serialized_end=303
# @@protoc_insertion_point(module_scope)
