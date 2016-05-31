package org.deephacks.lmdbjni;


import org.bytedeco.javacpp.annotation.Platform;
import org.bytedeco.javacpp.annotation.Properties;
import org.bytedeco.javacpp.tools.InfoMap;
import org.bytedeco.javacpp.tools.InfoMapper;

@Properties(target = "org.deephacks.lmdbjni.LMDB", value = {
  @Platform(cinclude = "<lmdb.h>"),
  @Platform(value = "linux", link = "lmdb")})
class JavaCppLmdbMapper implements InfoMapper {
  @Override
  public void map(InfoMap infoMap) {
  }
}
