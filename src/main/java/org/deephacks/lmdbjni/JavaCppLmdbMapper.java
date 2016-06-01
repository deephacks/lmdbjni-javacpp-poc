package org.deephacks.lmdbjni;


import org.bytedeco.javacpp.annotation.Platform;
import org.bytedeco.javacpp.annotation.Properties;
import org.bytedeco.javacpp.tools.Info;
import org.bytedeco.javacpp.tools.InfoMap;
import org.bytedeco.javacpp.tools.InfoMapper;

@Properties(target = "org.deephacks.lmdbjni.LMDB", value = {
  @Platform(cinclude = "<lmdb.h>"),
  @Platform(value = "linux", link = "lmdb")})
public class JavaCppLmdbMapper implements InfoMapper {
  @Override
  public void map(InfoMap infoMap) {
    infoMap.put(new Info("_WIN32").define(false));
  }
}
