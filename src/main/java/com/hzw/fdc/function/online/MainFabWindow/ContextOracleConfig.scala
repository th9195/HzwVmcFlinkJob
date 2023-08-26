//package com.hzw.fdc.function.online.MainFabWindow
//
//import com.hzw.fdc.scalabean.{ContextConfig, ContextConfigData}
//
//import scala.collection.mutable.ListBuffer
//
///**
// * <p>描述</p>
// *
// * @author liuwentao
// * @date 2021/4/23 15:03
// */
//object ContextOracleConfig {
//
//  def fetch(): ContextConfig = {
//    val javaOracle = InitFetchContextConfig.fetch()
//    val javaIterator = javaOracle.getDatas.iterator
//
//    val contexts = ListBuffer[ContextConfigData]()
//    while (javaIterator.hasNext) {
//      val javaContext = javaIterator.next()
//
//      contexts.append(ContextConfigData(
//        javaContext.getLocationId,
//        javaContext.getLocationName,
//        javaContext.getModuleId,
//        javaContext.getModuleName,
//        javaContext.getToolName,
//        javaContext.getChamberName,
//        javaContext.getRecipeName,
//        javaContext.getProductName,
//        javaContext.getStage,
//        javaContext.getContextId,
//        javaContext.getToolGroupId,
//        javaContext.getToolGroupName,
//        javaContext.getToolGroupVersion,
//        javaContext.getChamberGroupId,
//        javaContext.getChamberGroupName,
//        javaContext.getChamberGroupVersion,
//        javaContext.getRecipeGroupId,
//        javaContext.getRecipeGroupName,
//        javaContext.getRecipeGroupVersion,
//        javaContext.getLimitStatus,
//        javaContext.getArea,
//        javaContext.getSection,
//        javaContext.getMesChamberName
//      ))
//    }
//
//    ContextConfig("context",
//      status = true,
//      contexts.toList)
//  }
//
//}
