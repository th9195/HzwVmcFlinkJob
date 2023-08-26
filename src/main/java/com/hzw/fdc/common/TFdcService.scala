package com.hzw.fdc.common

/**
 * FdcTService
 *
 * @desc:
 * @author tobytang
 * @date 2022/12/14 13:30
 * @since 1.0.0
 * @update 2022/12/14 13:30
 * */
trait TFdcService extends Serializable {
  /**
   * 获取
   *
   * @return
   */
  def getDao(): TFdcDao

  /**
   * 分析
   *
   * @return
   */
  def analyses(): Any

  /**
   * 获取data数据
   */
  protected def getDatas(): Any = {

  }
}

