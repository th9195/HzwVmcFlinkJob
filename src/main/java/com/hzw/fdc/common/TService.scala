package com.hzw.fdc.common

/**
 * @author gdj
 * @create 2020-05-25-17:23
 *
 */
trait TService extends Serializable {
  /**
   * 获取
   *
   * @return
   */
  def getDao(): TDao

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
