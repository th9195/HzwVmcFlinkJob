package com.hzw.fdc.common

/**
 * @author gdj
 * @create 2020-05-25-17:16
 *
 */
trait TController extends Serializable {
  def execute(): Unit
}
