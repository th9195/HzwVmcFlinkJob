package com.hzw.fdc.scalabean

import com.hzw.fdc.bean.OfflineStatus

/**
 * <p>描述</p>
 *
 * @author liuwentao
 * @date 2021/5/14 19:38
 */
case class OfflineResult(taskId: Int,
                         status: OfflineStatus,
                         errorMsg: String)