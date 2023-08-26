package com.hzw.fdc.scalabean

import jdk.nashorn.internal.objects.annotations.Property

/**
 * @author Liuwt
 * @date 2021/9/1712:51
 */
case class VirtualLevel(@Property var virtualSensorId: Option[Long],
                        @Property var rootVirtualSensorId: Option[Long],
                        @Property var sensorLevel: Option[Int],
                        @Property var paramValue: Option[Long])
