package com.hzw.fdc.scalabean

/**
 * @author gdj
 * @create 2020-08-19-18:43
 *
 */
case class RawData(locationId: Long,
                   locationName: String,
                   moduleId: Long,
                   moduleName: String,
                   toolGroupId: Long,
                   toolGroupName: String,
                   chamberGroupId: Long,
                   chamberGroupName: String,
                   recipeGroupName: String,
                   limitStatus: Boolean,
                   toolName: String,
                   toolId: Long,
                   chamberName: String,
                   chamberId: Long,
                   recipeName: String,
                   recipeId: Long,
                   contextId: Long,
                   productName: List[String],
                   stage: List[String],
                   data: List[sensorDataList],
                   runId: String,
                   runStartTime: Long,
                   runEndTime: Long,
                   windowStartTime: Long,
                   windowEndTime: Long,
                   windowindDataCreateTime: Long,
                   dataMissingRatio: Double,
                   controlWindowId: Long,
                   materialName:String,
                   pmStatus:String,
                   pmTimestamp:Long,
                   area: String,
                   section: String,
                   mesChamberName: String,
                   lotMESInfo: List[Option[Lot]],
                   unit: String,
                   dataVersion:String,
                   cycleIndex: String
                   )



case class windowRawData(stepId: Long,
                         timestamp: Long,
                         rawList: List[(String, Double, String)])


case class scanWindowTime(time: Long,
                          status: Boolean)

