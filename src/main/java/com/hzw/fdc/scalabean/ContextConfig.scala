package com.hzw.fdc.scalabean

/**
 * <p>描述</p>
 *
 * @author liuwentao
 * @date 2021/4/23 16:23
 */
case class ContextConfig(dataType: String, status: Boolean, datas: List[ContextConfigData])

case class ContextConfigData(locationId: Long,
                             locationName: String,
                             moduleId: Long,
                             moduleName: String,
                             toolName: String,
                             toolId: Long,
                             chamberName: String,
                             chamberId: Long,
                             recipeName: String,
                             recipeId: Long,
                             productName: String,
                             stage: String,
                             contextId: Long,
                             toolGroupId: Long,
                             toolGroupName: String,
                             toolGroupVersion: Long,
                             chamberGroupId: Long,
                             chamberGroupName: String,
                             chamberGroupVersion: Long,
                             recipeGroupId: Long,
                             recipeGroupName: String,
                             recipeGroupVersion: Long,
                             limitStatus: Boolean,
                             area: String,
                             section: String,
                             mesChamberName: String
                            )
