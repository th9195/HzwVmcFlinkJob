package com.hzw.fdc.scalabean

/**
 * @author gdj
 * @create 2020-08-27-18:25
 *
 */
case class ScalaProcessContext(ProcessContext: ProcessContext,
                               frequency: String,
                               sensor: String,
                               datas: List[Datas])

case class ProcessContext(Location: Location, Material: Material, Process: Process)

case class Location(
                     Factory: Factory,
                     Line: Line,
                     Area: Area,
                     Department: Department,
                     EquipmentType: EquipmentType,
                     Equipment: Equipment,
                     ModuleType: ModuleType,
                     Module: Module,
                     SubSystems: SubSystems)

case class Material(Lot: Lot1, materialName: String)

case class Process(ControlJobName: ControlJobName,
                   ProcessJobName: ProcessJobName,
                   ProcessTime: ProcessTime,
                   Recipe: Recipe)

//Location
case class Area()

case class Department()

case class Equipment(name: String)

case class EquipmentType()

case class Factory()

case class Line()

case class Module(name: String)

case class ModuleType()

case class SubSystems()

//Material
case class Lot1(name: String)

//Process
case class ControlJobName()

case class ProcessJobName()

case class ProcessTime(start: String, stop: String)

case class Recipe(module: String)


case class Datas(data: String)