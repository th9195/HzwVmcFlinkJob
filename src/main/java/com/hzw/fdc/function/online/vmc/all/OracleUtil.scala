package com.hzw.fdc.function.online.vmc.all

import com.hzw.fdc.scalabean.VmcBeans.VmcControlPlanConfig
import com.hzw.fdc.util.ProjectConfig
import org.apache.commons.dbcp.BasicDataSource
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, ResultSet}
import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 *
 * @author tanghui
 * @date 2023/9/7 8:46
 * @description OracleUtil
 */
object OracleUtil {

  private val logger: Logger = LoggerFactory.getLogger(OracleUtil.getClass)

  //连接池对象
  private var ds : BasicDataSource = _

  private var connection : Connection = _

  def initDB() = {

    try{
      ds = new BasicDataSource()
      ds.setDriverClassName(ProjectConfig.VMC_ORACLE_DRIVER_CLASS)
      ds.setUrl(ProjectConfig.VMC_ORACLE_URL);
      ds.setUsername(ProjectConfig.VMC_ORACLE_USER)
      ds.setPassword(ProjectConfig.VMC_ORACLE_PASSWORD)
      ds.setInitialSize(new Integer(ProjectConfig.VMC_ORACLE_POOL_MIN_SIZE))
      ds.setMaxActive(new Integer(ProjectConfig.VMC_ORACLE_POOL_MAX_SIZE))
    }catch {
      case e:Exception =>{
        logger.error(s"initDB error ! ${e.printStackTrace()}")
      }
    }
  }

  def getConnection()={
    try{
      if(null == ds){
        initDB()
      }

      if(null != ds){
        connection = ds.getConnection
      }
    }catch{
      case e:Exception =>
        logger.error(s"获取oracle 连接失败！getConnection error!")
    }
  }


  def close () = {
    if(null != connection){
      try{
        connection.close()
      }catch{
        case e:Exception =>
          logger.error(s"关闭oracle 连接失败！close error!")
      }
    }
  }


  def queryVmcOracle(toolName:String,route:String,stageName:String,tableName:String = "tableName") = {

    val controlPlanConfigList = new ListBuffer[VmcControlPlanConfig]()

    val sql = s"select * from ${tableName} where toolName = ? and route = ? and stageName = ?"

    try{
      if(null == connection){
        getConnection()
      }else{
        val preparedStatement = connection.prepareStatement(sql)

        preparedStatement.setString(1,toolName)
        preparedStatement.setString(2,route)
        preparedStatement.setString(3,stageName)

        val resultSet: ResultSet = preparedStatement.executeQuery()
        while (resultSet.next()){
          val controlPlanId = resultSet.getInt("COL_ID")
          val toolName = resultSet.getString("EQP")
        }
      }

    }catch {
      case e:Exception => {
        logger.error(s"查询策略表失败！queryVmcOracle error!\n " +
          s"tableName == ${tableName} ; toolName == ${toolName} ; route == ${route} ; stageName == ${stageName} \n " +
          s"sql == ${sql}")
        close()
        getConnection()
      }
    }

    controlPlanConfigList
  }


}
