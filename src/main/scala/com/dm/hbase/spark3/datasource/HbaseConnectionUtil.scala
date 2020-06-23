package com.dm.hbase.spark3.datasource

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}

object HbaseConnectionUtil {
  def withConnection[That](options: java.util.Map[String, String])(apply: Connection => That): That = {
    var connection: Connection = null;
    try {
      connection = getConnection(options);
      apply(connection)
    } finally {
      connection.close()
    }
  }

  def withAdmin[That](options: java.util.Map[String, String])(apply: Admin => That): That = {
    withConnection(options) {
      connection => {
        var admin: Admin = null
        try {
          admin = connection.getAdmin
          apply(admin)
        } finally {
          admin.close()
        }
      }
    }
  }

  def getConnection(options: java.util.Map[String, String]): Connection = {
    val zookeeperQuorum = options.get(HConstants.ZOOKEEPER_QUORUM)
    val zookeeperClientPort = options.get(HConstants.ZOOKEEPER_CLIENT_PORT)
    val configuration = HBaseConfiguration.create
    if (StringUtils.isNotBlank(zookeeperQuorum)) {
      configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    }
    if (StringUtils.isNotBlank(zookeeperClientPort)) {
      configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperClientPort)
    }
    ConnectionFactory.createConnection(configuration)
  }
}
