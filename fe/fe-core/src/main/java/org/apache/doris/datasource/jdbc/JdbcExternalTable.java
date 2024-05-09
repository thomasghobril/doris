// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.jdbc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.JdbcAnalysisTask;
import org.apache.doris.thrift.TTableDescriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Elasticsearch external table.
 */
public class JdbcExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(JdbcExternalTable.class);

    private JdbcTable jdbcTable;

    /**
     * Create jdbc external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param dbName Database name.
     * @param catalog HMSExternalDataSource.
     */
    public JdbcExternalTable(long id, String name, String dbName, JdbcExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.JDBC_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            jdbcTable = toJdbcTable();
            objectCreated = true;
        }
    }

    public JdbcTable getJdbcTable() {
        makeSureInitialized();
        return jdbcTable;
    }

    @Override
    public TTableDescriptor toThrift() {
        makeSureInitialized();
        return jdbcTable.toThrift();
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        return Optional.of(new SchemaCacheValue(((JdbcExternalCatalog) catalog).getJdbcClient()
                .getColumnsFromJdbc(dbName, name)));
    }

    private JdbcTable toJdbcTable() {
        List<Column> schema = getFullSchema();
        JdbcExternalCatalog jdbcCatalog = (JdbcExternalCatalog) catalog;
        String fullDbName = this.dbName + "." + this.name;
        JdbcTable jdbcTable = new JdbcTable(this.id, fullDbName, schema, TableType.JDBC_EXTERNAL_TABLE);
        jdbcTable.setCatalogId(jdbcCatalog.getId());
        jdbcTable.setExternalTableName(fullDbName);
        jdbcTable.setRemoteDatabaseName(
                ((JdbcExternalCatalog) catalog).getJdbcClient().getRemoteDatabaseName(this.dbName));
        jdbcTable.setRemoteTableName(
                ((JdbcExternalCatalog) catalog).getJdbcClient().getRemoteTableName(this.dbName, this.name));
        jdbcTable.setRemoteColumnNames(((JdbcExternalCatalog) catalog).getJdbcClient().getRemoteColumnNames(this.dbName,
                this.name));
        jdbcTable.setJdbcTypeName(jdbcCatalog.getDatabaseTypeName());
        jdbcTable.setJdbcUrl(jdbcCatalog.getJdbcUrl());
        jdbcTable.setJdbcUser(jdbcCatalog.getJdbcUser());
        jdbcTable.setJdbcPasswd(jdbcCatalog.getJdbcPasswd());
        jdbcTable.setDriverClass(jdbcCatalog.getDriverClass());
        jdbcTable.setDriverUrl(jdbcCatalog.getDriverUrl());
        jdbcTable.setResourceName(jdbcCatalog.getResource());
        jdbcTable.setCheckSum(jdbcCatalog.getCheckSum());
        jdbcTable.setConnectionPoolMinSize(jdbcCatalog.getConnectionPoolMinSize());
        jdbcTable.setConnectionPoolMaxSize(jdbcCatalog.getConnectionPoolMaxSize());
        jdbcTable.setConnectionPoolMaxLifeTime(jdbcCatalog.getConnectionPoolMaxLifeTime());
        jdbcTable.setConnectionPoolMaxWaitTime(jdbcCatalog.getConnectionPoolMaxWaitTime());
        jdbcTable.setConnectionPoolKeepAlive(jdbcCatalog.isConnectionPoolKeepAlive());
        return jdbcTable;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new JdbcAnalysisTask(info);
    }
}