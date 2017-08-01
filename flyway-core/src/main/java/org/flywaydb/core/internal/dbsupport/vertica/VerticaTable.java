/*
 * Copyright 2010-2017 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.dbsupport.vertica;

import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.JdbcTemplate;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.Table;

import java.sql.SQLException;

/**
 * Vertica-specific table.
 */
public class VerticaTable extends Table {
    /**
     * Creates a new Vertica table.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param dbSupport    The database-specific support.
     * @param schema       The schema this table lives in.
     * @param name         The name of the table.
     */
    public VerticaTable(JdbcTemplate jdbcTemplate, DbSupport dbSupport, Schema schema, String name) {
        super(jdbcTemplate, dbSupport, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TABLE " + dbSupport.quote(schema.getName(), name) + " CASCADE");
    }

    @Override
    protected boolean doExists() throws SQLException {
        // Querying tables from v_catalog is really slow if there are lot of tables in database.
        // Trying to access table and failing is a lot faster.
        try {
            jdbcTemplate.execute("SELECT 1 FROM " + this + " LIMIT 1");
            return true;
        } catch(SQLException e) {
            if (e.getErrorCode() == 4650 || e.getErrorCode() == 4656 || e.getErrorCode() == 4568)
                return false;
            else
                throw e;
        }
    }

    @Override
    protected void doLock() throws SQLException {
        jdbcTemplate.execute("SELECT * FROM " + this + " FOR UPDATE");
    }
}