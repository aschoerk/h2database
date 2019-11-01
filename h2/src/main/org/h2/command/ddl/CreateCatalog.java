/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.message.DbException;
import org.h2.schema.Catalog;
import org.h2.schema.Schema;

/**
 * This class represents the statement
 * CREATE DATABASE
 */
public class CreateCatalog extends DefineCommand {

    private String catalogName;
    private String authorization;
    private boolean ifNotExists;
    private ArrayList<String> tableEngineParams;

    public CreateCatalog(Session session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.getUser().checkSchemaAdmin();  // TODO: checkCatalogAdmin
        session.commit(true);
        Database db = session.getDatabase();
        User user = db.getUser(authorization);
        // during DB startup, the Right/Role records have not yet been loaded
        if (!db.isStarting()) {
            user.checkSchemaAdmin();  // TODO: checkCatalogAdmin
        }
        if (db.findCatalog(catalogName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.CATALOG_ALREADY_EXISTS_1, catalogName);
        }
        int id = getObjectId();
        Catalog catalog = new Catalog(db, id, catalogName, user, false);
        catalog.setTableEngineParams(tableEngineParams);
        db.addDatabaseObject(session, catalog);
        catalog.open();
        return 0;
    }

    public void setCatalogName(String name) {
        this.catalogName = name;
    }

    public void setAuthorization(String userName) {
        this.authorization = userName;
    }

    public void setTableEngineParams(ArrayList<String> tableEngineParams) {
        this.tableEngineParams = tableEngineParams;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_CATALOG;
    }

}
