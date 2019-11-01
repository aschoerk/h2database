/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Catalog;

/**
 * This class represents the statement
 * DROP ALL OBJECTS
 */
public class DropCatalog extends DefineCommand {

    private boolean dropAllObjects;
    private boolean deleteFiles;
    private String catalogName;
    private boolean ifExists = false;
    private ConstraintActionType dropAction;

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(final String catalogName) {
        this.catalogName = catalogName;
    }

    public DropCatalog(Session session) {
        super(session);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                ConstraintActionType.RESTRICT : ConstraintActionType.CASCADE;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        db.lockMeta(session);

        // TODO session-local temp tables are not removed
        for (Catalog catalog: db.getAllCatalogs()) {
            if (catalog.getName().equals(catalogName)) {
                if (!catalog.canDrop()) {
                    throw DbException.get(ErrorCode.CATALOG_CAN_NOT_BE_DROPPED_1, catalogName);
                }
                db.removeDatabaseObject(session, catalog);
                return 0;
            }
        }
        if (ifExists)
            return 0;
        else
            throw DbException.get(ErrorCode.CATALOG_NOT_FOUND_1, catalogName);
    }


    @Override
    public int getType() {
        return CommandInterface.DROP_CATALOG;
    }

    public void setIfExists(final boolean ifExists) {
        this.ifExists = ifExists;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

}
