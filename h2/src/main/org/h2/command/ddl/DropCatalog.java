/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Catalog;
import org.h2.schema.SchemaObject;

/**
 * This class represents the statement
 * DROP CATALOG
 */
public class DropCatalog extends DefineCommand {

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

        Catalog catalog = db.findCatalog(catalogName);
        if (catalog == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.CATALOG_NOT_FOUND_1, catalogName);
            }
        } else {
            if (!catalog.canDrop()) {
                throw DbException.get(ErrorCode.CATALOG_CAN_NOT_BE_DROPPED_1, catalogName);
            }
            if (dropAction == ConstraintActionType.RESTRICT && !catalog.isEmpty()) {
                ArrayList<SchemaObject> all = catalog.getAllSchemaObjects();
                int size = all.size();
                if (size > 0) {
                    StringBuilder builder = new StringBuilder();
                    for (int i = 0; i < size; i++) {
                        if (i > 0) {
                            builder.append(", ");
                        }
                        builder.append(all.get(i).getName());
                    }
                    throw DbException.get(ErrorCode.CANNOT_DROP_2, catalogName, builder.toString());
                }
            }
            db.removeDatabaseObject(session, catalog);
        }
        return 0;
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
