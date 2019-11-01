package org.h2.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.DbObjectBase;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.engine.User;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.MetaTable;
import org.h2.table.Table;
import org.h2.table.TableSynonym;

/**
 * @author aschoerk
 */
public class Catalog extends DbObjectBase {
    private boolean system;
    private User owner;
    private ConcurrentHashMap<String, Schema> schemas;
    private Schema infoSchema;
    private Schema mainSchema;
    private ArrayList<String> tableEngineParams;
    private boolean metaTablesInitialized = false;

    public Catalog(Database database, int id, String catalogName, User owner,
                  boolean system) {
        super(database, id, catalogName, Trace.CATALOG);
        infoSchema = new Schema(database, this, Constants.INFORMATION_SCHEMA_ID, database.sysIdentifier("INFORMATION_SCHEMA"),
                database.getSystemUser(),
                true);
        mainSchema = new Schema(database, this, Constants.MAIN_SCHEMA_ID, database.sysIdentifier(Constants.SCHEMA_MAIN), database.getSystemUser(),
                true);

        schemas = new ConcurrentHashMap<>();
        this.owner = owner;
        this.system = system;
    }

    /**
     * Build a SQL statement to re-create the object, or to create a copy of the
     * object with a different name or referencing a different table
     *
     * @param table      the new table
     * @param quotedName the quoted name
     * @return the SQL statement
     */
    @Override
    public String getCreateSQLForCopy(final Table table, final String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getCreateSQL() {
        if (system) {
            return null;
        }
        StringBuilder builder = new StringBuilder("CREATE CATALOG IF NOT EXISTS ");
        getSQL(builder, true).append(" AUTHORIZATION ");
        owner.getSQL(builder, true);
        return builder.toString();
    }

    /**
     * Construct a DROP ... SQL statement for this object.
     *
     * @return the SQL statement
     */
    @Override
    public String getDropSQL() {
        StringBuilder builder = new StringBuilder("DROP CATALOG IF EXISTS ");
        getSQL(builder, true).append(" CASCADE");
        return builder.toString();
    }


    @Override
    public int getType() {
        return DbObject.CATALOG;
    }

    /**
     * Remove all dependent objects and free all resources (files, blocks in
     * files) of this object.
     *
     * @param session the session
     */
    @Override
    public void removeChildrenAndResources(final Session session) {
        if (canDrop()) {
            for (Schema schema : schemas.values()) {
                schema.setCanDrop(true);
                schema.removeChildrenAndResources(session);
            }
            database.removeMeta(session, getId());
        }
    }

    /**
     * Get table engine params of this schema.
     *
     * @return default table engine params
     */
    public ArrayList<String> getTableEngineParams() {
        return tableEngineParams;
    }

    /**
     * Set table engine params of this schema.
     * @param tableEngineParams default table engine params
     */
    public void setTableEngineParams(ArrayList<String> tableEngineParams) {
        this.tableEngineParams = tableEngineParams;
    }

    public ConcurrentHashMap<String, Schema> getSchemas() {
        return schemas;
    }

    public Schema getInfoSchema() {
        return infoSchema;
    }

    public Schema getMainSchema() {
        return mainSchema;
    }

    /**
     * Check if this catalog can be dropped. System catalogs can not be dropped.
     *
     * @return true if it can be dropped
     */
    public boolean canDrop() {
        return !system;
    }

    /**
     * Return whether is this schema is empty (does not contain any objects).
     *
     * @return {@code true} if this schema is empty, {@code false} otherwise
     */
    public boolean isEmpty() {
        return schemas.isEmpty() || schemas.values().stream().allMatch(s -> !s.canDrop());
    }

    public void open() {
        schemas.put(mainSchema.getName(), mainSchema);
        schemas.put(infoSchema.getName(), infoSchema);
    }

    public void initMetaTables() {
        if (metaTablesInitialized) {
            return;
        }
        synchronized (infoSchema) {
            if (!metaTablesInitialized) {
                for (int type = 0, count = MetaTable.getMetaTableTypeCount();
                     type < count; type++) {
                    MetaTable m = new MetaTable(infoSchema, -1 - type, type);
                    infoSchema.add(m);
                }
                metaTablesInitialized = true;
            }
        }
    }
    /**
     * Get all schema objects.
     *
     * @return all objects of all types
     */
    public ArrayList<SchemaObject> getAllSchemaObjects() {
        initMetaTables();
        ArrayList<SchemaObject> list = new ArrayList<>();
        for (Schema schema : schemas.values()) {
            schema.getAll(list);
        }
        return list;
    }

    /**
     * Get all schema objects of the given type.
     *
     * @param type the object type
     * @return all objects of that type
     */
    public ArrayList<SchemaObject> getAllSchemaObjects(int type) {
        if (type == DbObject.TABLE_OR_VIEW) {
            initMetaTables();
        }
        ArrayList<SchemaObject> list = new ArrayList<>();
        for (Schema schema : schemas.values()) {
            schema.getAll(type, list);
        }
        return list;
    }

    /**
     * Get all tables and views.
     *
     * @param includeMeta whether to force including the meta data tables (if
     *            true, metadata tables are always included; if false, metadata
     *            tables are only included if they are already initialized)
     * @return all objects of that type
     */
    public ArrayList<Table> getAllTablesAndViews(boolean includeMeta) {
        if (includeMeta) {
            initMetaTables();
        }
        ArrayList<Table> list = new ArrayList<>();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAllTablesAndViews());
        }
        return list;
    }

    /**
     * Get all synonyms.
     *
     * @return all objects of that type
     */
    public ArrayList<TableSynonym> getAllSynonyms() {
        ArrayList<TableSynonym> list = new ArrayList<>();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAllSynonyms());
        }
        return list;
    }

    /**
     * Get the tables with the given name, if any.
     *
     * @param name the table name
     * @return the list
     */
    public ArrayList<Table> getTableOrViewByName(String name) {
        // we expect that at most one table matches, at least in most cases
        ArrayList<Table> list = new ArrayList<>(1);
        for (Schema schema : schemas.values()) {
            Table table = schema.getTableOrViewByName(name);
            if (table != null) {
                list.add(table);
            }
        }
        return list;
    }

    public Collection<Schema> getAllSchemas() {
        initMetaTables();
        return schemas.values();
    }

    /**
     * Get the schema if it exists, or null if not.
     *
     * @param schemaName the name of the schema
     * @return the schema or null
     */
    public Schema findSchema(String schemaName) {
        if (schemaName == null) {
            return null;
        }
        Schema schema = schemas.get(schemaName);
        if (schema == infoSchema) {
            initMetaTables();
        }
        return schema;
    }
    public synchronized void addSchema(Session session, Schema obj) {
        int id = obj.getId();
        if (id > 0 && !database.isStarting()) {
            database.checkWritingAllowed();
        }
        String name = obj.getName();
        if (SysProperties.CHECK && schemas.get(name) != null) {
            DbException.throwInternalError("object already exists");
        }
        database.lockMeta(session);
        database.addMeta(session, obj);
        schemas.put(name, obj);
    }


}
