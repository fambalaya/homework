local sqlite = require('lsqlite3')

local db_mt = {}

local function open_db(db_name)
    local db = { name = db_name or 'memory' }
    if db_name then
        db.db = sqlite.open(db_name)
    else
        db.db = sqlite.open_memory()
    end
    return setmetatable(db, db_mt)
end

local function close_db(db)
    db.db:close()
    db.db = nil
end

local function table_names(db)
    local names = {}
    for a in db.db:urows('SELECT tbl_name FROM sqlite_master') do
        table.insert(names, a)
    end
    return names
end

local function demo_sql(demo)
    local function get_type(v)
        local sql_type = { TEXT = "TEXT", INTEGER = "INTEGER" }
        local lua_type = { string = "TEXT", number = "INTEGER" }
        return sql_type[v] or lua_type[type(v)] or "TEXT"
    end

    local ret = {}
    local exist = {}
    for _, v in ipairs(demo) do
        if not exist[v] then
            table.insert(ret, v .. ' ' .. get_type(demo[v]))
            exist[v] = true
        end
    end

    for k, v in pairs(demo) do
        if not exist[k] and type(k) == 'string' then
            table.insert(ret, k .. ' ' .. get_type(v))
            exist[v] = true
        end
    end
    return table.concat(ret, ',')
end

local function create_table(db, tb_name, demo)
    -- CREATE TABLE "t1" ("name" TEXT, "id" INTEGER)
    local cmd = string.format('CREATE TABLE "%s" (%s)', tb_name, demo_sql(demo))
    db.db:exec(cmd)
end

local function delete_table(db, tb_name)
    local cmd = string.format('DROP TABLE "%s"', tb_name)
    db.db:exec(cmd)
end

local table_mt = {}

local function get_table_sql(db, tb_name)
    local cmd = string.format('SELECT sql FROM sqlite_master WHERE tbl_name="%s"', tb_name)
    local f, v = db.db:urows(cmd)
    return f(v)
end

local function get_table(db, tb_name)
    local tb_sql = get_table_sql(db, tb_name)
    if not tb_sql then return end
    return setmetatable({ db = db, name = tb_name, sql = tb_sql }, table_mt)
end

db_mt.__gc = function(db)
    if db.db then
        db.db:close()
    end
end

db_mt.__index = {
    close = close_db,
    tables = table_names,
    create = create_table,
    delete = delete_table,
    table = get_table
}

local function table_head(tb)
    local param = tb.sql:match('%((.*)%)')
    local ret = {}
    for k, t in param:gmatch('"(.-)"%s(%a*)') do
        table.insert(ret, k)
        ret[k] = t
    end
    return ret
end

local function table_count(tb)
    local cmd = string.format('SELECT count() FROM %s', tb.name)
    local f, v = tb.db.db:urows(cmd)
    return f(v)
end

local function table_insert(tb, t)
end
local function table_delete(tb, k)
end
local function table_update(tb, k, t)
end
local function table_query(tb, k)
end

table_mt.__index = {
    head = table_head,
    count = table_count,
    insert = table_insert,
    delete = table_delete,
    query = table_query
}

return {
    open = open_db,
    db = open_db
}

    
