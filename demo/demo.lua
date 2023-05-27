local graphql_resolver = require("resty.ffi.graphql_resolver")
local cjson = require("cjson")

local _M = {}

local idx = 1
local schemas = {}

local function read_body()
    ngx.req.read_body()
    return cjson.decode(ngx.req.get_body_data())
end

function _M.create_schema()
    local schema, err = graphql_resolver.new(read_body())
    if err then
        ngx.say(require("inspect")(err))
    end
    assert(schema)
    schemas[idx] = schema
    ngx.say(string.format([[{"schema": %d}]], idx))
    idx = idx + 1
end

function _M.query()
    local idx = tonumber(ngx.var.arg_schema)
    local schema = schemas[idx]
    local ok, res, err = schema:query(read_body())
    ngx.say(cjson.encode(res))
    if err then
        ngx.say(require("inspect")(err))
    end
end

function _M.close_schema()
    local idx = tonumber(ngx.var.arg_schema)
    local schema = schemas[idx]
    local ok, err1, err2 = schema:close()
    schemas[idx] = nil
    ngx.say("ok")
end

return _M
