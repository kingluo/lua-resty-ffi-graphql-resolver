--
-- Copyright (c) 2023, Jinhua Luo (kingluo) luajit.io@gmail.com
-- All rights reserved.
--
-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions are met:
--
-- 1. Redistributions of source code must retain the above copyright notice, this
--    list of conditions and the following disclaimer.
--
-- 2. Redistributions in binary form must reproduce the above copyright notice,
--    this list of conditions and the following disclaimer in the documentation
--    and/or other materials provided with the distribution.
--
-- 3. Neither the name of the copyright holder nor the names of its
--    contributors may be used to endorse or promote products derived from
--    this software without specific prior written permission.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
-- AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
-- IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
-- DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
-- FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
-- DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
-- SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
-- CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
-- OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--
local cjson = require("cjson")
require("resty_ffi")
local graphql_resolver = ngx.load_ffi("resty_ffi_python", "resty.ffi.graphql_resolver,init,{}", {is_global=true})

local NEW_SCHEMA = 1
local CLOSE_SCHEMA = 2
local QUERY = 3

local _M = {}

local objs = {}

ngx.timer.every(3, function()
    if #objs > 0 then
        for _, s in ipairs(objs) do
            local ok = s:close()
            assert(ok)
        end
        objs = {}
    end
end)

local function setmt__gc(t, mt)
    local prox = newproxy(true)
    getmetatable(prox).__gc = function() mt.__gc(t) end
    t[prox] = true
    return setmetatable(t, mt)
end

local meta = {
    __gc = function(self)
        if self.closed then
            return
        end
        table.insert(objs, self)
    end,
    __index = {
        query = function(self, opts)
            local ok, res, err = graphql_resolver:query(cjson.encode({
                cmd = QUERY,
                schema = self.schema,
                data = opts,
            }))
            return ok, ok and cjson.decode(res) or nil, err
        end,
        close = function(self)
            self.closed = true
            return graphql_resolver:close(cjson.encode({
                cmd = CLOSE_SCHEMA,
                schema = self.schema,
            }))
        end,
    }
}

function _M.new(opts)
    local opts = {
        cmd = NEW_SCHEMA,
        data = opts,
    }
    local ok, res = graphql_resolver:new(cjson.encode(opts))
    if ok then
        res = cjson.decode(res)
        return setmt__gc({
            schema = res.schema,
            closed = false,
        }, meta)
    else
        return nil, res
    end
end

return _M
