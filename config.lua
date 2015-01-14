box.cfg{
    listen = 3013,
    wal_dir='xlog',
    snap_dir='snap',
}
local s = box.schema.space.create('test', {if_not_exists = true})
local i = s:create_index('primary', {type = 'hash', parts = {1, 'NUM'}, if_not_exists = true})

box.schema.user.grant('guest', 'read,write,execute', 'universe')

local console = require 'console'
console.listen '0.0.0.0:33015'

