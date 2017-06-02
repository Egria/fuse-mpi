#!/bin/sh
./ramfs $1 $2 -f -o allow_other,default_permissions,large_read,big_writes -s
