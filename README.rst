Sblob
=====

Indexes
-------

sblob
.....

Index: Seqnum
Value: Offset

The usage of the index for sblobs is: given a seqnum I want give me the offset
in the file, for this it looks at the closest seqnum (array index) that has the
offset defined (array value), then it reads until the required seqnum filling
the index along the way and then it reads the required sblob.

The index transversal is, I start from the seqnum I want and check if the value
is set, if not I go backwards until I find it or I get to the first array
index, if not found return the atom notfound which will cause all sblobs from
the beginning of the file to be read, filling the index along the way.

In this case closest means closest lower or equal seqnum.

gblob
.....

Index: Chunk Number
Value: Start Seqnum

The usage of the index for gblobs is: given a seqnum I want to start reading
from, give me the file chunk number where it's located so I can start reading
from there, for this it looks at the values of the array from left to right
until it finds the closest seqnum (array value) and returns the chunk number
(array index).

In this case closest means closest lower or equal seqnum.

Testing
-------

to run all tests:

::

    make eunit

to test one module::

    ./rebar eunit suites=gblob_tests skip_deps=true
